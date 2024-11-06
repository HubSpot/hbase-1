/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AtomicDouble;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.math.Quantiles;
import org.apache.hbase.thirdparty.com.google.common.primitives.Shorts;

/**
 * HubSpot addition: Cost function for balancing regions based on their (reversed) cell prefix. This
 * should not be upstreamed, and our upstream solution should instead focus on introduction of
 * balancer conditionals; see
 * <a href="https://issues.apache.org/jira/browse/HBASE-28513">HBASE-28513</a>
 */
@InterfaceAudience.Private
public class HubSpotCellCostFunction extends CostFunction {

  private static final Logger LOG = LoggerFactory.getLogger(HubSpotCellCostFunction.class);
  private static final String HUBSPOT_CELL_COST_MULTIPLIER =
    "hbase.master.balancer.stochastic.hubspotCellCost";
  private static final float DEFAULT_HUBSPOT_CELL_COST = 0;
  // hack - hard code this for now
  static final short MAX_CELL_COUNT = 360;
  private static final byte PAD_START_KEY = 0;
  private static final byte PAD_END_KEY = -1;

  private int numServers;
  private short numCells;
  private ServerName[] servers;
  private RegionInfo[] regions; // not necessarily sorted
  private int[][] regionLocations;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions, MAX_CELL_COUNT);
    regions = cluster.regions;
    regionLocations = cluster.regionLocations;
    servers = cluster.servers;
    super.prepare(cluster);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing {}", snapshotState());
    }
  }

  private String snapshotState() {
    StringBuilder stateString = new StringBuilder();

    stateString.append("HubSpotCellCostFunction[0] config for ")
      .append(Optional.ofNullable(regions[0]).map(RegionInfo::getTable)
        .map(TableName::getNameWithNamespaceInclAsString).orElseGet(() -> "N/A"))
      .append(":").append("\n\tnumServers=").append(numServers).append("\n\tnumCells=")
      .append(numCells).append("\n\tmultiplier=").append(String.format("%.3f", getMultiplier()))
      .append("\n\tregions=\n[");

    if (LOG.isDebugEnabled()) {
      int numAssigned = 0;
      int numUnassigned = 0;

      for (int i = 0; i < regions.length; i++) {
        RegionInfo region = regions[i];
        int[] regionLocations = this.regionLocations[i];
        Optional<ServerName> highestLocalityServerMaybe =
          Optional.ofNullable(regionLocations).filter(locations -> locations.length > 0)
            .map(locations -> locations[0]).map(serverIndex -> this.servers[serverIndex]);
        int assignedServers = Optional.ofNullable(regionLocations)
          .map(locations -> locations.length).orElseGet(() -> 0);

        if (assignedServers > 0) {
          numAssigned++;
        } else {
          numUnassigned++;
        }

        int regionSizeMb = super.cluster.getRegionSizeMB(i);
        String cellsInRegion =
          toCellSetString(toCells(region.getStartKey(), region.getEndKey(), numCells));

        stateString.append("\n\t").append(region.getShortNameToLog()).append("[")
          .append(Bytes.toHex(region.getStartKey())).append(", ")
          .append(Bytes.toHex(region.getEndKey())).append(") ").append(cellsInRegion).append(" [")
          .append(regionSizeMb).append(" mb] -> ")
          .append(highestLocalityServerMaybe.map(ServerName::getServerName).orElseGet(() -> "N/A"))
          .append("(with ").append(assignedServers).append(" total candidates)");
      }

      stateString.append("\n]\n\n\tAssigned regions: ").append(numAssigned)
        .append("\n\tUnassigned regions: ").append(numUnassigned).append("\n");
    } else {
      stateString.append("\n\t").append(regions.length).append(" regions\n]");
    }

    return stateString.toString();
  }

  private static String toCellSetString(Set<Short> cells) {
    return cells.stream().map(x -> Short.toString(x)).collect(Collectors.joining(", ", "{", "}"));
  }

  @Override
  protected double cost() {
    double cost = calculateCurrentCellCost(numCells, numServers, regions, regionLocations,
      super.cluster::getRegionSizeMB);

    if (
      regions != null && regions.length > 0
        && regions[0].getTable().getNamespaceAsString().equals("default") && LOG.isDebugEnabled()
    ) {
      LOG.debug("Evaluated (cost={}) {}", String.format("%.2f", cost), snapshotState());
    }

    return cost;
  }

  static int calculateCurrentCellCost(short numCells, int numServers, RegionInfo[] regions,
    int[][] regionLocations, Function<Integer, Integer> getRegionSizeMbFunc) {
    int bestCaseMaxCellsPerServer = (int) Math.min(1, Math.ceil((double) numCells / numServers));
    Preconditions.checkState(bestCaseMaxCellsPerServer > 0,
      "Best case max cells per server must be > 0");

    if (LOG.isDebugEnabled()) {
      Set<String> tableAndNamespace = Arrays.stream(regions).map(RegionInfo::getTable)
        .map(table -> table.getNameAsString() + "." + table.getNamespaceAsString())
        .collect(Collectors.toSet());
      LOG.debug("Calculating current cell cost for {} regions from these tables {}", regions.length,
        tableAndNamespace);
    }

    if (regions.length > 0 && !regions[0].getTable().getNamespaceAsString().equals("default")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping cost calculation for non-default namespace on {}",
          regions[0].getTable().getNameWithNamespaceInclAsString());
      }
      return 0;
    }

    boolean[][] serverHasCell = new boolean[numServers][numCells];
    for (int i = 0; i < regions.length; i++) {
      if (regions[i] == null) {
        throw new IllegalStateException("No region available at index " + i);
      }

      if (regionLocations[i] == null) {
        throw new IllegalStateException(
          "No server list available for region " + regions[i].getShortNameToLog());
      }

      if (regionLocations[i].length == 0) {
        int regionSizeMb = getRegionSizeMbFunc.apply(i);
        if (regionSizeMb == 0 && LOG.isTraceEnabled()) {
          LOG.trace("{} ({} mb): no servers available, this IS an empty region",
            regions[i].getShortNameToLog(), regionSizeMb);
        } else {
          LOG.warn("{} ({} mb): no servers available, this IS NOT an empty region",
            regions[i].getShortNameToLog(), regionSizeMb);
        }

        continue;
      }

      setCellsForServer(serverHasCell[regionLocations[i][0]], regions[i].getStartKey(),
        regions[i].getEndKey(), numCells);
    }

    int[] cellsPerServer = new int[numServers];
    for (int i = 0; i < numServers; i++) {
      int cellsOnThisServer = 0;
      for (int j = 0; j < numCells; j++) {
        if (serverHasCell[i][j]) {
          cellsOnThisServer++;
        }
      }

      cellsPerServer[i] = cellsOnThisServer;
    }

    Map<Integer, Double> stats = Quantiles.scale(100)
      .indexes(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100).compute(cellsPerServer);

    AtomicDouble totalCost = new AtomicDouble(0.0);
    stats.forEach((percentile, value) -> totalCost.addAndGet(value));

    return (int) Math
      .round(Math.max(0, totalCost.get() / stats.size() - bestCaseMaxCellsPerServer));
  }

  private static void setCellsForServer(boolean[] serverHasCell, byte[] startKey, byte[] endKey,
    short numCells) {
    short startCellId = (startKey == null || startKey.length == 0)
      ? 0
      : (startKey.length >= 2
        ? Bytes.toShort(startKey, 0, 2)
        : Bytes.toShort(new byte[] { 0, startKey[0] }));
    short stopCellId = (endKey == null || endKey.length == 0)
      ? (short) (numCells - 1)
      : (endKey.length >= 2
        ? Bytes.toShort(endKey, 0, 2)
        : Bytes.toShort(new byte[] { -1, endKey[0] }));

    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }

    if (startCellId == stopCellId) {
      serverHasCell[startCellId] = true;
      return;
    }

    for (short i = startCellId; i < stopCellId; i++) {
      serverHasCell[i] = true;
    }

    // if everything after the cell prefix is 0, this stop key is actually exclusive
    if (!isStopExclusive(endKey)) {
      serverHasCell[stopCellId] = true;
    }
  }

  static boolean isStopExclusive(byte[] endKey) {
    return endKey != null && endKey.length > 2 && areSubsequentBytesAllZero(endKey, 2);
  }

  static short calcNumCells(RegionInfo[] regionInfos, short totalCellCount) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    Set<Short> cellsInRegions = Arrays.stream(regionInfos)
      .map(region -> toCells(region.getStartKey(), region.getEndKey(), totalCellCount))
      .flatMap(Set::stream).collect(Collectors.toSet());
    return Shorts.checkedCast(cellsInRegions.size());
  }

  private static Set<Short> toCells(byte[] rawStart, byte[] rawStop, short numCells) {
    return range(padToTwoBytes(rawStart, (byte) 0), padToTwoBytes(rawStop, (byte) -1), numCells);
  }

  private static byte[] padToTwoBytes(byte[] key, byte pad) {
    if (key == null || key.length == 0) {
      return new byte[] { pad, pad };
    }

    if (key.length == 1) {
      return new byte[] { pad, key[0] };
    }

    return key;
  }

  private static Set<Short> range(byte[] start, byte[] stop, short numCells) {
    short stopCellId = toCell(stop);
    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }
    short startCellId = toCell(start);

    if (startCellId == stopCellId) {
      return ImmutableSet.of(startCellId);
    }

    // if everything after the cell prefix is 0, this stop key is actually exclusive
    boolean isStopExclusive = areSubsequentBytesAllZero(stop, 2);

    final IntStream cellStream;
    if (isStopExclusive) {
      cellStream = IntStream.range(startCellId, stopCellId);
    } else {
      // this is inclusive, but we have to make sure we include at least the startCellId,
      // even if stopCell = startCell + 1
      cellStream = IntStream.rangeClosed(startCellId, Math.max(stopCellId, startCellId + 1));
    }

    return cellStream.mapToObj(val -> (short) val).collect(Collectors.toSet());
  }

  private static boolean areSubsequentBytesAllZero(byte[] stop, int offset) {
    for (int i = offset; i < stop.length; i++) {
      if (stop[i] != (byte) 0) {
        return false;
      }
    }
    return true;
  }

  private static short toCell(byte[] key) {
    if (key == null || key.length < 2) {
      throw new IllegalArgumentException(
        "Key must be nonnull and at least 2 bytes long - passed " + Bytes.toHex(key));
    }

    return Bytes.toShort(key, 0, 2);
  }

  @Override
  public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.HUBSPOT_CELL.ordinal()] += cost();
  }
}
