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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
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
  private static final short MAX_CELL_COUNT = 360;

  private int numServers;
  private short numCells;
  private RegionInfo[] regions; // not necessarily sorted
  private int[][] regionLocations;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions, MAX_CELL_COUNT);
    regions = cluster.regions;
    regionLocations = cluster.regionLocations;
    super.prepare(cluster);

    StringBuilder initString = new StringBuilder();

    initString.append("Initializing HubSpotCellCostFunction:\n\t").append("numServers=")
      .append(numServers).append("\n\tnumCells=").append(numCells).append("\n\tregions=\n")
      .append(stringifyRegions(regions)).append("\n\tregionLocations=\n")
      .append(Arrays.deepToString(regionLocations));

    LOG.info("{}", initString);
  }

  private static String stringifyRegions(RegionInfo[] regions) {
    return "[\n\t" +
      Arrays.stream(regions)
      .map(HubSpotCellCostFunction::stringifyRegion )
      .collect(Collectors.joining("\n\t")) +
      "\n]";
  }

  private static String stringifyRegion(RegionInfo info) {
    return String.format(
      "%s [%s, %s)",
      info.getRegionNameAsString(),
      Bytes.toHex(info.getStartKey()),
      Bytes.toHex(info.getEndKey())
    );
  }

  @Override protected double cost() {
    if (regions != null && regions.length > 0 && regions[0].getTable().getNamespaceAsString().equals("default")) {
      StringBuilder stateString = new StringBuilder();
      stateString.append("Calculating cost for HubSpotCellCostFunction against default namespace:\n\t").append("numServers=")
        .append(numServers).append("\n\tnumCells=").append(numCells).append("\n\tregions=\n")
        .append(stringifyRegions(regions)).append("\n\tregionLocations=\n")
        .append(Arrays.deepToString(regionLocations));

      LOG.info("{}", stateString);
    }

    return calculateCurrentCellCost(numCells, numServers, regions, regionLocations);
  }

  static int calculateCurrentCellCost(short numCells, int numServers, RegionInfo[] regions,
    int[][] regionLocations) {
    int bestCaseMaxCellsPerServer = (int) Math.min(1, Math.ceil((double) numCells / numServers));
    Preconditions.checkState(bestCaseMaxCellsPerServer > 0, "Best case max cells per server must be > 0");

    if (LOG.isDebugEnabled()) {
      Set<String> tableAndNamespace = Arrays.stream(regions).map(RegionInfo::getTable)
        .map(table -> table.getNameAsString() + "." + table.getNamespaceAsString())
        .collect(Collectors.toSet());
      LOG.debug("Calculating current cell cost for {} regions from these tables {}", regions.length, tableAndNamespace);
    }

    if (regions.length > 0 && !regions[0].getTable().getNamespaceAsString().equals("default")) {
      LOG.info("Skipping cost calculation for non-default namespace on {}", regions[0].getTable().getNameWithNamespaceInclAsString());
      return 0;
    }

    int[] cellsPerServer = new int[numServers];
    for (int i = 0; i < regions.length; i++) {
      Preconditions.checkNotNull(regions[i], "No region available at index " + i);
      Preconditions.checkNotNull(regionLocations[i], "No region location available for " + stringifyRegion(regions[i]));
      int serverIndex = regionLocations[i][0];
      RegionInfo region = regions[i];
      Set<Short> regionCells = toCells(region.getStartKey(), region.getEndKey(), numCells);
      LOG.debug("Region {} has {} cells", region.getEncodedName(), regionCells);
      cellsPerServer[serverIndex] += regionCells.size();
    }

    for (int i = 0; i < numServers; i++) {
      LOG.info("Server {} has {} cells", i, cellsPerServer[i]);
    }

    int currentMaxCellsPerServer =
      Arrays.stream(cellsPerServer).max().orElseGet(() -> bestCaseMaxCellsPerServer);

    return Math.max(0, currentMaxCellsPerServer - bestCaseMaxCellsPerServer);
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
      return new byte[] { pad, key[0]};
    }

    return key;
  }

  private static Set<Short> range(byte[] start, byte[] stop, short numCells) {
    short stopCellId = toCell(stop);
    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }
    return IntStream.range(toCell(start), stopCellId)
      .mapToObj(val -> (short) val).collect(Collectors.toSet());
  }

  private static short toCell(byte[] key) {
    Preconditions.checkArgument(key != null && key.length >= 2, "Key must be nonnull and at least 2 bytes long - passed " + Bytes.toHex(key));

    return Bytes.toShort(key, 0, 2);
  }
}
