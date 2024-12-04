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
import java.util.stream.Stream;
import org.agrona.collections.Int2IntCounterMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMultimap;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;

/**
 * HubSpot addition: Cost function for balancing regions based on their cell prefix. This
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

  private int numServers;
  private short numCells;
  private ServerName[] servers;
  private RegionInfo[] regions;
  private int[] regionIndexToServerIndex;

  private boolean[][] serverHasCell;
  private Int2IntCounterMap regionCountByCell;

  private int maxAcceptableCellsPerServer;
  private int balancedRegionsPerServer;

  private int numServerCellsOutsideDesiredBand;
  private boolean[] serverIsBalanced;
  private int numServersUnbalanced;
  private double cost;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    if (!isNeeded(cluster)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HubSpotCellCostFunction is not needed for {}", cluster.tables);
      }
      return;
    }

    numServers = cluster.numServers;
    numCells = HubSpotCellUtilities.calcNumCells(cluster.regions, HubSpotCellUtilities.MAX_CELL_COUNT);
    regions = cluster.regions;
    regionIndexToServerIndex = cluster.regionIndexToServerIndex;
    servers = cluster.servers;

    if (LOG.isTraceEnabled()
      && isNeeded()
      && cluster.regions != null
      && cluster.regions.length > 0
    ) {
      try {
        LOG.trace("{} cluster state:\n{}", cluster.tables, HubSpotCellUtilities.OBJECT_MAPPER.toJson(cluster));
      } catch (Exception ex) {
        LOG.error("Failed to write cluster state", ex);
      }
    }

    this.serverHasCell = new boolean[numServers][numCells];
    int bestCaseMaxCellsPerServer = Ints.checkedCast((long) Math.ceil((double) cluster.numRegions / cluster.numServers));

    this.regionCountByCell = new Int2IntCounterMap(HubSpotCellUtilities.MAX_CELL_COUNT, 0.5f, 0);
    Arrays.stream(cluster.regions)
      .forEach(r -> HubSpotCellUtilities.toCells(r.getStartKey(), r.getEndKey(), HubSpotCellUtilities.MAX_CELL_COUNT).forEach(cell -> regionCountByCell.addAndGet((int) cell, 1)));
    int numTimesCellRegionsFillAllServers = 0;
    for (int cell = 0; cell < HubSpotCellUtilities.MAX_CELL_COUNT; cell++) {
      int numRegionsForCell = regionCountByCell.get(cell);
      numTimesCellRegionsFillAllServers += Ints.checkedCast((long) Math.floor((double) numRegionsForCell / numServers));
    }

    bestCaseMaxCellsPerServer -= numTimesCellRegionsFillAllServers;
    bestCaseMaxCellsPerServer = Math.min(bestCaseMaxCellsPerServer, HubSpotCellUtilities.MAX_CELLS_PER_RS);
    this.maxAcceptableCellsPerServer = bestCaseMaxCellsPerServer;
    this.balancedRegionsPerServer = Ints.checkedCast(
      (long) Math.floor((double) cluster.numRegions / cluster.numServers));
    this.serverIsBalanced = new boolean[cluster.numServers];
    IntStream.range(0, cluster.numServers)
      .forEach(server -> serverIsBalanced[server] = isBalanced(server));
    this.numServersUnbalanced =
      Ints.checkedCast(IntStream.range(0, cluster.numServers).filter(server -> !serverIsBalanced[server]).count());

    this.numServerCellsOutsideDesiredBand =
      calculateCurrentCountOfCellsOutsideDesiredBand(
        numCells,
        numServers,
        maxAcceptableCellsPerServer,
        regions,
        regionIndexToServerIndex,
        serverHasCell,
        super.cluster::getRegionSizeMB
      );

    recomputeCost();

    if (regions.length > 0
      && regions[0].getTable().getNamespaceAsString().equals("default")
    ) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Evaluated (cost={})", String.format("%.4f", this.cost));
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Evaluated (cost={}) {}", String.format("%.4f", this.cost), snapshotState());
      }
    }
  }

  @Override boolean isNeeded() {
    return isNeeded(cluster);
  }

  private boolean isNeeded(BalancerClusterState currentClusterState) {
    return currentClusterState.tables.size() == 1
      && HubSpotCellUtilities.CELL_AWARE_TABLES.contains(Iterables.getOnlyElement(currentClusterState.tables))
      && currentClusterState.regions != null
      && currentClusterState.regions.length > 0;
  }

  private boolean isBalanced(int server) {
    return cluster.regionsPerServer[server].length >= balancedRegionsPerServer && cluster.regionsPerServer[server].length <= balancedRegionsPerServer + 1;
  }

  @Override protected void regionMoved(int region, int oldServer, int newServer) {
    RegionInfo movingRegion = regions[region];

    Set<Short> cellsOnRegion = HubSpotCellUtilities.toCells(movingRegion.getStartKey(), movingRegion.getEndKey(), numCells);

    boolean isOldServerBalanced = isBalanced(oldServer);
    this.serverIsBalanced[oldServer] = isOldServerBalanced;
    boolean isNewServerBalanced = isBalanced(newServer);
    this.serverIsBalanced[newServer] = isNewServerBalanced;
    this.numServersUnbalanced =
      Ints.checkedCast(IntStream.range(0, cluster.numServers).filter(server -> !serverIsBalanced[server]).count());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Evaluating move of region {} [{}, {}). Cells are {}.",
        region,
        Bytes.toHex(movingRegion.getStartKey()),
        Bytes.toHex(movingRegion.getEndKey()),
        cellsOnRegion
      );
    }

    Map<Short, Integer> numRegionsForCellOnOldServer = computeCellFrequencyForServer(oldServer);
    Map<Short, Integer> numRegionsForCellOnNewServer = computeCellFrequencyForServer(newServer);

    int currentCellCountOldServer = numRegionsForCellOnOldServer.keySet().size();
    int currentCellCountNewServer = numRegionsForCellOnNewServer.keySet().size();

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Old server {} [{} cells] has cell frequency of {}.\n\nNew server {} [{} cells] has cell frequency of {}.",
        oldServer,
        currentCellCountOldServer,
        numRegionsForCellOnOldServer,
        newServer,
        currentCellCountNewServer,
        numRegionsForCellOnNewServer
      );
    }

    int changeInRegionCellsOutsideDesiredBand = 0;
    for (short movingCell : cellsOnRegion) {
      // this is invoked AFTER the region has been moved
      boolean didMoveDecreaseCellsOnOldServer = !numRegionsForCellOnOldServer.containsKey(movingCell);
      boolean didMoveIncreaseCellsOnNewServer = numRegionsForCellOnNewServer.get(movingCell) == 1;

      if (didMoveIncreaseCellsOnNewServer) {
        serverHasCell[newServer][movingCell] = true;
        if (currentCellCountNewServer <= maxAcceptableCellsPerServer) {
          changeInRegionCellsOutsideDesiredBand--;
        } else {
          changeInRegionCellsOutsideDesiredBand++;
        }
      }
      if (didMoveDecreaseCellsOnOldServer) {
        serverHasCell[oldServer][movingCell] = false;
        if (currentCellCountOldServer >= maxAcceptableCellsPerServer) {
          changeInRegionCellsOutsideDesiredBand--;
        } else {
          changeInRegionCellsOutsideDesiredBand++;
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Move cost delta for s{}.r{} --> s{} is {}", oldServer, region, newServer, changeInRegionCellsOutsideDesiredBand);
    }

    this.numServerCellsOutsideDesiredBand += changeInRegionCellsOutsideDesiredBand;
    recomputeCost();
  }

  private void recomputeCost() {
    double newCost =
      (double) numServerCellsOutsideDesiredBand / (maxAcceptableCellsPerServer * cluster.numServers)
        + numServersUnbalanced;
    cost = newCost;
  }

  private Map<Short, Integer> computeCellFrequencyForServer(int server) {
    int[] regions = cluster.regionsPerServer[server];
    ImmutableMultimap.Builder<Short, Integer> regionsByCell = ImmutableMultimap.builder();
    for (int regionIndex : regions) {
      RegionInfo region = cluster.regions[regionIndex];
      Set<Short> cellsInRegion = HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(), numCells);
      cellsInRegion.forEach(cell -> regionsByCell.put(cell, regionIndex));
    }

    return regionsByCell.build()
      .asMap()
      .entrySet()
      .stream()
      .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().size()));
  }

  private String snapshotState() {
    StringBuilder stateString = new StringBuilder();

    stateString.append("HubSpotCellCostFunction config for ")
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
        int location = this.regionIndexToServerIndex[i];
        Optional<ServerName> highestLocalityServerMaybe =
          Optional.ofNullable(location).filter(serverLocation -> serverLocation >= 0)
            .map(serverIndex -> this.servers[serverIndex]);

        if (location > 0) {
          numAssigned++;
        } else {
          numUnassigned++;
        }

        int regionSizeMb = super.cluster.getRegionSizeMB(i);
        String cellsInRegion =
          HubSpotCellUtilities.toCellSetString(
            HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(), numCells));

        stateString.append("\n\t").append(region.getShortNameToLog()).append("[")
          .append(Bytes.toHex(region.getStartKey())).append(", ")
          .append(Bytes.toHex(region.getEndKey())).append(") ").append(cellsInRegion).append(" [")
          .append(regionSizeMb).append(" mb] -> ")
          .append(highestLocalityServerMaybe.map(ServerName::getServerName).orElseGet(() -> "N/A"));
      }

      stateString.append("\n]\n\n\tAssigned regions: ").append(numAssigned)
        .append("\n\tUnassigned regions: ").append(numUnassigned).append("\n");
    } else {
      stateString.append("\n\t").append(regions.length).append(" regions\n]");
    }

    return stateString.toString();
  }

  @Override
  protected double cost() {
    return cost;
  }

  static int calculateCurrentCountOfCellsOutsideDesiredBand(
    short numCells,
    int numServers,
    int maxAcceptableCellsPerServer,
    RegionInfo[] regions,
    int[] regionLocations,
    boolean[][] serverHasCell,
    Function<Integer, Integer> getRegionSizeMbFunc
  ) {
    Preconditions.checkState(maxAcceptableCellsPerServer > 0,
      "Max cells per server must be > 0");

    if (LOG.isTraceEnabled()) {
      Set<String> tableAndNamespace = Arrays.stream(regions).map(RegionInfo::getTable)
        .map(table -> table.getNameAsString() + "." + table.getNamespaceAsString())
        .collect(Collectors.toSet());
      LOG.trace("Calculating current cell cost for {} regions from these tables {}", regions.length,
        tableAndNamespace);
    }

    for (int i = 0; i < regions.length; i++) {
      if (regions[i] == null) {
        throw new IllegalStateException("No region available at index " + i);
      }

      if (regionLocations[i] == -1) {
        int regionSizeMb = getRegionSizeMbFunc.apply(i);
        if (regionSizeMb == 0 && LOG.isTraceEnabled()) {
          LOG.trace("{} ({} mb): no servers available, this IS an empty region",
            regions[i].getShortNameToLog(), regionSizeMb);
        } else {
          throw new IllegalStateException(
            "No server list available for region " + regions[i].getShortNameToLog());
        }
      }

      setCellsForServer(serverHasCell[regionLocations[i]], regions[i].getStartKey(),
        regions[i].getEndKey(), numCells);
    }

    int cost = 0;
    StringBuilder debugBuilder = new StringBuilder().append("[");
    for (int server = 0; server < numServers; server++) {
      int cellsOnThisServer = 0;
      for (int j = 0; j < numCells; j++) {
        if (serverHasCell[server][j]) {
          cellsOnThisServer++;
        }
      }
      int costForThisServer = Math.abs(cellsOnThisServer - maxAcceptableCellsPerServer);
      if (LOG.isDebugEnabled()) {
        debugBuilder.append(server).append("=").append(costForThisServer).append(", ");
      }
      cost += costForThisServer;
    }

    if (LOG.isDebugEnabled()) {
      debugBuilder.append("]");
      LOG.debug("Unweighted cost {} from {}", cost, debugBuilder);
    }

    return cost;
  }

  private static void setCellsForServer(
    boolean[] serverHasCell,
    byte[] startKey,
    byte[] endKey,
    short numCells
  ) {
    HubSpotCellUtilities.range(startKey, endKey, numCells)
      .forEach(cellId -> {
        Preconditions.checkState(0 <= cellId && cellId < numCells,
          "Cell ID %d is out of bounds - failed to compute for [%s, %s)", cellId, Bytes.toHex(startKey), Bytes.toHex(endKey));
        serverHasCell[cellId] = true;
      });
  }

  @Override
  public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.HUBSPOT_CELL.ordinal()] += cost();
  }
}
