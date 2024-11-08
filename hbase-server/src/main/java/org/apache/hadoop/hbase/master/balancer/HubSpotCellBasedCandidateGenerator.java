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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

@InterfaceAudience.Private class HubSpotCellBasedCandidateGenerator extends CandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(HubSpotCellBasedCandidateGenerator.class);
  private static final double CHANCE_OF_NOOP = 0.2;

  @Override BalanceAction generate(BalancerClusterState cluster) {
    if (cluster.tables.stream().noneMatch(name -> name.contains("objects-3"))) {
      return BalanceAction.NULL_ACTION;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Running HubSpotCellBasedCandidateGenerator with {} servers and {} regions for tables {}",
        cluster.regionsPerServer.length, cluster.regions.length, cluster.tables);
    }

    cluster.sortServersByRegionCount();
    int[][] regionsPerServer = cluster.regionsPerServer;

    int serverWithMostCells = -1;
    int mostCellsPerServerSoFar = 0;
    double mostCellsReservoirRandom = -1;

    for (int serverIndex = 0; serverIndex < regionsPerServer.length; serverIndex++) {
      int[] regionsForServer = regionsPerServer[serverIndex];
      int cellsOnServer = numCells(cluster, serverIndex, regionsForServer);

      if (LOG.isTraceEnabled()) {
        LOG.trace("Server {} has {} regions, which have {} cells",
          serverIndex,
          Arrays.stream(regionsForServer).boxed().sorted().collect(
          Collectors.toList()), cellsOnServer);
      }

      // we don't know how many servers have the same cell count, so use a simplified online
      // reservoir sampling approach (http://gregable.com/2007/10/reservoir-sampling.html)
      if (cellsOnServer > mostCellsPerServerSoFar) {
        mostCellsPerServerSoFar = cellsOnServer;
        mostCellsReservoirRandom = ThreadLocalRandom.current().nextDouble();
      } else if (cellsOnServer == mostCellsPerServerSoFar) {
        double maxCellRandom = ThreadLocalRandom.current().nextDouble();
        if (maxCellRandom > mostCellsReservoirRandom) {
          serverWithMostCells = serverIndex;
          mostCellsReservoirRandom = maxCellRandom;
        }
      }
    }

    return maybeMoveRegion(cluster, serverWithMostCells);
  }

  private int numCells(BalancerClusterState cluster, int serverIndex, int[] regionsForServer) {
    boolean[] cellsPresent = new boolean[HubSpotCellCostFunction.MAX_CELL_COUNT];

    for (int regionIndex : regionsForServer) {
      if (regionIndex < 0 || regionIndex > cluster.regions.length) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping region {} because it's <0 or >{}", regionIndex, regionsForServer.length);
        }
        continue;
      }

      RegionInfo region = cluster.regions[regionIndex];

      if (!region.getTable().getNamespaceAsString().equals("default")) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping region {} because it's not in the default namespace", region.getTable().getNameWithNamespaceInclAsString());
        }
        continue;
      }

      byte[] startKey = region.getStartKey();
      byte[] endKey = region.getEndKey();

      short startCellId = (startKey == null || startKey.length == 0) ?
        0 :
        (startKey.length >= 2 ?
          Bytes.toShort(startKey, 0, 2) :
          Bytes.toShort(new byte[] { 0, startKey[0] }));
      short endCellId = (endKey == null || endKey.length == 0) ?
        (short) (HubSpotCellCostFunction.MAX_CELL_COUNT - 1) :
        (endKey.length >= 2 ?
          Bytes.toShort(endKey, 0, 2) :
          Bytes.toShort(new byte[] { -1, endKey[0] }));

      if (startCellId < 0 || startCellId > HubSpotCellCostFunction.MAX_CELL_COUNT) {
        startCellId = HubSpotCellCostFunction.MAX_CELL_COUNT - 1;
      }

      if (endCellId < 0 || endCellId > HubSpotCellCostFunction.MAX_CELL_COUNT) {
        endCellId = HubSpotCellCostFunction.MAX_CELL_COUNT - 1;
      }

      for (short i = startCellId; i < endCellId; i++) {
        cellsPresent[i] = true;
      }

      if (!HubSpotCellCostFunction.isStopExclusive(endKey)) {
        cellsPresent[endCellId] = true;
      }
    }

    int count = 0;
    for (boolean hasCell : cellsPresent) {
      if (hasCell) {
        count++;
      }
    }

    return count;
  }

  BalanceAction maybeMoveRegion(BalancerClusterState cluster, int serverWithMostCells) {
    if (serverWithMostCells < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No server with cells found");
      }
      return BalanceAction.NULL_ACTION;
    }

    if (cluster.regionsPerServer[serverWithMostCells].length == 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} has no regions", serverWithMostCells);
      }
      return BalanceAction.NULL_ACTION;
    }

    if (ThreadLocalRandom.current().nextFloat() < CHANCE_OF_NOOP) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Randomly taking no action. Chaos! Mwahahaha!");
      }
      return BalanceAction.NULL_ACTION;
    }

    Multimap<Integer, Short> cellsByRegionOnSource =
      computeCellsByRegion(cluster.regionsPerServer[serverWithMostCells], cluster.regions);
    Map<Short, AtomicInteger> countOfRegionsForCellOnSource = new HashMap<>();
    cellsByRegionOnSource.forEach(
      (region, cell) -> countOfRegionsForCellOnSource.computeIfAbsent(cell,
        ignored -> new AtomicInteger()).incrementAndGet());

    int regionWithFewestInstancesOfCellsPresent =
      cellsByRegionOnSource.keySet().stream().min(Comparator.comparing(region -> {
        return cellsByRegionOnSource.get(region).stream().mapToInt(cell -> {
          return countOfRegionsForCellOnSource.get(cell).get();
        }).max().orElseGet(() -> 0);
      })).orElseGet(() -> -1);

    int targetServer = computeBestServerToReceiveRegion(cluster, serverWithMostCells,
      regionWithFewestInstancesOfCellsPresent);

    if (LOG.isTraceEnabled()) {
      Multimap<Integer, Short> cellsByRegionOnTarget =
        computeCellsByRegion(cluster.regionsPerServer[targetServer], cluster.regions);

      Set<Short> currentCellsOnSource = new HashSet<>(cellsByRegionOnSource.values());
      Set<Short> currentCellsOnTarget = new HashSet<>(cellsByRegionOnTarget.values());

      Set<Short> afterMoveCellsOnSource = cellsByRegionOnSource.keySet().stream()
        .filter(region -> region != regionWithFewestInstancesOfCellsPresent)
        .flatMap(region -> cellsByRegionOnSource.get(region).stream())
        .collect(Collectors.toSet());
      Set<Short> afterMoveCellsOnTarget = new HashSet<>(currentCellsOnTarget);
      afterMoveCellsOnTarget.addAll(
        cellsByRegionOnSource.get(regionWithFewestInstancesOfCellsPresent));

      boolean sourceImproves = afterMoveCellsOnSource.size() < currentCellsOnSource.size();
      boolean targetStaysSame = afterMoveCellsOnTarget.size() == currentCellsOnTarget.size();

      LOG.trace("Moving s{}.r{} to {}. SOURCE is {} -> {}, TARGET is {} -> {}. Change is {}",
        serverWithMostCells,
        regionWithFewestInstancesOfCellsPresent,
        targetServer,
        currentCellsOnSource.size(),
        afterMoveCellsOnSource.size(),
        currentCellsOnTarget.size(),
        afterMoveCellsOnTarget.size(),
        (sourceImproves && targetStaysSame) ? "GOOD" : ((sourceImproves) ? "NEUTRAL" : "BAD")
      );
    }

    return getAction(serverWithMostCells, regionWithFewestInstancesOfCellsPresent, targetServer, -1);
  }

  private int computeBestServerToReceiveRegion(BalancerClusterState cluster, int currentServer,
    int region) {
    // This is the lightest loaded (by count), but we want to keep cell collocation to a minimum
    int target = cluster.serverIndicesSortedByRegionCount[0];

    Set<Short> cellsOnTransferRegion =
      new HashSet<>(computeCellsByRegion(new int[] { region }, cluster.regions).get(region));

    // so, we'll make a best effort to see if we can find a reasonably loaded server that already
    // has the cells for this region
    for (int i = 0; i < cluster.serverIndicesSortedByRegionCount.length; i++) {
      int server = cluster.serverIndicesSortedByRegionCount[i];

      if (server == currentServer) {
        continue;
      }

      int[] regionsOnCandidate = cluster.regionsPerServer[server];
      if (regionsOnCandidate.length > 2 * cluster.regionsPerServer[currentServer].length) {
        // don't try to transfer a region to a server that already has more than 2x ours
        break;
      }

      Multimap<Integer, Short> possibleTargetCellsByRegion =
        computeCellsByRegion(regionsOnCandidate, cluster.regions);
      // if the candidate server has all the cells we need, this transfer can only improve isolation
      if (new HashSet<>(possibleTargetCellsByRegion.values()).containsAll(cellsOnTransferRegion)) {
        target = server;
        break;
      }
    }

    return target;
  }

  private Multimap<Integer, Short> computeCellsByRegion(int[] regionIndices, RegionInfo[] regions) {
    ImmutableMultimap.Builder<Integer, Short> resultBuilder = ImmutableMultimap.builder();
    for (int regionIndex : regionIndices) {
      if (regionIndex < 0 || regionIndex > regions.length) {
        continue;
      }

      RegionInfo region = regions[regionIndex];

      if (!region.getTable().getNamespaceAsString().equals("default")) {
        continue;
      }

      byte[] startKey = region.getStartKey();
      byte[] endKey = region.getEndKey();

      short startCellId = (startKey == null || startKey.length == 0) ?
        0 :
        (startKey.length >= 2 ?
          Bytes.toShort(startKey, 0, 2) :
          Bytes.toShort(new byte[] { 0, startKey[0] }));
      short endCellId = (endKey == null || endKey.length == 0) ?
        (short) (HubSpotCellCostFunction.MAX_CELL_COUNT - 1) :
        (endKey.length >= 2 ?
          Bytes.toShort(endKey, 0, 2) :
          Bytes.toShort(new byte[] { -1, endKey[0] }));

      if (startCellId < 0 || startCellId > HubSpotCellCostFunction.MAX_CELL_COUNT) {
        startCellId = HubSpotCellCostFunction.MAX_CELL_COUNT - 1;
      }

      if (endCellId < 0 || endCellId > HubSpotCellCostFunction.MAX_CELL_COUNT) {
        endCellId = HubSpotCellCostFunction.MAX_CELL_COUNT - 1;
      }

      for (short i = startCellId; i < endCellId; i++) {
        resultBuilder.put(regionIndex, i);
      }

      if (!HubSpotCellCostFunction.isStopExclusive(endKey)) {
        resultBuilder.put(regionIndex, endCellId);
      }
    }
    return resultBuilder.build();
  }
}
