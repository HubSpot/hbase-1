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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;

@InterfaceAudience.Private class HubSpotCellBasedCandidateGenerator extends CandidateGenerator {
  private static final int NO_SERVER = -1;
  private static final boolean DEBUG_MAJOR = false;
  private static final boolean DEBUG_MINOR = false;

  private static final Logger LOG =
    LoggerFactory.getLogger(HubSpotCellBasedCandidateGenerator.class);

  @Override BalanceAction generate(BalancerClusterState cluster) {
    if (cluster.tables.stream().noneMatch(name -> name.contains("objects-3"))) {
      return BalanceAction.NULL_ACTION;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Running HubSpotCellBasedCandidateGenerator with {} servers and {} regions for tables {}",
        cluster.regionsPerServer.length, cluster.regions.length, cluster.tables);
    }

    int[] cellCounts = new int[HubSpotCellCostFunction.MAX_CELL_COUNT];
    Arrays.stream(cluster.regions)
      .flatMap(region -> HubSpotCellCostFunction.toCells(region.getStartKey(), region.getEndKey(), HubSpotCellCostFunction.MAX_CELL_COUNT).stream())
      .forEach(cellOnRegion -> cellCounts[cellOnRegion]++);
    double[] cellPercents = new double[HubSpotCellCostFunction.MAX_CELL_COUNT];
    for (int i = 0; i < cellCounts.length; i++) {
      cellPercents[i] = (double) cellCounts[i] / cluster.numRegions;
    }

    List<Map<Short, Integer>> cellGroupSizesPerServer =
      IntStream.range(0, cluster.regionsPerServer.length).mapToObj(
        serverIndex -> computeCellGroupSizes(cluster, serverIndex,
          cluster.regionsPerServer[serverIndex])).collect(Collectors.toList());

    return generateAction(cluster, cellCounts, cellGroupSizesPerServer);
  }

  private BalanceAction generateAction(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer
  ) {
    int targetRegionsPerServer = Ints.checkedCast(
      (long) Math.ceil((double) cluster.numRegions / cluster.numServers));

    List<Integer> underloadedServers = IntStream.range(0, cluster.numServers)
      .filter(server -> cluster.regionsPerServer[server].length < targetRegionsPerServer).boxed()
      .distinct()
      .collect(Collectors.toList());
    List<Integer> overloadedServers = IntStream.range(0, cluster.numServers)
      .filter(server -> cluster.regionsPerServer[server].length > targetRegionsPerServer).boxed()
      .distinct()
      .collect(Collectors.toList());

    // Step 1: if a previous action unbalanced us, try to rebalance region balance to be within plus/minus 1 of the target
    if (!underloadedServers.isEmpty() && !overloadedServers.isEmpty()) {
      return moveRegionFromOverloadedToUnderloaded(overloadedServers, underloadedServers, cellGroupSizesPerServer, cluster);
    }

    // Step 2: knowing we have region balance, try to expand the highest frequency cell(s) via swaps
    Pair<Short, Integer> cellOnServer = pickMostFrequentCellOnAnyUnsaturatedServer(cellGroupSizesPerServer, cellCounts, cluster);

    if (cellOnServer.getSecond() != NO_SERVER) {
      return swapSomeRegionToImprove(cellOnServer, cellGroupSizesPerServer, cluster);
    }

    // Step 3: balanced regions, and many/most servers are full now. We have a lot of smaller disconnected pieces
    // left to sort out. Pick the most loaded server, and try to reduce the cell count by 1. We can either swap
    // if possible, or give away if not. We're allowed to slightly imbalance here, knowing that subsequent rounds
    // will use step (1) to repair the imbalance.
    cellOnServer =
      pickLeastFrequentCellOnMostLoadedServer(cellGroupSizesPerServer, cellCounts, cluster);

    if (cellOnServer.getSecond() == NO_SERVER) {
      return BalanceAction.NULL_ACTION;
    }

    return giveAwaySomeRegionToImprove(
      cellOnServer,
      cellGroupSizesPerServer,
      cellCounts,
      cluster
    );
  }

  private BalanceAction moveRegionFromOverloadedToUnderloaded(
    List<Integer> overloadedServers,
    List<Integer> underloadedServers,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    BalancerClusterState cluster
  ) {
    int underloadedServer =
      underloadedServers.get(ThreadLocalRandom.current().nextInt(underloadedServers.size()));
    int overloadedServer = overloadedServers.get(ThreadLocalRandom.current().nextInt(overloadedServers.size()));
    short cellToMove = pickLeastFrequentCell(cellGroupSizesPerServer.get(overloadedServer));
    Multimap<Integer, Short> cellsByRegionForOverloadedServer =
      computeCellsByRegion(cluster.regionsPerServer[overloadedServer], cluster.regions);

    for (int overloadedServerCandidate : overloadedServers) {
      short lightest = pickLeastFrequentCell(cellGroupSizesPerServer.get(overloadedServerCandidate));
      if (cellGroupSizesPerServer.get(overloadedServerCandidate).get(lightest) == 1) {
        overloadedServer = overloadedServerCandidate;
        cellToMove = lightest;
        cellsByRegionForOverloadedServer = computeCellsByRegion(cluster.regionsPerServer[overloadedServer], cluster.regions);
        break;
      }
    }

    return getAction(
      overloadedServer,
      pickRegionForCell(cellsByRegionForOverloadedServer, cellToMove), underloadedServer,
      -1
    );
  }

  private BalanceAction giveAwaySomeRegionToImprove(
    Pair<Short, Integer> cellOnServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int[] cellCounts,
    BalancerClusterState cluster
  ) {

    short cellToRemove = cellOnServer.getFirst();
    int serverToYieldCell = cellOnServer.getSecond();

    Map<Short, Integer> cellCountsOnServerToYieldCell = cellGroupSizesPerServer.get(serverToYieldCell);
    Set<Short> cellsOnServerToYieldCell = cellCountsOnServerToYieldCell.keySet();

    int targetRegionsPerServer = Ints.checkedCast(
      (long) Math.ceil((double) cluster.numRegions / cluster.numServers));
    double allowableImbalanceInRegions = 1.05;

    Set<Integer> partialCandidatesWithInstanceOfOurCell = new HashSet<>();
    Set<Integer> fullCandidatesWithInstanceOfOurCell = new HashSet<>();
    List<Integer> candidatesWithFewerTotalCells = new ArrayList<>();
    for (int server = 0; server < cellGroupSizesPerServer.size(); server++) {
      if (server == serverToYieldCell) {
        continue;
      }

      Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(server);

      Set<Short> cellsOnServerAndOthers =
        cellsOnServer.keySet().stream().filter(cell -> cellsOnServer.get(cell) < cellCounts[cell])
          .collect(Collectors.toSet());

      double maxAllowedRegionCountPerServer = allowableImbalanceInRegions * targetRegionsPerServer;
      if (cellsOnServer.keySet().size() < cellCountsOnServerToYieldCell.keySet().size() &&
        cluster.regionsPerServer[server].length <= Math.ceil(maxAllowedRegionCountPerServer)) {
        candidatesWithFewerTotalCells.add(server);
      }

      // if that server is perfectly isolated, don't allow that to be broken even to fix another
      if (cellsOnServer.keySet().size() == 1 && !cellsOnServer.containsKey(cellToRemove)) {
        continue;
      }

      if (cluster.regionsPerServer[server].length >= Math.ceil(maxAllowedRegionCountPerServer)) {
        continue;
      }

      if (cellsOnServer.containsKey(cellToRemove)) {
        if (cellsOnServer.keySet().size() == 1
          || cellsOnServerAndOthers.size() == 1) {
          fullCandidatesWithInstanceOfOurCell.add(server);
        } else {
          partialCandidatesWithInstanceOfOurCell.add(server);
        }

        Sets.SetView<Short> cellsInCommon =
          Sets.intersection(cellsOnServerToYieldCell, cellsOnServer.keySet());

        if (cellsInCommon.size() > 1) {
          short commonCellToSwap =
            cellsInCommon.stream().filter(cell -> cell != cellToRemove).findAny().get();
          SwapRegionsAction action =
            swap(server, cellToRemove, serverToYieldCell, commonCellToSwap, cluster);
          if (LOG.isDebugEnabled() || DEBUG_MINOR) {
            int sourceOldTotal = cellsOnServerToYieldCell.size();
            int sourceNewTotal = cellsOnServerToYieldCell.size() - (cellCountsOnServerToYieldCell.get(cellToRemove) == 1 ? 1 : 0);
            int targetOldTotal = cellsOnServer.size();
            int targetNewTotal = cellsOnServer.size() - (cellsOnServer.get(commonCellToSwap) == 1 ? 1 : 0);

            boolean sourceImproves = sourceNewTotal < sourceOldTotal;
            boolean targetImproves = targetNewTotal < targetOldTotal;
            boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
            boolean targetStaysSame = targetOldTotal == targetNewTotal;

            String descrOfQuality =
              (sourceImproves && targetImproves) ? "GREAT" :
                ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
                (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
                  "BAD";

            System.out.printf(
              "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
                + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
              action.getFromServer(),
              action.getFromRegion(),
              action.getToServer(),
              action.getToRegion(),
              cellToRemove,
              cellCountsOnServerToYieldCell.get(cellToRemove),
              commonCellToSwap,
              cellCountsOnServerToYieldCell.get(commonCellToSwap),
              commonCellToSwap,
              cellsOnServer.get(commonCellToSwap),
              cellToRemove,
              cellsOnServer.get(cellToRemove),
              descrOfQuality
            );
            LOG.debug("Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
                + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
              action.getFromServer(),
              action.getFromRegion(),
              action.getToServer(),
              action.getToRegion(),
              cellToRemove,
              cellCountsOnServerToYieldCell.get(cellToRemove),
              commonCellToSwap,
              cellCountsOnServerToYieldCell.get(commonCellToSwap),
              commonCellToSwap,
              cellsOnServer.get(commonCellToSwap),
              cellToRemove,
              cellsOnServer.get(cellToRemove),
              descrOfQuality
            );
          }
          return action;
        }
      }
    }

    int serverToSend = -1;
    List<Integer> candidates = new ArrayList<>(partialCandidatesWithInstanceOfOurCell);

    Optional<Integer> serverWeCanImprove = candidates.stream()
      .filter(
        server -> {
          Map<Short, Integer> countsForServer = cellGroupSizesPerServer.get(server);
          return countsForServer.keySet().stream()
            .anyMatch(cell -> cell != cellToRemove && countsForServer.get(cell) == 1);
        }
      ).findFirst();

    if (partialCandidatesWithInstanceOfOurCell.isEmpty() && fullCandidatesWithInstanceOfOurCell.isEmpty()) {
      // nobody else has a copy of this cell that we can offload, we'll need to increase another server's load to reduce ours
      serverToSend = candidatesWithFewerTotalCells.get(ThreadLocalRandom.current().nextInt(candidatesWithFewerTotalCells.size()));
    } else if (serverWeCanImprove.isPresent()) {
      int serverToSwap = serverWeCanImprove.get();
      Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(serverToSwap);
      short cellToTakeFromSwap = cellsOnServer.keySet().stream()
        .filter(cell -> cell != cellToRemove && cellsOnServer.get(cell) == 1)
        .findFirst()
        .get();

      SwapRegionsAction action =
        swap(serverToSwap, cellToRemove, serverToYieldCell, cellToTakeFromSwap, cluster);

      if (LOG.isDebugEnabled() || DEBUG_MINOR) {
        int sourceOldTotal = cellsOnServerToYieldCell.size();
        int sourceNewTotal = cellsOnServerToYieldCell.size() - (cellCountsOnServerToYieldCell.get(cellToRemove) == 1 ? 1 : 0);
        int targetOldTotal = cellsOnServer.size();
        int targetNewTotal = cellsOnServer.size() - (cellsOnServer.get(cellToRemove) == 1 ? 1 : 0);

        boolean sourceImproves = sourceNewTotal < sourceOldTotal;
        boolean targetImproves = targetNewTotal < targetOldTotal;
        boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
        boolean targetStaysSame = targetOldTotal == targetNewTotal;

        String descrOfQuality =
          (sourceImproves && targetImproves) ? "GREAT" :
            ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
              (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
                "BAD";

        System.out.printf(
          "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
            + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
          action.getFromServer(),
          action.getFromRegion(),
          action.getToServer(),
          action.getToRegion(),
          cellToRemove,
          cellCountsOnServerToYieldCell.get(cellToRemove),
          cellToTakeFromSwap,
          cellCountsOnServerToYieldCell.get(cellToTakeFromSwap),
          cellToTakeFromSwap,
          cellsOnServer.get(cellToTakeFromSwap),
          cellToRemove,
          cellsOnServer.get(cellToRemove),
          descrOfQuality
        );
        LOG.debug("Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
            + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
          action.getFromServer(),
          action.getFromRegion(),
          action.getToServer(),
          action.getToRegion(),
          cellToRemove,
          cellCountsOnServerToYieldCell.get(cellToRemove),
          cellToTakeFromSwap,
          cellCountsOnServerToYieldCell.get(cellToTakeFromSwap),
          cellToTakeFromSwap,
          cellsOnServer.get(cellToTakeFromSwap),
          cellToRemove,
          cellsOnServer.get(cellToRemove),
          descrOfQuality
        );
      }
      return action;
    } else if (!fullCandidatesWithInstanceOfOurCell.isEmpty()) {
      serverToSend = fullCandidatesWithInstanceOfOurCell.stream()
        .findAny()
        .get();
    } else {
      candidates.sort(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).get(cellToRemove)));

      serverToSend = candidates.get(Math.max(0, candidates.size() - 1));
      int numInstancesOfCellOnServerToSend = cellGroupSizesPerServer.get(serverToSend).get(cellToRemove);

      double reservoirRandom = ThreadLocalRandom.current().nextDouble();
      for (int i = candidates.size() - 2; i >= 0; i--) {
        int nextCandidate = candidates.get(i);
        int numInstancesOfCellOnNextCandidate = cellGroupSizesPerServer.get(nextCandidate).get(cellToRemove);

        if (numInstancesOfCellOnNextCandidate < numInstancesOfCellOnServerToSend) {
          break;
        }

        double nextRandom = ThreadLocalRandom.current().nextDouble();
        if (nextRandom > reservoirRandom) {
          reservoirRandom = nextRandom;
          serverToSend = nextCandidate;
          numInstancesOfCellOnServerToSend = numInstancesOfCellOnNextCandidate;
        }
      }

      short cellToTake = pickRandomMinorityCell(cellGroupSizesPerServer.get(serverToSend));

      Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(serverToSend);
      SwapRegionsAction action =
        swap(serverToSend, cellToRemove, serverToYieldCell, cellToTake, cluster);

      if (LOG.isDebugEnabled() || DEBUG_MINOR) {
        int sourceOldTotal = cellsOnServerToYieldCell.size();
        int sourceNewTotal = cellsOnServerToYieldCell.size() - (cellCountsOnServerToYieldCell.get(cellToRemove) == 1 ? 1 : 0);
        int targetOldTotal = cellsOnServer.size();
        int targetNewTotal = cellsOnServer.size() - (cellsOnServer.get(cellToRemove) == 1 ? 1 : 0);

        boolean sourceImproves = sourceNewTotal < sourceOldTotal;
        boolean targetImproves = targetNewTotal < targetOldTotal;
        boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
        boolean targetStaysSame = targetOldTotal == targetNewTotal;

        String descrOfQuality =
          (sourceImproves && targetImproves) ? "GREAT" :
            ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
              (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
                "BAD";

        System.out.printf(
          "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
            + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
          action.getFromServer(),
          action.getFromRegion(),
          action.getToServer(),
          action.getToRegion(),
          cellToRemove,
          cellCountsOnServerToYieldCell.get(cellToRemove),
          cellToTake,
          cellCountsOnServerToYieldCell.get(cellToTake),
          cellToTake,
          cellsOnServer.get(cellToTake),
          cellToRemove,
          cellsOnServer.get(cellToRemove),
          descrOfQuality
        );
        LOG.debug("Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
            + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
          action.getFromServer(),
          action.getFromRegion(),
          action.getToServer(),
          action.getToRegion(),
          cellToRemove,
          cellCountsOnServerToYieldCell.get(cellToRemove),
          cellToTake,
          cellCountsOnServerToYieldCell.get(cellToTake),
          cellToTake,
          cellsOnServer.get(cellToTake),
          cellToRemove,
          cellsOnServer.get(cellToRemove),
          descrOfQuality
        );
      }
      return action;
    }

    Multimap<Integer, Short> cellsByRegion =
      computeCellsByRegion(cluster.regionsPerServer[serverToYieldCell], cluster.regions);

    MoveRegionAction action = (MoveRegionAction) getAction(
      serverToYieldCell,
      pickRegionForCell(cellsByRegion, cellToRemove),
      serverToSend,
      -1
    );

    Map<Short, Integer> cellsOnTarget = cellGroupSizesPerServer.get(serverToSend);

    if (LOG.isDebugEnabled() || DEBUG_MINOR) {
      int sourceOldTotal = cellsOnServerToYieldCell.size();
      int sourceNewTotal = cellsOnServerToYieldCell.size() - (cellCountsOnServerToYieldCell.get(cellToRemove) == 1 ? 1 : 0);
      int targetOldTotal = cellsOnTarget.size();
      int targetNewTotal = cellsOnTarget.size() + (cellsOnTarget.get(cellToRemove) == 0 ? 1 : 0);

      boolean sourceImproves = sourceNewTotal < sourceOldTotal;
      boolean targetImproves = targetNewTotal < targetOldTotal;
      boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
      boolean targetStaysSame = targetOldTotal == targetNewTotal;

      String descrOfQuality =
        (sourceImproves && targetImproves) ? "GREAT" :
          ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
            (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
              "BAD";

      System.out.printf(
        "Moving s%d.r%d c[%d / %d] to s%d. SOURCE is %d -> %d, TARGET is %d -> %d. Change is %s\n",
        action.getFromServer(),
        action.getRegion(),
        cellToRemove,
        cellCountsOnServerToYieldCell.get(cellToRemove),
        action.getToServer(),
        sourceOldTotal,
        sourceNewTotal,
        targetOldTotal,
        targetNewTotal,
        descrOfQuality
      );
      LOG.debug("Moving s{}.r{} c[{} / {}] to s{}. SOURCE is {} -> {}, TARGET is {} -> {}. Change is {}",
        action.getFromServer(),
        action.getRegion(),
        cellToRemove,
        cellCountsOnServerToYieldCell.get(cellToRemove),
        action.getToServer(),
        sourceOldTotal,
        sourceNewTotal,
        targetOldTotal,
        targetNewTotal,
        descrOfQuality
      );
    }

    return action;
  }

  private short pickRandomMinorityCell(Map<Short, Integer> cellCounts) {
    short preservedCell = pickMostFrequentCell(cellCounts);
    List<Short> candidates = cellCounts.keySet().stream().filter(cell -> cell != preservedCell)
      .collect(Collectors.toList());
    return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
  }

  private short pickLeastFrequentCell(
    Map<Short, Integer> cellCounts
  ) {
    short cellToPick = -1;
    int lowestCountSoFar = Integer.MAX_VALUE;
    double reservoirRandom = -1;

    for (short cell : cellCounts.keySet()) {
      int count = cellCounts.get(cell);
      if (count < lowestCountSoFar) {
        cellToPick = cell;
        lowestCountSoFar = count;
        reservoirRandom = ThreadLocalRandom.current().nextDouble();
      } else if (count == lowestCountSoFar) {
        double cellRandom = ThreadLocalRandom.current().nextDouble();
        if (cellRandom > reservoirRandom) {
          cellToPick = cell;
          reservoirRandom = cellRandom;
        }
      }
    }

    return cellToPick;
  }

  private short pickMostFrequentCell(
    Map<Short, Integer> cellCounts
  ) {
    short cellToPick = -1;
    int highestCountSoFar = Integer.MIN_VALUE;
    double reservoirRandom = -1;

    for (short cell : cellCounts.keySet()) {
      int count = cellCounts.get(cell);
      if (count > highestCountSoFar) {
        cellToPick = cell;
        highestCountSoFar = count;
        reservoirRandom = ThreadLocalRandom.current().nextDouble();
      } else if (count == highestCountSoFar) {
        double cellRandom = ThreadLocalRandom.current().nextDouble();
        if (cellRandom > reservoirRandom) {
          cellToPick = cell;
          reservoirRandom = cellRandom;
        }
      }
    }

    return cellToPick;
  }

  private BalanceAction swapSomeRegionToImprove(Pair<Short, Integer> cellOnServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer, BalancerClusterState cluster) {

    short cellToImprove = cellOnServer.getFirst();
    int serverToImprove = cellOnServer.getSecond();

    if (serverToImprove == -1) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No server available to improve");
      }
      return BalanceAction.NULL_ACTION;
    }

    Map<Short, Integer> cellCountsOnServerToImprove = cellGroupSizesPerServer.get(serverToImprove);
    Set<Short> cellsOnServerToImprove = cellCountsOnServerToImprove.keySet();

    if (serverToImprove < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No server with cells found");
      }
      return BalanceAction.NULL_ACTION;
    }

    if (cluster.regionsPerServer[serverToImprove].length == 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} has no regions", serverToImprove);
      }
      return BalanceAction.NULL_ACTION;
    }

    Set<Integer> candidateSet = new HashSet<>();
    for (int server = 0; server < cellGroupSizesPerServer.size(); server++) {
      if (server == serverToImprove) {
        continue;
      }

      Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(server);

      // if that server is perfectly isolated, don't allow that to be broken even to fix another
      if (cellsOnServer.keySet().size() == 1) {
        continue;
      }

      if (cellsOnServer.containsKey(cellToImprove)) {
        candidateSet.add(server);

        Sets.SetView<Short> cellsInCommon =
          Sets.intersection(cellsOnServerToImprove, cellsOnServer.keySet());

        if (cellsInCommon.size() > 1) {
          short commonCellToSwap =
            cellsInCommon.stream().filter(cell -> cell != cellToImprove).findAny().get();
          SwapRegionsAction action =
            swap(serverToImprove, cellToImprove, server, commonCellToSwap, cluster);
          if (LOG.isDebugEnabled() || DEBUG_MAJOR) {
            int sourceOldTotal = cellsOnServer.size();
            int sourceNewTotal = cellsOnServer.size() - (cellsOnServer.get(cellToImprove) == 1 ? 1 : 0);
            int targetOldTotal = cellsOnServerToImprove.size();
            int targetNewTotal = cellCountsOnServerToImprove.size() - (cellCountsOnServerToImprove.get(commonCellToSwap) == 1 ? 1 : 0);

            boolean sourceImproves = sourceNewTotal < sourceOldTotal;
            boolean targetImproves = targetNewTotal < targetOldTotal;
            boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
            boolean targetStaysSame = targetOldTotal == targetNewTotal;

            String descrOfQuality =
              (sourceImproves && targetImproves) ? "GREAT" :
                ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
                  (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
                    "BAD";

            System.out.printf(
              "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
                + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
              action.getFromServer(),
              action.getFromRegion(),
              action.getToServer(),
              action.getToRegion(),
              commonCellToSwap,
              cellCountsOnServerToImprove.get(commonCellToSwap),
              cellToImprove,
              cellCountsOnServerToImprove.get(cellToImprove),
              cellToImprove,
              cellsOnServer.get(cellToImprove),
              commonCellToSwap,
              cellsOnServer.get(commonCellToSwap),
              descrOfQuality
            );
            LOG.debug("Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
                + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
              action.getFromServer(),
              action.getFromRegion(),
              action.getToServer(),
              action.getToRegion(),
              commonCellToSwap,
              cellCountsOnServerToImprove.get(commonCellToSwap),
              cellToImprove,
              cellCountsOnServerToImprove.get(cellToImprove),
              cellToImprove,
              cellsOnServer.get(cellToImprove),
              commonCellToSwap,
              cellsOnServer.get(commonCellToSwap),
              descrOfQuality
            );
          }
          return action;
        }
      }
    }

    List<Integer> candidates = new ArrayList<>(candidateSet);

    if (candidates.isEmpty()) {
      // this means we've reached the end of the road for this particular cell
      return BalanceAction.NULL_ACTION;
    }

    int serverToSwap = candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
    short cellToOffer = cellsOnServerToImprove.stream()
      .filter(cell -> cell != cellToImprove)
      .collect(Collectors.toList())
      .get(ThreadLocalRandom.current().nextInt(cellsOnServerToImprove.size() - 1));

    Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(serverToSwap);

    SwapRegionsAction action =
      swap(serverToImprove, cellToImprove, serverToSwap, cellToOffer, cluster);

    if (LOG.isDebugEnabled() || DEBUG_MAJOR) {
      int sourceOldTotal = cellsOnServer.size();
      int sourceNewTotal = cellsOnServer.size() - (cellsOnServer.get(cellToImprove) == 1 ? 1 : 0);
      int targetOldTotal = cellsOnServerToImprove.size();
      int targetNewTotal = cellCountsOnServerToImprove.size() - (cellCountsOnServerToImprove.get(cellToOffer) == 1 ? 1 : 0);

      boolean sourceImproves = sourceNewTotal < sourceOldTotal;
      boolean targetImproves = targetNewTotal < targetOldTotal;
      boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
      boolean targetStaysSame = targetOldTotal == targetNewTotal;

      String descrOfQuality =
        (sourceImproves && targetImproves) ? "GREAT" :
          ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD" :
            (sourceStaysSame && targetStaysSame) ? "NEUTRAL" :
              "BAD";

      System.out.printf(
        "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
          + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
        action.getFromServer(),
        action.getFromRegion(),
        action.getToServer(),
        action.getToRegion(),
        cellToOffer,
        cellCountsOnServerToImprove.get(cellToOffer),
        cellToImprove,
        cellCountsOnServerToImprove.get(cellToImprove),
        cellToImprove,
        cellsOnServer.get(cellToImprove),
        cellToOffer,
        cellsOnServer.get(cellToOffer),
        descrOfQuality
      );
      LOG.debug("Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
          + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
        action.getFromServer(),
        action.getFromRegion(),
        action.getToServer(),
        action.getToRegion(),
        cellToOffer,
        cellCountsOnServerToImprove.get(cellToOffer),
        cellToImprove,
        cellCountsOnServerToImprove.get(cellToImprove),
        cellToImprove,
        cellsOnServer.get(cellToImprove),
        cellToOffer,
        cellsOnServer.get(cellToOffer),
        descrOfQuality
      );
    }

    return action;
  }

  private SwapRegionsAction swap(
    int receivingServer,
    short cellToGiveToReceivingServer,
    int offeringServer,
    short cellToOfferFromReceivingServerToOrigin,
    BalancerClusterState cluster
  ) {
    Multimap<Integer, Short> cellsByRegionForReceivingServer =
      computeCellsByRegion(cluster.regionsPerServer[receivingServer], cluster.regions);
    Multimap<Integer, Short> cellsByRegionForOfferingServer =
      computeCellsByRegion(cluster.regionsPerServer[offeringServer], cluster.regions);

    return (SwapRegionsAction) getAction(
      offeringServer, pickRegionForCell(cellsByRegionForOfferingServer, cellToGiveToReceivingServer),
      receivingServer, pickRegionForCell(cellsByRegionForReceivingServer, cellToOfferFromReceivingServerToOrigin)
    );
  }

  private int pickRegionForCell(Multimap<Integer, Short> cellsByRegionOnServer, short cellToMove) {
    return cellsByRegionOnServer.keySet().stream()
      .filter(region -> cellsByRegionOnServer.get(region).contains(cellToMove))
      .min(Comparator.comparingInt(region -> cellsByRegionOnServer.get(region).size()))
      .orElseGet(() -> -1);
  }

  static List<Integer> computeCellsPerRs(BalancerClusterState cluster) {
    List<Map<Short, Integer>> cellGroupSizesPerServer =
      IntStream.range(0, cluster.regionsPerServer.length).mapToObj(
        serverIndex -> computeCellGroupSizes(cluster, serverIndex,
          cluster.regionsPerServer[serverIndex])).collect(Collectors.toList());
    return cellGroupSizesPerServer.stream().map(Map::size).collect(Collectors.toList());
  }

  private Pair<Short, Integer> pickMostFrequentCellOnAnyUnsaturatedServer(
    List<Map<Short, Integer>> cellGroupSizesPerServer, int[] cellCounts, BalancerClusterState cluster) {
    cluster.sortServersByRegionCount();
    int[][] regionsPerServer = cluster.regionsPerServer;

    Pair<Short, Integer> mostFrequentCellOnServer = Pair.newPair((short) -1, -1);

    int targetCellsPerServer = Ints.checkedCast(
      (long) Math.ceil((double) HubSpotCellCostFunction.MAX_CELL_COUNT / cluster.numServers));
    int highestCellCountSoFar = Integer.MIN_VALUE;
    double mostCellsReservoirRandom = -1;

    for (int serverIndex = 0; serverIndex < regionsPerServer.length; serverIndex++) {
      int[] regionsForServer = regionsPerServer[serverIndex];
      Map<Short, Integer> cellsOnServer = cellGroupSizesPerServer.get(serverIndex);

      Set<Short> cellsOnThisServerAndOthers =
        cellsOnServer.keySet().stream().filter(cell -> cellsOnServer.get(cell) < cellCounts[cell])
          .collect(Collectors.toSet());

      if (cellsOnServer.keySet().size() <= targetCellsPerServer
      // if we have a small cell where the entire cell is local, we MUST have at least 2 cells on this server to have
      // an overall region balance, so allow us to go over the target by 1 cell
        || cellsOnThisServerAndOthers.size() == 1) {
        continue;
      }

      List<Map.Entry<Short, Integer>> cellsByFrequencyDesc =
        cellsOnServer.entrySet().stream().sorted(Map.Entry.comparingByValue())
          .collect(Collectors.toList());

      if (cellsByFrequencyDesc.isEmpty()) {
        continue;
      }


      int probe = cellsByFrequencyDesc.size() - 1;
      short mostFrequentCellTemp = -1;
      int mostFrequentCellCountTemp = -1;

      do {
        Map.Entry<Short, Integer> entry = cellsByFrequencyDesc.get(probe);
        mostFrequentCellTemp = entry.getKey();
        mostFrequentCellCountTemp = entry.getValue();
        probe--;
      } while(mostFrequentCellCountTemp == cellCounts[mostFrequentCellTemp] && probe >= 0);

      final short mostFrequentCell = mostFrequentCellTemp;
      final int mostFrequentCellCount = mostFrequentCellCountTemp;

      // if we've collected all of the regions for a given cell on one server, we can't improve
      if (mostFrequentCellCount == cellCounts[mostFrequentCell]) {
        continue;
      }

      long numServersWithMostFrequentCellNotSaturated =
        cellGroupSizesPerServer.stream().filter(cellMap -> cellMap.containsKey(mostFrequentCell))
          .filter(cellMap -> cellMap.keySet().size() > 1).count();
      // if we're down to only one server unsaturated with the most frequent cell, there are no good swaps
      if (numServersWithMostFrequentCellNotSaturated == 1) {
        continue;
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Server {} has {} regions, which have {} cells", serverIndex,
          Arrays.stream(regionsForServer).boxed().sorted().collect(Collectors.toList()),
          cellsOnServer.size());
      }

      // we don't know how many servers have the same cell count, so use a simplified online
      // reservoir sampling approach (http://gregable.com/2007/10/reservoir-sampling.html)
      if (mostFrequentCellCount > highestCellCountSoFar) {
        mostFrequentCellOnServer = Pair.newPair(mostFrequentCell, serverIndex);
        highestCellCountSoFar = mostFrequentCellCount;
        mostCellsReservoirRandom = ThreadLocalRandom.current().nextDouble();
      } else if (mostFrequentCellCount == highestCellCountSoFar) {
        double maxCellRandom = ThreadLocalRandom.current().nextDouble();
        if (maxCellRandom > mostCellsReservoirRandom) {
          mostFrequentCellOnServer = Pair.newPair(mostFrequentCell, serverIndex);
          mostCellsReservoirRandom = maxCellRandom;
        }
      }
    }

    return mostFrequentCellOnServer;
  }

  private Pair<Short, Integer> pickLeastFrequentCellOnMostLoadedServer(
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int[] cellCounts,
    BalancerClusterState cluster
  ) {
    int targetCellsPerServer = Ints.checkedCast(
      (long) Math.ceil((double) HubSpotCellCostFunction.MAX_CELL_COUNT / cluster.numServers));

    int highestLoadedServer = IntStream.range(0, cluster.numServers).boxed()
      .sorted(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).keySet().size()))
      .collect(Collectors.toList()).get(cluster.numServers - 1);

    Map<Short, Integer> cellCountsForHighestLoadedServer = cellGroupSizesPerServer.get(highestLoadedServer);
    int numCellsOnHighestLoadedServer = cellCountsForHighestLoadedServer.keySet().size();

    if (numCellsOnHighestLoadedServer <= targetCellsPerServer + 1) {
      return Pair.newPair((short) -1, -1);
    }

    return Pair.newPair(pickLeastFrequentCell(cellCountsForHighestLoadedServer), highestLoadedServer);
  }

  private static Map<Short, Integer> computeCellGroupSizes(BalancerClusterState cluster,
    int serverIndex, int[] regionsForServer) {
    Map<Short, Integer> cellGroupSizes = new HashMap<>();
    int[] cellCounts = new int[HubSpotCellCostFunction.MAX_CELL_COUNT];

    for (int regionIndex : regionsForServer) {
      if (regionIndex < 0 || regionIndex > cluster.regions.length) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping region {} because it's <0 or >{}", regionIndex,
            regionsForServer.length);
        }
        continue;
      }

      RegionInfo region = cluster.regions[regionIndex];

      if (!region.getTable().getNamespaceAsString().equals("default")) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping region {} because it's not in the default namespace",
            region.getTable().getNameWithNamespaceInclAsString());
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
        cellCounts[i]++;
      }

      if (!HubSpotCellCostFunction.isStopExclusive(endKey)) {
        cellCounts[endCellId]++;
      }
    }

    for (short c = 0; c < cellCounts.length; c++) {
      if (cellCounts[c] > 0) {
        cellGroupSizes.put(c, cellCounts[c]);
      }
    }

    return cellGroupSizes;
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
