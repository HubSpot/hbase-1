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

@InterfaceAudience.Private
class HubSpotCellBasedCandidateGenerator extends CandidateGenerator {
  private static final int NO_SERVER = -1;
  private static final int NO_REGION = -1;
  private static final boolean DEBUG_MAJOR = false;
  private static final boolean DEBUG_MINOR = false;

  private static final Logger LOG =
    LoggerFactory.getLogger(HubSpotCellBasedCandidateGenerator.class);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
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
      .flatMap(region -> HubSpotCellCostFunction
        .toCells(region.getStartKey(), region.getEndKey(), HubSpotCellCostFunction.MAX_CELL_COUNT)
        .stream())
      .forEach(cellOnRegion -> cellCounts[cellOnRegion]++);
    double[] cellPercents = new double[HubSpotCellCostFunction.MAX_CELL_COUNT];
    for (int i = 0; i < cellCounts.length; i++) {
      cellPercents[i] = (double) cellCounts[i] / cluster.numRegions;
    }

    List<Map<Short, Integer>> cellGroupSizesPerServer =
      IntStream.range(0, cluster.regionsPerServer.length)
        .mapToObj(serverIndex -> computeCellGroupSizes(cluster, serverIndex,
          cluster.regionsPerServer[serverIndex]))
        .collect(Collectors.toList());

    return generateAction(cluster, cellCounts, cellGroupSizesPerServer);
  }

  private BalanceAction generateAction(BalancerClusterState cluster, int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer) {
    int targetRegionsPerServer =
      Ints.checkedCast((long) Math.ceil((double) cluster.numRegions / cluster.numServers));
    List<Integer> regionCounts = Arrays.stream(cluster.regionsPerServer)
      .map(regions -> regions.length).collect(Collectors.toList());

    List<Map<Short, Integer>> bigServers = cellGroupSizesPerServer.stream()
      .filter(e -> e.keySet().size() > 7).collect(Collectors.toList());
    Map<Short, Integer> collective = new HashMap<>();
    bigServers.forEach(e -> e.forEach((k, v) -> collective.merge(k, v, Integer::sum)));

    List<Integer> underloadedServers = IntStream.range(0, cluster.numServers)
      .filter(server -> cluster.regionsPerServer[server].length < targetRegionsPerServer - 1)
      .boxed().collect(Collectors.toList());

    // Step 1: if a previous action unbalanced us, try to rebalance region balance to be within
    // plus/minus 1 of the target
    if (!underloadedServers.isEmpty()) {
      List<Integer> serversThatCanLoseOneRegion = IntStream.range(0, cluster.numServers)
        .filter(server -> cluster.regionsPerServer[server].length >= targetRegionsPerServer).boxed()
        .collect(Collectors.toList());

      return moveRegionFromOverloadedToUnderloaded(serversThatCanLoseOneRegion, underloadedServers,
        cellGroupSizesPerServer, cluster);
    }

    // Step 2: knowing we have region balance, try to expand the highest frequency cell(s) via swaps
    Pair<Short, Integer> cellOnServer =
      pickMostFrequentCellOnAnyUnsaturatedServer(cellGroupSizesPerServer, cellCounts, cluster);

    if (cellOnServer.getSecond() != NO_SERVER) {
      return swapSomeRegionToImprove(cellOnServer, cellGroupSizesPerServer, cluster);
    }

    // Step 3: balanced regions, and many/most servers are full now. We have a lot of smaller
    // disconnected pieces
    // left to sort out. Pick the most loaded server, and try to reduce the cell count by 1. We can
    // either swap
    // if possible, or give away if not. We're allowed to slightly imbalance here, knowing that
    // subsequent rounds
    // will use step (1) to repair the imbalance.
    cellOnServer =
      pickLeastFrequentCellOnMostLoadedServer(cellGroupSizesPerServer, cellCounts, cluster);

    if (cellOnServer.getSecond() == NO_SERVER) {
      return BalanceAction.NULL_ACTION;
    }

    BalanceAction swapAttempt =
      giveAwayRegionViaSwap(cellOnServer, cellGroupSizesPerServer, cluster);

    if (swapAttempt != BalanceAction.NULL_ACTION) {
      return swapAttempt;
    }

    return giveAwaySomeRegionToImprove(cellOnServer, cellGroupSizesPerServer, cellCounts, cluster);
  }

  private Pair<Short, Integer> pickSecondMostFrequentCellOnAnyUnsaturatedServer(
    List<Map<Short, Integer>> cellGroupSizesPerServer, int[] cellCounts,
    BalancerClusterState cluster) {
    return IntStream.range(0, cluster.numServers).boxed()
      .filter(server -> cellGroupSizesPerServer.get(server).size() > 1)
      .map(
        server -> Pair.newPair(get2ndMostFrequentCell(cellGroupSizesPerServer.get(server)), server))
      .sorted(Comparator
        .comparing(pair -> -1 * cellGroupSizesPerServer.get(pair.getSecond()).get(pair.getFirst())))
      .findFirst().orElseGet(() -> Pair.newPair((short) -1, NO_SERVER));
  }

  private short get2ndMostFrequentCell(Map<Short, Integer> countOfCells) {
    short mostFrequent = pickMostFrequentCell(countOfCells);
    return countOfCells.keySet().stream().filter(cell -> cell != mostFrequent)
      .max(Comparator.comparing(countOfCells::get)).get();
  }

  private BalanceAction giveAwayRegionViaSwap(Pair<Short, Integer> cellOnServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer, BalancerClusterState cluster) {
    short sourceCell = cellOnServer.getFirst();
    int sourceServer = cellOnServer.getSecond();

    Map<Short, Integer> sourceCellCounts = cellGroupSizesPerServer.get(sourceServer);
    Set<Short> sourceCells = sourceCellCounts.keySet();

    Optional<Integer> otherServerWithSharedCellAndMostOfTheCellToGiveAway =
      IntStream.range(0, cluster.numServers).boxed().filter(server -> server != sourceServer)
        .filter(server -> cellGroupSizesPerServer.get(server).containsKey(sourceCell))
        .filter(server -> Sets
          .intersection(cellGroupSizesPerServer.get(server).keySet(), sourceCells).size() > 1)
        .max(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).get(sourceCell)));

    if (!otherServerWithSharedCellAndMostOfTheCellToGiveAway.isPresent()) {
      return BalanceAction.NULL_ACTION;
    }

    int targetServer = otherServerWithSharedCellAndMostOfTheCellToGiveAway.get();
    Map<Short, Integer> targetCells = cellGroupSizesPerServer.get(targetServer);

    short targetCell = targetCells.keySet().stream().filter(cell -> cell != sourceCell)
      .filter(sourceCells::contains).findAny().get();

    return swapCells(sourceServer, sourceCell, targetServer, targetCell, cluster);
  }

  private BalanceAction moveRegionFromOverloadedToUnderloaded(List<Integer> overloadedServers,
    List<Integer> underloadedServers, List<Map<Short, Integer>> cellGroupSizesPerServer,
    BalancerClusterState cluster) {
    List<Integer> overloadedServersMostToLeastCells = overloadedServers.stream()
      .sorted(
        Comparator.comparing(server -> -1 * cellGroupSizesPerServer.get(server).keySet().size()))
      .collect(Collectors.toList());
    // if there's a server w/ excess that has a single instance of a cell that we already have,
    // prioritize that first (easy +2)
    for (int source : overloadedServersMostToLeastCells) {
      for (int target : underloadedServers) {
        Map<Short, Integer> cellsOnSource = cellGroupSizesPerServer.get(source);
        Map<Short, Integer> cellsOnTarget = cellGroupSizesPerServer.get(target);

        List<Short> singletonCellsOnSourceWeCanMoveToTarget =
          cellsOnSource.keySet().stream().filter(cell -> cellsOnSource.get(cell) == 1)
            .filter(cellsOnTarget::containsKey).collect(Collectors.toList());

        if (!singletonCellsOnSourceWeCanMoveToTarget.isEmpty()) {
          Multimap<Integer, Short> cellsByRegionOnSource =
            computeCellsByRegion(cluster.regionsPerServer[source], cluster.regions);
          short cellToMove = singletonCellsOnSourceWeCanMoveToTarget.get(
            ThreadLocalRandom.current().nextInt(singletonCellsOnSourceWeCanMoveToTarget.size()));

          return getAction(source, pickRegionForCell(cellsByRegionOnSource, cellToMove), target,
            -1);
        }
      }
    }

    int target =
      underloadedServers.get(ThreadLocalRandom.current().nextInt(underloadedServers.size()));

    // if there's a server w/ excess that has a singleton cell we don't have but only one instance,
    // accept it
    // (0, neutral)
    for (int source : overloadedServersMostToLeastCells) {
      Map<Short, Integer> cellCountsOnServer = cellGroupSizesPerServer.get(source);
      short leastFrequentCell = pickLeastFrequentCell(cellCountsOnServer);
      if (cellCountsOnServer.get(leastFrequentCell) == 1) {
        return getAction(source,
          pickRegionForCell(computeCellsByRegion(cluster.regionsPerServer[source], cluster.regions),
            leastFrequentCell),
          target, NO_REGION);
      }
    }

    // ok, we give up. just pick a random region from the least loaded cell of some instance and
    // call it a day
    // this will be (-1) but allows balancing to continue
    int source = overloadedServersMostToLeastCells.get(
      ThreadLocalRandom.current().nextInt(Math.min(overloadedServersMostToLeastCells.size(), 5)));
    short cellToMove = pickLeastFrequentCell(cellGroupSizesPerServer.get(source));

    Multimap<Integer, Short> cellsByRegionForSource =
      computeCellsByRegion(cluster.regionsPerServer[source], cluster.regions);
    return getAction(source, pickRegionForCell(cellsByRegionForSource, cellToMove), target,
      NO_REGION);
  }

  private BalanceAction giveAwaySomeRegionToImprove(Pair<Short, Integer> cellOnServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer, int[] cellCounts,
    BalancerClusterState cluster) {

    short cell = cellOnServer.getFirst();
    int sourceServer = cellOnServer.getSecond();

    Map<Short, Integer> cellCountsOnSource = cellGroupSizesPerServer.get(sourceServer);
    Set<Short> cellsOnSource = cellCountsOnSource.keySet();

    Optional<Integer> otherServerWithThisCell =
      pickOtherServerWithThisCellToGiveItTo(cell, sourceServer, cellGroupSizesPerServer, cluster);

    int targetServer = NO_SERVER;

    if (otherServerWithThisCell.isPresent()) {
      targetServer = otherServerWithThisCell.get();
    } else {
      Optional<Integer> lowerLoadedServer =
        pickOtherLowerLoadedServerToGiveCell(sourceServer, cellGroupSizesPerServer, cluster);

      if (lowerLoadedServer.isPresent()) {
        targetServer = lowerLoadedServer.get();
      }
    }

    if (targetServer == NO_SERVER) {
      return BalanceAction.NULL_ACTION;
    }

    MoveRegionAction action = (MoveRegionAction) getAction(sourceServer,
      pickRegionForCell(
        computeCellsByRegion(cluster.regionsPerServer[sourceServer], cluster.regions), cell),
      targetServer, NO_REGION);

    if (LOG.isDebugEnabled() || DEBUG_MINOR) {
      Map<Short, Integer> cellsOnTarget = cellGroupSizesPerServer.get(targetServer);
      int sourceOldTotal = cellsOnSource.size();
      int sourceNewTotal = cellsOnSource.size() - (cellCountsOnSource.get(cell) == 1 ? 1 : 0);
      int targetOldTotal = cellsOnTarget.size();
      int targetNewTotal = cellsOnTarget.size() - (cellsOnTarget.get(cell) == 1 ? 1 : 0);

      boolean sourceImproves = sourceNewTotal < sourceOldTotal;
      boolean targetImproves = targetNewTotal < targetOldTotal;
      boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
      boolean targetStaysSame = targetOldTotal == targetNewTotal;

      String descrOfQuality = (sourceImproves && targetImproves) ? "GREAT"
        : ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD"
        : (sourceStaysSame && targetStaysSame) ? "NEUTRAL"
        : "BAD";

      // System.out.printf(
      // "Moving s%d.r%d -> s%d [cell = %d]. SOURCE has %d copies, TARGET has %d copies. Change is
      // %s\n",
      // action.getFromServer(),
      // action.getRegion(),
      // action.getToServer(),
      // cell,
      // cellCountsOnSource.get(cell),
      // cellsOnTarget.get(cell),
      // descrOfQuality
      // );
      LOG.debug(
        "Moving s{}.r{} -> s{} [cell = {}]. SOURCE has {} copies, TARGET has {} copies. Change is {}",
        action.getFromServer(), action.getRegion(), action.getToServer(), cell,
        cellCountsOnSource.get(cell), cellsOnTarget.get(cell), descrOfQuality);
    }

    return action;
  }

  private Optional<Integer> pickOtherLowerLoadedServerToGiveCell(int sourceServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer, BalancerClusterState cluster) {
    List<Integer> serversByCellCountAsc =
      IntStream.range(0, cluster.numServers).boxed().filter(server -> server != sourceServer)
        .sorted(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).keySet().size()))
        .collect(Collectors.toList());

    int serverToPick = NO_SERVER;
    int lowestCountSoFar = Integer.MAX_VALUE;
    double reservoirRandom = -1;

    for (int server : serversByCellCountAsc) {
      int cellCount = cellGroupSizesPerServer.get(server).keySet().size();
      if (cellCount < lowestCountSoFar) {
        serverToPick = server;
        lowestCountSoFar = cellCount;
        reservoirRandom = ThreadLocalRandom.current().nextDouble();
      } else if (cellCount == lowestCountSoFar) {
        double serverRandom = ThreadLocalRandom.current().nextDouble();
        if (serverRandom > reservoirRandom) {
          serverToPick = server;
          reservoirRandom = serverRandom;
        }
      }
    }

    return Optional.of(serverToPick).filter(server -> server != NO_SERVER);
  }

  private Optional<Integer> pickOtherServerWithThisCellToGiveItTo(short cell, int sourceServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer, BalancerClusterState cluster) {
    return IntStream.range(0, cluster.numServers).boxed().filter(server -> server != sourceServer)
      .filter(server -> cellGroupSizesPerServer.get(server).containsKey(cell))
      .filter(server -> cluster.regionsPerServer[server].length
          <= Math.ceil((double) cluster.numRegions / cluster.numServers))
      .max(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).get(cell)));
  }

  private short pickLeastFrequentCell(Map<Short, Integer> cellCounts) {
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

  private short pickMostFrequentCell(Map<Short, Integer> cellCounts) {
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

    short sourceCell = cellOnServer.getFirst();
    int targetServer = cellOnServer.getSecond();

    Map<Short, Integer> cellCountsOnTargetServer = cellGroupSizesPerServer.get(targetServer);
    Set<Short> cellsOnTargetServer = cellCountsOnTargetServer.keySet();

    if (cluster.regionsPerServer[targetServer].length == 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} has no regions", targetServer);
      }
      return BalanceAction.NULL_ACTION;
    }

    Set<Integer> sourceCandidateSet = new HashSet<>();
    for (int sourceServerCandidate = 0; sourceServerCandidate
        < cellGroupSizesPerServer.size(); sourceServerCandidate++) {
      if (sourceServerCandidate == targetServer) {
        continue;
      }

      Map<Short, Integer> cellsOnSourceCandidate =
        cellGroupSizesPerServer.get(sourceServerCandidate);

      // if that server is perfectly isolated, don't allow that to be broken even to fix another
      if (cellsOnSourceCandidate.keySet().size() == 1) {
        continue;
      }

      if (cellsOnSourceCandidate.containsKey(sourceCell)) {
        sourceCandidateSet.add(sourceServerCandidate);

        Sets.SetView<Short> cellsInCommon =
          Sets.intersection(cellsOnTargetServer, cellsOnSourceCandidate.keySet());

        if (cellsInCommon.size() > 1) {
          short commonCellToSwap =
            cellsInCommon.stream().filter(cell -> cell != sourceCell).findAny().get();
          SwapRegionsAction action =
            swapCells(sourceServerCandidate, sourceCell, targetServer, commonCellToSwap, cluster);
          if (LOG.isDebugEnabled() || DEBUG_MAJOR) {
            int sourceOldTotal = cellsOnSourceCandidate.size();
            int sourceNewTotal =
              cellsOnSourceCandidate.size() - (cellsOnSourceCandidate.get(sourceCell) == 1 ? 1 : 0);
            int targetOldTotal = cellsOnTargetServer.size();
            int targetNewTotal = cellCountsOnTargetServer.size()
              - (cellCountsOnTargetServer.get(commonCellToSwap) == 1 ? 1 : 0);

            boolean sourceImproves = sourceNewTotal < sourceOldTotal;
            boolean targetImproves = targetNewTotal < targetOldTotal;
            boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
            boolean targetStaysSame = targetOldTotal == targetNewTotal;

            String descrOfQuality = (sourceImproves && targetImproves) ? "GREAT"
              : ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame))
                ? "GOOD"
              : (sourceStaysSame && targetStaysSame) ? "NEUTRAL"
              : "BAD";

            // System.out.printf(
            // "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies),
            // "
            // + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
            // action.getFromServer(),
            // action.getFromRegion(),
            // action.getToServer(),
            // action.getToRegion(),
            // commonCellToSwap,
            // cellCountsOnTargetServer.get(commonCellToSwap),
            // sourceCell,
            // cellCountsOnTargetServer.get(sourceCell),
            // sourceCell,
            // cellsOnSourceCandidate.get(sourceCell),
            // commonCellToSwap,
            // cellsOnSourceCandidate.get(commonCellToSwap),
            // descrOfQuality
            // );
            LOG.debug(
              "Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
                + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
              action.getFromServer(), action.getFromRegion(), action.getToServer(),
              action.getToRegion(), commonCellToSwap,
              cellCountsOnTargetServer.get(commonCellToSwap), sourceCell,
              cellCountsOnTargetServer.get(sourceCell), sourceCell,
              cellsOnSourceCandidate.get(sourceCell), commonCellToSwap,
              cellsOnSourceCandidate.get(commonCellToSwap), descrOfQuality);
          }
          return action;
        }
      }
    }

    List<Integer> candidates = new ArrayList<>(sourceCandidateSet);

    if (candidates.isEmpty()) {
      // this means we've reached the end of the road for this particular cell
      return BalanceAction.NULL_ACTION;
    }

    int sourceServer = candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
    Map<Short, Integer> cellsOnSource = cellGroupSizesPerServer.get(sourceServer);
    short targetCell = cellsOnTargetServer.stream().filter(cell -> cell != sourceCell)
      .sorted(Comparator.comparing(cellCountsOnTargetServer::get)).findFirst().get();

    SwapRegionsAction action =
      swapCells(sourceServer, sourceCell, targetServer, targetCell, cluster);

    if (LOG.isDebugEnabled() || DEBUG_MAJOR) {
      int sourceOldTotal = cellsOnSource.size();
      int sourceNewTotal = cellsOnSource.size() - (cellsOnSource.get(sourceCell) == 1 ? 1 : 0);
      int targetOldTotal = cellsOnTargetServer.size();
      int targetNewTotal =
        cellCountsOnTargetServer.size() - (cellCountsOnTargetServer.get(sourceCell) == 1 ? 1 : 0);

      boolean sourceImproves = sourceNewTotal < sourceOldTotal;
      boolean targetImproves = targetNewTotal < targetOldTotal;
      boolean sourceStaysSame = sourceOldTotal == sourceNewTotal;
      boolean targetStaysSame = targetOldTotal == targetNewTotal;

      String descrOfQuality = (sourceImproves && targetImproves) ? "GREAT"
        : ((sourceStaysSame && targetImproves) || (sourceImproves && targetStaysSame)) ? "GOOD"
        : (sourceStaysSame && targetStaysSame) ? "NEUTRAL"
        : "BAD";

      // System.out.printf(
      // "Swapping s%d.r%d for s%d.r%d. SOURCE loses %d (%d copies) and gains %d (%d copies), "
      // + "TARGET loses %d (%d copies) and gains %d (%d copies). Change is %s\n",
      // action.getFromServer(),
      // action.getFromRegion(),
      // action.getToServer(),
      // action.getToRegion(),
      // sourceCell,
      // cellCountsOnTargetServer.get(sourceCell),
      // sourceCell,
      // cellCountsOnTargetServer.get(sourceCell),
      // sourceCell,
      // cellsOnSource.get(sourceCell),
      // sourceCell,
      // cellsOnSource.get(sourceCell),
      // descrOfQuality
      // );
      LOG.debug(
        "Swapping s{}.r{} to s{}.r{}. SOURCE loses {} ({} copies) and gains {} ({} copies), "
          + "TARGET loses {} ({} copies) and gains {} ({} copies). Change is {}",
        action.getFromServer(), action.getFromRegion(), action.getToServer(), action.getToRegion(),
        sourceCell, cellCountsOnTargetServer.get(sourceCell), sourceCell,
        cellCountsOnTargetServer.get(sourceCell), sourceCell, cellsOnSource.get(sourceCell),
        sourceCell, cellsOnSource.get(sourceCell), descrOfQuality);
    }

    return action;
  }

  private SwapRegionsAction swapCells(int fromServer, short fromCell, int toServer, short toCell,
    BalancerClusterState cluster) {
    return (SwapRegionsAction) getAction(fromServer,
      resolveCellToRegion(cluster, fromServer, fromCell), toServer,
      resolveCellToRegion(cluster, toServer, toCell));
  }

  private int resolveCellToRegion(BalancerClusterState cluster, int server, short cell) {
    Multimap<Integer, Short> cellsByRegion =
      computeCellsByRegion(cluster.regionsPerServer[server], cluster.regions);
    return pickRegionForCell(cellsByRegion, cell);
  }

  private SwapRegionsAction swap(int receivingServer, short cellToGiveToReceivingServer,
    int offeringServer, short cellToOfferFromReceivingServerToOrigin,
    BalancerClusterState cluster) {
    Multimap<Integer, Short> cellsByRegionForReceivingServer =
      computeCellsByRegion(cluster.regionsPerServer[receivingServer], cluster.regions);
    Multimap<Integer, Short> cellsByRegionForOfferingServer =
      computeCellsByRegion(cluster.regionsPerServer[offeringServer], cluster.regions);

    return (SwapRegionsAction) getAction(offeringServer,
      pickRegionForCell(cellsByRegionForOfferingServer, cellToGiveToReceivingServer),
      receivingServer,
      pickRegionForCell(cellsByRegionForReceivingServer, cellToOfferFromReceivingServerToOrigin));
  }

  private int pickRegionForCell(Multimap<Integer, Short> cellsByRegionOnServer, short cellToMove) {
    return cellsByRegionOnServer.keySet().stream()
      .filter(region -> cellsByRegionOnServer.get(region).contains(cellToMove))
      .min(Comparator.comparingInt(region -> cellsByRegionOnServer.get(region).size()))
      .orElseGet(() -> -1);
  }

  static List<Integer> computeCellsPerRs(BalancerClusterState cluster) {
    List<Map<Short, Integer>> cellGroupSizesPerServer =
      IntStream.range(0, cluster.regionsPerServer.length)
        .mapToObj(serverIndex -> computeCellGroupSizes(cluster, serverIndex,
          cluster.regionsPerServer[serverIndex]))
        .collect(Collectors.toList());
    return cellGroupSizesPerServer.stream().map(Map::size).collect(Collectors.toList());
  }

  private Pair<Short, Integer> pickMostFrequentCellOnAnyUnsaturatedServer(
    List<Map<Short, Integer>> cellGroupSizesPerServer, int[] cellCounts,
    BalancerClusterState cluster) {
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

      Set<Short> cellsOnThisServerAndOthers = cellsOnServer.keySet().stream()
        .filter(cell -> cellsOnServer.get(cell) < cellCounts[cell]).collect(Collectors.toSet());

      if (
        cellsOnServer.keySet().size() <= targetCellsPerServer
          // if we have a small cell where the entire cell is local, we MUST have at least 2 cells
          // on this server to have
          // an overall region balance, so allow us to go over the target by 1 cell
          || cellsOnThisServerAndOthers.size() == 1
      ) {
        continue;
      }

      List<Map.Entry<Short, Integer>> cellsByFrequencyAsc = cellsOnServer.entrySet().stream()
        .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

      if (cellsByFrequencyAsc.isEmpty()) {
        continue;
      }

      int probe = cellsByFrequencyAsc.size() - 1;
      short mostFrequentCellTemp = -1;
      int mostFrequentCellCountTemp = -1;

      do {
        Map.Entry<Short, Integer> entry = cellsByFrequencyAsc.get(probe);
        mostFrequentCellTemp = entry.getKey();
        mostFrequentCellCountTemp = entry.getValue();
        probe--;
      } while (mostFrequentCellCountTemp == cellCounts[mostFrequentCellTemp] && probe >= 0);

      final short mostFrequentCell = mostFrequentCellTemp;
      final int mostFrequentCellCount = mostFrequentCellCountTemp;

      // if we've collected all of the regions for a given cell on one server, we can't improve
      if (mostFrequentCellCount == cellCounts[mostFrequentCell]) {
        continue;
      }

      long numServersWithMostFrequentCellNotSaturated =
        cellGroupSizesPerServer.stream().filter(cellMap -> cellMap.containsKey(mostFrequentCell))
          .filter(cellMap -> cellMap.keySet().size() > 1).count();
      // if we're down to only one server unsaturated with the most frequent cell, there are no good
      // swaps
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
    List<Map<Short, Integer>> cellGroupSizesPerServer, int[] cellCounts,
    BalancerClusterState cluster) {
    int targetCellsPerServer = Ints.checkedCast(
      (long) Math.ceil((double) HubSpotCellCostFunction.MAX_CELL_COUNT / cluster.numServers));

    int highestLoadedServer = IntStream.range(0, cluster.numServers).boxed()
      .sorted(Comparator.comparing(server -> cellGroupSizesPerServer.get(server).keySet().size()))
      .collect(Collectors.toList()).get(cluster.numServers - 1);

    Map<Short, Integer> cellCountsForHighestLoadedServer =
      cellGroupSizesPerServer.get(highestLoadedServer);
    int numCellsOnHighestLoadedServer = cellCountsForHighestLoadedServer.keySet().size();

    if (numCellsOnHighestLoadedServer <= targetCellsPerServer + 1) {
      return Pair.newPair((short) -1, -1);
    }

    return Pair.newPair(pickLeastFrequentCell(cellCountsForHighestLoadedServer),
      highestLoadedServer);
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

      short startCellId = (startKey == null || startKey.length == 0)
        ? 0
        : (startKey.length >= 2
          ? Bytes.toShort(startKey, 0, 2)
          : Bytes.toShort(new byte[] { 0, startKey[0] }));
      short endCellId = (endKey == null || endKey.length == 0)
        ? (short) (HubSpotCellCostFunction.MAX_CELL_COUNT - 1)
        : (endKey.length >= 2
          ? Bytes.toShort(endKey, 0, 2)
          : Bytes.toShort(new byte[] { -1, endKey[0] }));

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

      short startCellId = (startKey == null || startKey.length == 0)
        ? 0
        : (startKey.length >= 2
          ? Bytes.toShort(startKey, 0, 2)
          : Bytes.toShort(new byte[] { 0, startKey[0] }));
      short endCellId = (endKey == null || endKey.length == 0)
        ? (short) (HubSpotCellCostFunction.MAX_CELL_COUNT - 1)
        : (endKey.length >= 2
          ? Bytes.toShort(endKey, 0, 2)
          : Bytes.toShort(new byte[] { -1, endKey[0] }));

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
