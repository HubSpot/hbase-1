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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;

@InterfaceAudience.Private class HubSpotCellBasedCandidateGenerator extends CandidateGenerator {
  private static final int NO_REGION = -1;

  private static final Logger LOG =
    LoggerFactory.getLogger(HubSpotCellBasedCandidateGenerator.class);

  @Override BalanceAction generate(BalancerClusterState cluster) {
    if (cluster.tables.stream().noneMatch(HubSpotCellUtilities.TABLES_TO_BALANCE::contains)) {
      return BalanceAction.NULL_ACTION;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Running HubSpotCellBasedCandidateGenerator with {} servers and {} regions for tables {}",
        cluster.regionsPerServer.length, cluster.regions.length, cluster.tables);
    }

    int[] cellCounts = new int[HubSpotCellUtilities.MAX_CELL_COUNT];
    Arrays.stream(cluster.regions)
      .flatMap(region -> HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(), HubSpotCellUtilities.MAX_CELL_COUNT).stream())
      .forEach(cellOnRegion -> cellCounts[cellOnRegion]++);
    double[] cellPercents = new double[HubSpotCellUtilities.MAX_CELL_COUNT];
    for (int i = 0; i < cellCounts.length; i++) {
      cellPercents[i] = (double) cellCounts[i] / cluster.numRegions;
    }

    List<Map<Short, Integer>> cellGroupSizesPerServer =
      Arrays.stream(cluster.regionsPerServer).map(regionsForServer -> computeCellGroupSizes(cluster, regionsForServer)).collect(Collectors.toList());

    return generateAction(cluster, cellCounts, cellGroupSizesPerServer);
  }

  private BalanceAction generateAction(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer
  ) {
    int targetRegionsPerServer = Ints.checkedCast(
      (long) Math.floor((double) cluster.numRegions / cluster.numServers));

    int numTimesCellRegionsFillAllServers = 0;
    for (int cell = 0; cell < HubSpotCellUtilities.MAX_CELL_COUNT; cell++) {
      int numRegionsForCell = cellCounts[cell];
      numTimesCellRegionsFillAllServers += Ints.checkedCast((long) Math.floor((double) numRegionsForCell / cluster.numServers));
    }

    int targetCellsPerServer = targetRegionsPerServer - numTimesCellRegionsFillAllServers;
    BalanceAction moveRegionToUnderloadedServer = tryMoveRegionToSomeUnderloadedServer(cluster, cellCounts, cellGroupSizesPerServer, targetRegionsPerServer);

    if (moveRegionToUnderloadedServer != BalanceAction.NULL_ACTION) {
      return moveRegionToUnderloadedServer;
    }

    return swapRegionsToIncreaseDistinctCellsPerServer(cluster, cellCounts, cellGroupSizesPerServer, targetCellsPerServer);
  }

  private BalanceAction tryMoveRegionToSomeUnderloadedServer(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int targetRegionsPerServer
  ) {
    Optional<Integer> toServerMaybe = pickUnderloadedServer(cluster, targetRegionsPerServer);
    if (!toServerMaybe.isPresent()) {
      return BalanceAction.NULL_ACTION;
    }

    int toServer = toServerMaybe.get();
    Optional<Integer> fromServerMaybe = pickOverloadedServer(cluster, targetRegionsPerServer);
    if (!fromServerMaybe.isPresent()) {
      return BalanceAction.NULL_ACTION;
    }
    int fromServer = fromServerMaybe.get();
    short cell = pickCellToMove(cluster, cellCounts, cellGroupSizesPerServer.get(fromServer));

    return moveCell("fill underloaded", fromServer, cell, toServer, cellGroupSizesPerServer, cluster);
  }

  private Optional<Integer> pickOverloadedServer(BalancerClusterState cluster, int targetRegionsPerServer) {
    for (int server = 0; server < cluster.numServers; server++) {
      if (cluster.regionsPerServer[server].length > targetRegionsPerServer) {
        return Optional.of(server);
      }
    }

    return Optional.empty();
  }

  private Optional<Integer> pickUnderloadedServer(BalancerClusterState cluster, int targetRegionsPerServer) {
    for (int server = 0; server < cluster.numServers; server++) {
      if (cluster.regionsPerServer[server].length < targetRegionsPerServer) {
        return Optional.of(server);
      }
    }

    return Optional.empty();
  }

  private BalanceAction swapRegionsToIncreaseDistinctCellsPerServer(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int targetCellsPerServer
  ) {
    Optional<Integer> fromServerMaybe = pickServerWithoutEnoughIsolation(cluster, cellGroupSizesPerServer, targetCellsPerServer);
    if (!fromServerMaybe.isPresent()) {
      return BalanceAction.NULL_ACTION;
    }
    int fromServer = fromServerMaybe.get();
    short fromCell = pickCellToMove(cluster, cellCounts, cellGroupSizesPerServer.get(fromServer));

    Optional<Pair<Short, Integer>> toCellMaybe = pickCellOnServerNotPresentOnSource(cluster, cellCounts, cellGroupSizesPerServer, fromServer, fromCell);
    if (!toCellMaybe.isPresent()) {
      return BalanceAction.NULL_ACTION;
    }

    short toCell = toCellMaybe.get().getFirst();
    int toServer = toCellMaybe.get().getSecond();

    return swapCells("swap", fromServer, fromCell, toServer, toCell, cellGroupSizesPerServer, cluster);
  }

  private Optional<Pair<Short, Integer>> pickCellOnServerNotPresentOnSource(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int fromServer,
    short cell
  ) {
    Map<Short, Integer> countsForFromServer = cellGroupSizesPerServer.get(fromServer);
    Optional<Pair<Short, Integer>> result = Optional.empty();

    // randomly select one using a simplified inline reservoir sample
    // See: http://gregable.com/2007/10/reservoir-sampling.html
    double reservoirRandom = -1;
    for (int server = 0; server < cluster.numServers; server++) {
      if (server == fromServer) {
        continue;
      }

      Map<Short, Integer> countsForToCandidate = cellGroupSizesPerServer.get(server);
      Set<Short> candidateCellsOnTo = new HashSet<>();
      for (short cellOnTo : countsForToCandidate.keySet()) {
        int regionsForCell = cellCounts[cellOnTo];
        int expectedCountOnAllServers = Ints.checkedCast((long) Math.floor((double) regionsForCell / cluster.numServers));

        if (!countsForFromServer.containsKey(cellOnTo) || countsForFromServer.get(cellOnTo) <= expectedCountOnAllServers) {
          candidateCellsOnTo.add(cellOnTo);
        }
      }

      if (!countsForToCandidate.containsKey(cell) &&
        !candidateCellsOnTo.isEmpty()) {
        double candidateRandom = ThreadLocalRandom.current().nextDouble();
        if (candidateRandom > reservoirRandom) {
          reservoirRandom = candidateRandom;
          result = Optional.of(Pair.newPair(candidateCellsOnTo.stream().findAny().get(), server));
        }
      }
    }

    return result;
  }

  private Optional<Integer> pickServerWithoutEnoughIsolation(
    BalancerClusterState cluster,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    int targetCellsPerServer
  ) {
    // randomly select one using a simplified inline reservoir sample
    // See: http://gregable.com/2007/10/reservoir-sampling.html
    Optional<Integer> result = Optional.empty();
    int lowestSoFar = Integer.MAX_VALUE;
    double reservoirRandom = -1;

    for (int server = 0; server < cluster.numServers; server++) {
      int numCellsOnServer = cellGroupSizesPerServer.get(server).keySet().size();
      if (numCellsOnServer < targetCellsPerServer) {
        if (numCellsOnServer < lowestSoFar) {
          lowestSoFar = numCellsOnServer;
          reservoirRandom = ThreadLocalRandom.current().nextDouble();
          result = Optional.of(server);
        } else if (numCellsOnServer == lowestSoFar) {
          double candidateRandom = ThreadLocalRandom.current().nextDouble();
          if (candidateRandom > reservoirRandom) {
            reservoirRandom = candidateRandom;
            result = Optional.of(server);
          }
        }
      }
    }

    return result;
  }

  private short pickCellToMove(BalancerClusterState cluster, int[] cellCounts, Map<Short, Integer> cellCountsForServer) {
    return cellCountsForServer.keySet().stream()
      .max(Comparator.comparing(cell -> {
        int regionsForCell = cellCounts[cell];
        int expectedCountOnAllServers = Ints.checkedCast((long) Math.floor((double) regionsForCell / cluster.numServers));

        return cellCountsForServer.get(cell) - expectedCountOnAllServers;
      }))
      .get();
  }

  private MoveRegionAction moveCell(
    String originStep,
    int fromServer, short fromCell,
    int toServer,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    BalancerClusterState cluster
  ) {
    if (LOG.isDebugEnabled()) {
      Map<Short, Integer> fromCounts = cellGroupSizesPerServer.get(fromServer);
      Map<Short, Integer> toCounts = cellGroupSizesPerServer.get(toServer);

      String fromCountsString = fromCounts.values().stream().mapToInt(x -> x).sum() + "." +
        fromCounts.entrySet().stream().map(entry -> (entry.getKey() == fromCell ? "**" : "") + entry.getKey() + "=" + entry.getValue() + (entry.getKey() == fromCell ? "**" : ""))
          .collect(Collectors.joining(", ", "{", "}"));
      String toCountsString = toCounts.values().stream().mapToInt(x -> x).sum() + "." +
        toCounts.entrySet().stream().map(entry -> (entry.getKey() == fromCell ? "!!" : "") + entry.getKey() + "=" + entry.getValue() + (entry.getKey() == fromCell ? "!!" : ""))
          .collect(Collectors.joining(", ", "{", "}"));

      LOG.debug("{}", String.format("[%20s]\t\tmove %d:%d -> %d %s -> %s\n",
        originStep,
        fromServer, fromCell,
        toServer, fromCountsString, toCountsString));
    }

    return (MoveRegionAction) getAction(fromServer, resolveCellToRegion(cluster, fromServer, fromCell), toServer, NO_REGION);
  }

  private SwapRegionsAction swapCells(
    String originStep,
    int fromServer, short fromCell,
    int toServer, short toCell,
    List<Map<Short, Integer>> cellGroupSizesPerServer,
    BalancerClusterState cluster
  ) {
    if (LOG.isDebugEnabled()) {
      Map<Short, Integer> fromCounts = cellGroupSizesPerServer.get(fromServer);
      Map<Short, Integer> toCounts = cellGroupSizesPerServer.get(toServer);

      String fromCountsString = fromCounts.values().stream().mapToInt(x -> x).sum() + "." +
        fromCounts.entrySet().stream().map(entry -> (entry.getKey() == fromCell ? "**" : "") + (entry.getKey() == toCell ? "!!" : "") + entry.getKey() + "=" + entry.getValue() + (entry.getKey() == fromCell ? "**" : "") + (entry.getKey() == toCell ? "!!" : ""))
          .collect(Collectors.joining(", ", "{", "}"));
      String toCountsString = toCounts.values().stream().mapToInt(x -> x).sum() + "." +
        toCounts.entrySet().stream().map(entry -> (entry.getKey() == toCell ? "**" : "") + (entry.getKey() == fromCell ? "!!" : "") + entry.getKey() + "=" + entry.getValue() + (entry.getKey() == toCell ? "**" : "") + (entry.getKey() == fromCell ? "!!" : ""))
          .collect(Collectors.joining(", ", "{", "}"));

      LOG.debug("{}", String.format("[%20s]\t\tswap %3d:%3d <-> %3d:%3d %s <-> %s\n",
        originStep,
        fromServer, fromCell,
        toServer, toCell, fromCountsString, toCountsString));
    }

    return (SwapRegionsAction) getAction(
      fromServer,
      resolveCellToRegion(cluster, fromServer, fromCell),
      toServer,
      resolveCellToRegion(cluster, toServer, toCell)
    );
  }

  private int resolveCellToRegion(BalancerClusterState cluster, int server, short cell) {
    Multimap<Integer, Short> cellsByRegion =
      computeCellsByRegion(cluster.regionsPerServer[server], cluster.regions);
    return pickRegionForCell(cellsByRegion, cell);
  }

  private int pickRegionForCell(Multimap<Integer, Short> cellsByRegionOnServer, short cellToMove) {
    return cellsByRegionOnServer.keySet().stream()
      .filter(region -> cellsByRegionOnServer.get(region).contains(cellToMove))
      .min(Comparator.comparingInt(region -> cellsByRegionOnServer.get(region).size()))
      .orElseGet(() -> NO_REGION);
  }

  private static Map<Short, Integer> computeCellGroupSizes(BalancerClusterState cluster, int[] regionsForServer) {
    Map<Short, Integer> cellGroupSizes = new HashMap<>();
    int[] cellCounts = new int[HubSpotCellUtilities.MAX_CELL_COUNT];

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

      HubSpotCellUtilities.range(region.getStartKey(), region.getEndKey(), HubSpotCellUtilities.MAX_CELL_COUNT)
        .forEach(cell -> cellCounts[cell]++);
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

      HubSpotCellUtilities.range(region.getStartKey(), region.getEndKey(), HubSpotCellUtilities.MAX_CELL_COUNT)
        .forEach(cell -> resultBuilder.put(regionIndex, cell));
    }
    return resultBuilder.build();
  }
}
