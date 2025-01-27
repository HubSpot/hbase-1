package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@InterfaceAudience.Private class PrefixIsolationCandidateGenerator
  extends PrefixCandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(PrefixIsolationCandidateGenerator.class);


  @Override BalanceAction generate(BalancerClusterState cluster) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Running PrefixIsolationCandidateGenerator with {} servers and {} regions for tables {}",
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
      Arrays.stream(cluster.regionsPerServer).map(regionsForServer -> computeCellGroupSizes(cluster, regionsForServer)).collect(
        Collectors.toList());

    return generateAction(cluster, cellCounts, cellGroupSizesPerServer);
  }

  private BalanceAction generateAction(
    BalancerClusterState cluster,
    int[] cellCounts,
    List<Map<Short, Integer>> cellGroupSizesPerServer
  ) {
    int targetRegionsPerServer = Ints.checkedCast(
      (long) Math.floor((double) cluster.numRegions / cluster.numServers));

    BalanceAction moveRegionToUnderloadedServer = tryMoveRegionToSomeUnderloadedServer(
      cluster,
      cellCounts,
      cellGroupSizesPerServer,
      targetRegionsPerServer
    );

    if (moveRegionToUnderloadedServer != BalanceAction.NULL_ACTION) {
      return moveRegionToUnderloadedServer;
    }

    BalanceAction moveRegionFromOverloadedServer = tryMoveRegionFromSomeOverloadedServer(
      cluster,
      cellCounts,
      cellGroupSizesPerServer,
      targetRegionsPerServer
    );

    if (moveRegionFromOverloadedServer != BalanceAction.NULL_ACTION) {
      return moveRegionFromOverloadedServer;
    }

    int numTimesCellRegionsFillAllServers = 0;
    for (int cell = 0; cell < HubSpotCellUtilities.MAX_CELL_COUNT; cell++) {
      int numRegionsForCell = cellCounts[cell];
      numTimesCellRegionsFillAllServers += Ints.checkedCast((long) Math.floor((double) numRegionsForCell / cluster.numServers));
    }

    int targetCellsPerServer = targetRegionsPerServer - numTimesCellRegionsFillAllServers;
    targetCellsPerServer = Math.min(targetCellsPerServer, HubSpotCellUtilities.getMaxCellsPerRs(cluster.numServers));
    Set<Integer> serversBelowTarget = new HashSet<>();
    Set<Integer> serversAboveTarget = new HashSet<>();

    for (int server = 0; server < cluster.numServers; server++) {
      int numCellsOnServer = cellGroupSizesPerServer.get(server).keySet().size();
      if (numCellsOnServer < targetCellsPerServer) {
        serversBelowTarget.add(server);
      } else if (numCellsOnServer > targetCellsPerServer) {
        serversAboveTarget.add(server);
      }
    }

    if (serversBelowTarget.isEmpty() && serversAboveTarget.isEmpty()) {
      return BalanceAction.NULL_ACTION;
    } else if (!serversAboveTarget.isEmpty()) {
      return swapRegionsToDecreaseDistinctCellsPerServer(cluster, cellCounts, cellGroupSizesPerServer, targetCellsPerServer);
    } else {
      return swapRegionsToIncreaseDistinctCellsPerServer(cluster, cellCounts, cellGroupSizesPerServer, targetCellsPerServer);
    }
  }
}
