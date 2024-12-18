package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A candidate generator that tries to reduce prefix distribution across servers,
 * aiming to consolidate the same prefix onto fewer servers and improve prefix-based locality.
 */
public class PrefixColocationCandidateGenerator extends CandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixColocationCandidateGenerator.class);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  double getWeight(BalancerClusterState cluster) {
    // If a good move is found, then it's important to find perfect conditional compliance.
    // If a good move is not found, then skip this expensive candidate generator,
    // until region locations change.
    return (generateCandidate(cluster, true) != BalanceAction.NULL_ACTION) ? Double.MAX_VALUE : 0;
  }

  /**
   * Generates a candidate action to improve prefix colocation balance.
   * If isWeighing is true, the method should return quickly after finding a good candidate.
   */
  private BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (!BalancerConditionals.INSTANCE.isPrefixColocationEnabled()) {
      return BalanceAction.NULL_ACTION;
    }

    PrefixColocationConditional conditional = BalancerConditionals.INSTANCE.getPrefixColocationConditional();
    if (conditional == null) {
      return BalanceAction.NULL_ACTION;
    }

    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIdxToPrefixes = conditional.getServerIndexToPrefixes();

    // Check if there are any servers that are not "too many prefixes" - this ensures there's a potential improvement.
    if (!hasUnderutilizedServer(serverIdxToPrefixes, conditional)) {
      return BalanceAction.NULL_ACTION;
    }

    MoveCandidate candidate = findGoodMoveCandidate(cluster, serverIdxToPrefixes, conditional, isWeighing);

    // If no good move was found in the normal path, try a fallback "last mile" scenario.
    if (candidate == null) {
      candidate = findLastMileCandidate(cluster, serverIdxToPrefixes, conditional, isWeighing);
    }

    if (candidate == null) {
      // No moves found at all.
      return BalanceAction.NULL_ACTION;
    }

    return getAction(candidate.fromServer, candidate.regionToMove, candidate.toServer, -1);
  }

  /**
   * Checks if there exists at least one server that is not overloaded with prefixes,
   * ensuring that there's a potential improvement.
   */
  private boolean hasUnderutilizedServer(
    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIdxToPrefixes,
    PrefixColocationConditional conditional
  ) {
    return serverIdxToPrefixes.values().stream().anyMatch(prefixes ->
      !conditional.isTooManyPrefixesOnNode(prefixes.size())
    );
  }

  /**
   * Attempt to find a good move using the current heuristic:
   * 1. Identify a server with too many prefixes.
   * 2. From that server, find a prefix that is "least established" there (fewest regions).
   * 3. Attempt to move one of those regions to a server that already has the prefix and is better suited.
   */
  private MoveCandidate findGoodMoveCandidate(
    BalancerClusterState cluster,
    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIdxToPrefixes,
    PrefixColocationConditional conditional,
    boolean isWeighing
  ) {
    int largestPrefixCount = 0;
    MoveCandidate bestCandidate = null;

    // Iterate through servers that might be good candidates to move from.
    for (Map.Entry<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> entry : serverIdxToPrefixes.entrySet()) {
      int serverIdx = entry.getKey();
      Set<PrefixColocationConditional.PrefixByteArrayWrapper> prefixesOnServer = entry.getValue();
      int prefixCount = prefixesOnServer.size();

      // We only consider servers that have too many prefixes and have more prefixes than our current best candidate.
      if (conditional.isTooManyPrefixesOnNode(prefixCount) && prefixCount > largestPrefixCount) {
        MoveCandidate candidate = pickRegionFromOverloadedServer(cluster, conditional, serverIdxToPrefixes, serverIdx, isWeighing);
        if (candidate != null) {
          largestPrefixCount = prefixCount;
          bestCandidate = candidate;
          if (candidate.foundGoodMove || (isWeighing && candidate.regionToMove != -1)) {
            // If we found a great move or we are just weighing and found any move, we can stop searching.
            break;
          }
        }
      }
    }

    return bestCandidate;
  }

  /**
   * Given a server that has too many prefixes, pick a region to move based on the prefix that occurs least frequently on it.
   * Then choose a destination server that already has that prefix and is relatively underutilized.
   */
  private MoveCandidate pickRegionFromOverloadedServer(
    BalancerClusterState cluster,
    PrefixColocationConditional conditional,
    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIdxToPrefixes,
    int fromServer,
    boolean isWeighing
  ) {
    // Count how many times each prefix occurs on this server.
    Map<PrefixColocationConditional.PrefixByteArrayWrapper, Integer> prefixToCount = new HashMap<>();
    PrefixColocationConditional.PrefixByteArrayWrapper prefixToMove = null;
    int regionToMove = -1;
    int minCount = Integer.MAX_VALUE;

    // Identify the prefix to move: the one with the fewest regions, as it will be easiest to "unload".
    for (int regionIdx : cluster.regionsPerServer[fromServer]) {
      RegionInfo region = cluster.regions[regionIdx];
      if (region.getTable().isSystemTable()) {
        continue; // Don't consider system table regions
      }
      PrefixColocationConditional.PrefixByteArrayWrapper prefix = conditional.getPrefix(region);
      int count = prefixToCount.merge(prefix, 1, Integer::sum);
      if (count < minCount) {
        minCount = count;
        prefixToMove = prefix;
        regionToMove = regionIdx;
      }
    }

    if (prefixToMove == null) {
      return null;
    }

    // Try to find a better destination server for this prefix.
    boolean foundGoodMove = false;
    int toServer = pickOtherRandomServer(cluster, fromServer);
    int fewestPrefixesOnDestination = -1;

    Set<Integer> destinations = conditional.getPrefixToServerIndexes().get(prefixToMove);
    // If there are no known servers that have this prefix, we can't consolidate. Just return null.
    if (destinations == null || destinations.isEmpty()) {
      return null;
    }

    for (int candidateServer : destinations) {
      if (candidateServer == fromServer) {
        continue; // No point in moving within the same server
      }

      int destinationPrefixCount = serverIdxToPrefixes.get(candidateServer).size();
      // Select the destination that results in the fewest total prefixes (better consolidation).
      if (fewestPrefixesOnDestination == -1 || destinationPrefixCount < fewestPrefixesOnDestination) {
        fewestPrefixesOnDestination = destinationPrefixCount;
        toServer = candidateServer;

        // If the destination server is not overloaded with prefixes, consider this a good move.
        if (!conditional.isTooManyPrefixesOnNode(destinationPrefixCount)) {
          foundGoodMove = true;
        }

        if (isWeighing) {
          // For weighing, we can return on the first found candidate.
          break;
        }
      }
    }

    return new MoveCandidate(fromServer, toServer, regionToMove, foundGoodMove);
  }

  /**
   * When the cluster is nearly balanced but still split prefixes remain between two or few servers,
   * try a fallback (last mile) strategy:
   * - Relax some conditions:
   *   If the selected move doesn't strictly reduce the overloaded condition, still allow it if it consolidates prefixes.
   *   This might push the cluster from "almost balanced" to fully balanced after a few iterations.
   */
  private MoveCandidate findLastMileCandidate(
    BalancerClusterState cluster,
    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIdxToPrefixes,
    PrefixColocationConditional conditional,
    boolean isWeighing
  ) {
    // Last mile approach:
    // Instead of insisting on "tooManyPrefixes" > "maxProportionPerNode" conditions,
    // just find any prefix that is split between servers and try to unify them,
    // even if it means moving to a server that is not significantly underutilized.

    // Example heuristic:
    // 1. Find any prefix distributed across exactly two servers.
    // 2. Move one region to the other server to unify it.

    for (Map.Entry<PrefixColocationConditional.PrefixByteArrayWrapper, Set<Integer>> prefixEntry : conditional.getPrefixToServerIndexes().entrySet()) {
      Set<Integer> serversWithPrefix = prefixEntry.getValue();
      if (serversWithPrefix.size() == 2) {
        // Only two servers have this prefix - try to unify
        Iterator<Integer> it = serversWithPrefix.iterator();
        int serverA = it.next();
        int serverB = it.next();

        // Pick the server with slightly fewer prefixes as the destination
        int serverAPrefixCount = serverIdxToPrefixes.get(serverA).size();
        int serverBPrefixCount = serverIdxToPrefixes.get(serverB).size();
        int fromServer, toServer;

        if (serverAPrefixCount > serverBPrefixCount) {
          fromServer = serverA;
          toServer = serverB;
        } else {
          fromServer = serverB;
          toServer = serverA;
        }

        // Try to pick a region from the "fromServer" that belongs to this prefix
        // Move the first match we find:
        for (int regionIdx : cluster.regionsPerServer[fromServer]) {
          RegionInfo region = cluster.regions[regionIdx];
          if (region.getTable().isSystemTable()) continue;
          PrefixColocationConditional.PrefixByteArrayWrapper prefix = conditional.getPrefix(region);
          if (prefix.equals(prefixEntry.getKey())) {
            // Even if not reducing overload strictly, try this move to unify prefix
            return new MoveCandidate(fromServer, toServer, regionIdx, true);
          }
        }
      }
    }

    return null; // No last mile scenario found.
  }

  /**
   * Simple container for a candidate move.
   */
  private static class MoveCandidate {
    final int fromServer;
    final int toServer;
    final int regionToMove;
    final boolean foundGoodMove;

    MoveCandidate(int fromServer, int toServer, int regionToMove, boolean foundGoodMove) {
      this.fromServer = fromServer;
      this.toServer = toServer;
      this.regionToMove = regionToMove;
      this.foundGoodMove = foundGoodMove;
    }
  }

}
