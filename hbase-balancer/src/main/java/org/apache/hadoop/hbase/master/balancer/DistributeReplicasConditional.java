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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures region replicas aren't placed on the same servers or racks as their primary.
 * <p>
 * This version handles multiple replicas of the same ReplicaKey (e.g., 100 copies).
 * Instead of simple presence/absence, it uses reference counts for each (server, ReplicaKey).
 * <p>
 * We also generate up to a fixed number of moves in {@link #computeNextMoves()} to distribute
 * replicas more evenly, and we optionally apply test-mode logic where we *locally* treat each
 * server as a unique host or rack without mutating the real {@link BalancerClusterState} arrays.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DistributeReplicasConditional extends RegionPlanConditional {

  private static final Logger LOG = LoggerFactory.getLogger(DistributeReplicasConditional.class);

  /**
   * Enable this to simulate multiple distinct hosts/racks when the real cluster might
   * report only 1 host or 1 rack. Useful for local unit tests.
   */
  public static final String TEST_MODE_ENABLED_KEY =
    "hbase.replica.distribution.conditional.testModeEnabled";

  /**
   * Maximum number of moves that {@link #computeNextMoves()} will generate at once.
   * This prevents the balancer from creating an enormous list of moves in large clusters.
   */
  private static final int MAX_MOVES = 1;

  private final Configuration conf;
  private BalancerClusterState cluster;

  /**
   * True if test-mode is enabled. If so, we create local arrays for host/rack
   * mapping and artificially split them if cluster.numHosts == 1 or cluster.numRacks == 1.
   */
  private final boolean isTestModeEnabled;

  /**
   * For each server s: (ReplicaKey -> reference count).
   * If reference count > 1, it means server s has multiple copies of the same replica.
   */
  private Map<ReplicaKey, Integer>[] serverReplicaKeyCounts;

  /**
   * For each "host" h: (ReplicaKey -> reference count).
   * We do *not* mutate the real cluster arrays if test mode is on; instead, we keep
   * these local arrays / mappings for host indices.
   */
  private Map<ReplicaKey, Integer>[] hostReplicaKeyCounts;

  /**
   * For each "rack" r: (ReplicaKey -> reference count).
   */
  private Map<ReplicaKey, Integer>[] rackReplicaKeyCounts;

  /**
   * Local copies of host/rack indexes per server. This is either the real data from cluster,
   * or a simulated test-mode expansion.
   */
  private int[] localServerToHostIndex;
  private int[] localServerToRackIndex;

  /** The local count of "hosts" or "racks", possibly expanded in test mode. */
  private int localNumHosts;
  private int localNumRacks;

  @SuppressWarnings("unchecked")
  public DistributeReplicasConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    this.conf = conf;
    this.cluster = cluster;
    this.isTestModeEnabled = conf.getBoolean(TEST_MODE_ENABLED_KEY, false);

    // Build local copies of server -> host/rack mapping,
    // possibly expanded if test mode is on.
    buildLocalHostRackMapping();

    // Now allocate & populate reference-count arrays
    allocateReplicaCountArrays();
    populateReplicaLocations();
  }

  @Override
  void refresh(Configuration conf, BalancerClusterState cluster) {
    this.cluster = cluster;

    // Rebuild local host/rack mapping (in case cluster changed, or test mode toggled)
    buildLocalHostRackMapping();

    // Rebuild the arrays
    allocateReplicaCountArrays();
    populateReplicaLocations();
  }

  /**
   * Creates local arrays for server -> host and server -> rack mapping so we don't
   * mutate {@code cluster.serverIndexToHostIndex} or {@code cluster.serverIndexToRackIndex}.
   * If test mode is on and there's only 1 host (or 1 rack), we artificially expand
   * localNumHosts (or localNumRacks) to match cluster.numServers.
   */
  private void buildLocalHostRackMapping() {
    // Start from actual cluster state
    this.localNumHosts = cluster.numHosts;
    this.localNumRacks = cluster.numRacks;

    // Make a local copy of server -> host & rack
    this.localServerToHostIndex = new int[cluster.numServers];
    this.localServerToRackIndex = new int[cluster.numServers];

    for (int s = 0; s < cluster.numServers; s++) {
      localServerToHostIndex[s] = cluster.serverIndexToHostIndex[s];
      localServerToRackIndex[s] = cluster.serverIndexToRackIndex[s];
    }

    // If test mode is enabled, expand the local arrays (without touching real cluster arrays)
    if (isTestModeEnabled) {
      if (localNumHosts == 1 && cluster.numServers > 1) {
        LOG.trace("Test mode: locally splitting single host into multiple hosts ({}).",
          cluster.numServers);
        for (int s = 0; s < cluster.numServers; s++) {
          localServerToHostIndex[s] = s;
        }
        localNumHosts = cluster.numServers; // local expansion only
      }

      if (localNumRacks == 1 && cluster.numServers > 1) {
        LOG.trace("Test mode: locally splitting single rack into multiple racks ({}).",
          cluster.numServers);
        for (int s = 0; s < cluster.numServers; s++) {
          localServerToRackIndex[s] = s;
        }
        localNumRacks = cluster.numServers; // local expansion only
      }
    }
  }

  private void allocateReplicaCountArrays() {
    // Allocate arrays using the *local* counts, not the real cluster counts.
    this.serverReplicaKeyCounts = new HashMap[cluster.numServers];
    this.hostReplicaKeyCounts = new HashMap[localNumHosts];
    this.rackReplicaKeyCounts = new HashMap[localNumRacks];

    for (int s = 0; s < cluster.numServers; s++) {
      serverReplicaKeyCounts[s] = new HashMap<>();
    }
    for (int h = 0; h < localNumHosts; h++) {
      hostReplicaKeyCounts[h] = new HashMap<>();
    }
    for (int r = 0; r < localNumRacks; r++) {
      rackReplicaKeyCounts[r] = new HashMap<>();
    }
  }

  /**
   * Populate reference counts from the cluster's current region assignments,
   * using our local server->host/rack mapping.
   */
  private void populateReplicaLocations() {
    for (int serverIdx = 0; serverIdx < cluster.numServers; serverIdx++) {
      int[] regionIndices = cluster.regionsPerServer[serverIdx];
      for (int regionIdx : regionIndices) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        ReplicaKey rKey = new ReplicaKey(regionInfo);
        addReplicaKey(serverIdx, rKey);
      }
    }
  }

  /**
   * Used to confirm that our local arrays are still aligned with cluster.numServers.
   * For example, if the cluster changed server count mid-run.
   */
  private boolean isClusterStateConsistent() {
    if (serverReplicaKeyCounts.length != cluster.numServers) {
      return false;
    }
    if (localServerToHostIndex.length != cluster.numServers ||
      localServerToRackIndex.length != cluster.numServers) {
      return false;
    }
    return true;
  }

  @Override
  boolean isViolating(RegionPlan regionPlan) {
    if (!cluster.hasRegionReplicas) {
      return false;
    }

    if (!isClusterStateConsistent()) {
      LOG.warn("Cluster state changed. Refreshing local arrays in isViolating(...)");
      refresh(conf, cluster);
    }

    Integer destServerIdx = cluster.serversToIndex.get(
      regionPlan.getDestination().getAddress()
    );
    if (destServerIdx == null) {
      LOG.warn("Destination server index not found for address: {}",
        regionPlan.getDestination().getHostname());
      return false;
    }

    // Build the replica key
    ReplicaKey keyToMove = new ReplicaKey(regionPlan.getRegionInfo());

    // Check local host
    int hostIdx = localServerToHostIndex[destServerIdx];
    if (localNumHosts == 1) {
      // If there's truly only 1 host in local mapping
      LOG.warn("Single host (locally) => collisions inevitable.");
      return true;
    }
    if (hostReplicaKeyCounts[hostIdx].getOrDefault(keyToMove, 0) > 0) {
      LOG.trace("Host-level violation: local host {} has key {}", hostIdx, keyToMove);
      return true;
    }

    // Check local rack
    int rackIdx = localServerToRackIndex[destServerIdx];
    if (localNumRacks == 1) {
      LOG.warn("Single rack (locally) => collisions inevitable.");
      return true;
    }
    if (rackReplicaKeyCounts[rackIdx].getOrDefault(keyToMove, 0) > 0) {
      LOG.trace("Rack-level violation: local rack {} has key {}", rackIdx, keyToMove);
      return true;
    }

    return false;
  }

  @Override
  List<RegionPlan> computeNextMoves() {
    if (!cluster.hasRegionReplicas) {
      return Collections.emptyList();
    }

    if (!isClusterStateConsistent()) {
      LOG.warn("Cluster state changed. Refreshing local arrays in computeNextMoves(...)");
      refresh(conf, cluster);
    }

    List<RegionPlan> plans = new ArrayList<>();
    int movesCount = 0;

    // For each server, check for duplicates
    for (int sourceServer = 0; sourceServer < cluster.numServers && movesCount < MAX_MOVES; sourceServer++) {
      Map<ReplicaKey, Integer> serverMap = serverReplicaKeyCounts[sourceServer];

      // Identify keys with count > 1 => duplicates
      for (Map.Entry<ReplicaKey, Integer> entry : serverMap.entrySet()) {
        ReplicaKey key = entry.getKey();
        int count = entry.getValue();

        // If count <= 1, no duplicates here
        if (count <= 1) {
          continue;
        }

        int duplicatesToMove = count - 1;
        while (duplicatesToMove > 0 && movesCount < MAX_MOVES) {
          List<RegionInfo> regionsToMove = findAllDuplicatesOfKey(sourceServer, key);
          for (RegionInfo regionToMove : regionsToMove) {
            if (regionToMove == null) {
              break;  // can't find a region for that key
            }

            RegionPlan regionPlan = findGoodPlanOrForce(sourceServer, regionToMove, () -> findServerWithoutKey(key));
            plans.add(regionPlan);

            movesCount++;
            duplicatesToMove--;

            if (movesCount >= MAX_MOVES) {
              break;
            }
          }
          if (movesCount >= MAX_MOVES) {
            break;
          }
        }
        if (movesCount >= MAX_MOVES) {
          break;
        }
      }
    }

    LOG.debug("DistributeReplicasConditional: returning {} moves", plans.size());
    return plans;
  }


  @Override
  void acceptMove(BalanceAction balanceAction) {
    if (balanceAction == BalanceAction.NULL_ACTION) {
      return;
    }
    List<RegionPlan> regionPlans = cluster.convertActionToPlans(balanceAction);
    for (RegionPlan regionPlan : regionPlans) {
      Integer srcIdx = cluster.serversToIndex.get(regionPlan.getSource().getAddress());
      Integer dstIdx = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
      if (srcIdx == null || dstIdx == null) {
        return;
      }

      ReplicaKey key = new ReplicaKey(regionPlan.getRegionInfo());
      removeReplicaKey(srcIdx, key);
      addReplicaKey(dstIdx, key);
    }
  }

  private void addReplicaKey(int serverIndex, ReplicaKey rKey) {
    incrementCount(serverReplicaKeyCounts[serverIndex], rKey);

    int hostIndex = localServerToHostIndex[serverIndex];
    incrementCount(hostReplicaKeyCounts[hostIndex], rKey);

    int rackIndex = localServerToRackIndex[serverIndex];
    incrementCount(rackReplicaKeyCounts[rackIndex], rKey);
  }

  private void removeReplicaKey(int serverIndex, ReplicaKey rKey) {
    decrementCount(serverReplicaKeyCounts[serverIndex], rKey);

    int hostIndex = localServerToHostIndex[serverIndex];
    decrementCount(hostReplicaKeyCounts[hostIndex], rKey);

    int rackIndex = localServerToRackIndex[serverIndex];
    decrementCount(rackReplicaKeyCounts[rackIndex], rKey);
  }

  private static void incrementCount(Map<ReplicaKey, Integer> map, ReplicaKey rKey) {
    map.put(rKey, map.getOrDefault(rKey, 0) + 1);
  }

  private static void decrementCount(Map<ReplicaKey, Integer> map, ReplicaKey rKey) {
    Integer current = map.get(rKey);
    if (current == null) {
      return; // not present
    }
    if (current <= 1) {
      map.remove(rKey);
    } else {
      map.put(rKey, current - 1);
    }
  }

  private List<RegionInfo> findAllDuplicatesOfKey(int serverIndex, ReplicaKey key) {
    List<RegionInfo> results = new ArrayList<>();
    int found = 0;
    for (int regionIdx : cluster.regionsPerServer[serverIndex]) {
      RegionInfo candidate = cluster.regions[regionIdx];
      if (new ReplicaKey(candidate).equals(key)) {
        found++;
        if (found > 1) {
          results.add(candidate);
        }
      }
    }
    return results;
  }

  private Integer findServerWithoutKey(ReplicaKey key) {
    for (int i = 0; i < cluster.numServers; i++) {
      // Introduce some randomness here, which is preferable to
      // predictable correctness in many ways
      int possibleServer = pickRandomServer();
      int count = serverReplicaKeyCounts[possibleServer].getOrDefault(key, 0);
      if (count == 0) {
        return possibleServer;
      }
    }
    return null;
  }

  // --------------------------------------------------------------------------
  // REPLICA KEY
  // --------------------------------------------------------------------------

  static class ReplicaKey {
    private final Pair<ByteArrayWrapper, ByteArrayWrapper> startStopPair;

    ReplicaKey(RegionInfo regionInfo) {
      this.startStopPair = new Pair<>(
        new ByteArrayWrapper(regionInfo.getStartKey()),
        new ByteArrayWrapper(regionInfo.getEndKey())
      );
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ReplicaKey)) return false;
      ReplicaKey other = (ReplicaKey) o;
      return this.startStopPair.equals(other.startStopPair);
    }

    @Override
    public int hashCode() {
      return startStopPair.hashCode();
    }

    @Override
    public String toString() {
      return "ReplicaKey(" + startStopPair.getFirst() + ", " + startStopPair.getSecond() + ")";
    }
  }

  static class ByteArrayWrapper {
    private final byte[] bytes;

    ByteArrayWrapper(byte[] arr) {
      this.bytes = (arr == null ? new byte[0] : arr.clone());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ByteArrayWrapper)) return false;
      ByteArrayWrapper other = (ByteArrayWrapper) o;
      if (this.bytes.length != other.bytes.length) return false;
      for (int i = 0; i < this.bytes.length; i++) {
        if (this.bytes[i] != other.bytes[i]) return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      for (byte b : this.bytes) {
        result = 31 * result + b;
      }
      return result;
    }

    @Override
    public String toString() {
      return bytes.length == 0 ? "[]" : ("[" + bytes.length + " bytes]");
    }
  }
}
