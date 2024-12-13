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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If enabled, this class will help the balancer ensure that replicas aren't placed on the same
 * servers or racks as their primary. Configure this via
 * {@link BalancerConditionals#DISTRIBUTE_REPLICAS_CONDITIONALS_KEY}
 */
public class DistributeReplicasConditional extends RegionPlanConditional {

  /**
   * Local mini cluster tests can only one on one server/rack by design. If enabled, this will
   * pretend that localhost RegionServer threads are actually running on separate hosts/racks. This
   * should only be used in unit tests.
   */
  public static boolean TEST_MODE_ENABLED = false;

  private static final Logger LOG = LoggerFactory.getLogger(DistributeReplicasConditional.class);

  private final BalancerClusterState cluster;

  public DistributeReplicasConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    this.cluster = cluster;
  }

  @Override
  boolean isViolating(RegionPlan regionPlan) {
    if (!cluster.hasRegionReplicas) {
      return false;
    }

    Integer destinationServerIndex =
      cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
    if (destinationServerIndex == null) {
      LOG.warn("Could not find server index for {}", regionPlan.getDestination().getHostname());
      return false;
    }

    int regionIndex = cluster.regionsToIndex.get(regionPlan.getRegionInfo());
    if (regionIndex == -1) {
      LOG.warn("Region {} not found in the cluster state", regionPlan.getRegionInfo());
      return false;
    }

    int primaryRegionIndex = cluster.regionIndexToPrimaryIndex[regionIndex];
    if (primaryRegionIndex == -1) {
      LOG.warn("No primary index found for region {}", regionPlan.getRegionInfo());
      return false;
    }

    if (
      checkViolation(cluster.regions, regionPlan.getRegionInfo(), destinationServerIndex,
        cluster.serversPerHost, cluster.serverIndexToHostIndex, cluster.regionsPerServer,
        primaryRegionIndex, "host")
    ) {
      return true;
    }

    if (
      checkViolation(cluster.regions, regionPlan.getRegionInfo(), destinationServerIndex,
        cluster.serversPerRack, cluster.serverIndexToRackIndex, cluster.regionsPerServer,
        primaryRegionIndex, "rack")
    ) {
      return true;
    }

    return false;
  }

  /**
   * Checks if placing a region replica on a location (host/rack) violates distribution rules.
   * @param destinationServerIndex Index of the destination server.
   * @param serversPerLocation     Array mapping locations (hosts/racks) to servers.
   * @param serverToLocationIndex  Array mapping servers to their location index.
   * @param regionsPerServer       Array mapping servers to their assigned regions.
   * @param primaryRegionIndex     Index of the primary region.
   * @param locationType           Type of location being checked ("Host" or "Rack").
   * @return True if a violation is found, false otherwise.
   */
  static boolean checkViolation(RegionInfo[] regions, RegionInfo regionToBeMoved,
    int destinationServerIndex, int[][] serversPerLocation, int[] serverToLocationIndex,
    int[][] regionsPerServer, int primaryRegionIndex, String locationType) {

    if (TEST_MODE_ENABLED) {
      // Take the flat serversPerLocation, like {0: [0, 1, 2, 3, 4]}
      // and pretend it is multi-location, like {0: [1], 1: [2] ...}
      int numServers = serversPerLocation[0].length;
      // Create a new serversPerLocation array where each server gets its own "location"
      int[][] simulatedServersPerLocation = new int[numServers][];
      for (int i = 0; i < numServers; i++) {
        simulatedServersPerLocation[i] = new int[] { serversPerLocation[0][i] };
      }
      // Adjust serverToLocationIndex to map each server to its simulated location
      int[] simulatedServerToLocationIndex = new int[numServers];
      for (int i = 0; i < numServers; i++) {
        simulatedServerToLocationIndex[serversPerLocation[0][i]] = i;
      }
      LOG.trace("Test mode enabled: Simulated {} locations for servers.", numServers);
      // Use the simulated arrays for test mode
      serversPerLocation = simulatedServersPerLocation;
      serverToLocationIndex = simulatedServerToLocationIndex;
    }

    if (serversPerLocation == null) {
      LOG.trace("{} violation check skipped: serversPerLocation is null", locationType);
      return false;
    }

    if (serversPerLocation.length == 1) {
      LOG.warn(
        "{} violation inevitable: serversPerLocation has only 1 entry. You probably should not be using read replicas.",
        locationType);
      return true;
    }

    int destinationLocationIndex = serverToLocationIndex[destinationServerIndex];
    LOG.trace("Checking {} violations for destination server index {} at location index {}",
      locationType, destinationServerIndex, destinationLocationIndex);

    // For every RegionServer on host/rack
    for (int serverIndex : serversPerLocation[destinationLocationIndex]) {
      // For every Region on RegionServer
      for (int hostedRegion : regionsPerServer[serverIndex]) {
        RegionInfo targetRegion = regions[hostedRegion];
        if (targetRegion.getEncodedName().equals(regionToBeMoved.getEncodedName())) {
          // The balancer state will already show this region as having moved.
          // A region's replicas will also have unique encoded names.
          // So we should skip this check if the encoded name is the same.
          continue;
        }
        boolean isReplicaForSameRegion =
          RegionReplicaUtil.isReplicasForSameRegion(targetRegion, regionToBeMoved);
        if (isReplicaForSameRegion) {
          LOG.trace("{} violation detected: region {} on {} {}", locationType, primaryRegionIndex,
            locationType, destinationLocationIndex);
          return true;
        }
      }
    }
    return false;
  }

}
