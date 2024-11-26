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

import org.apache.hadoop.hbase.master.RegionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If enabled, this class will help the balancer ensure that replicas aren't placed on the same
 * servers or racks as their primary. Configure this via
 * {@link BalancerConditionals#DISTRIBUTE_REPLICAS_CONDITIONALS_KEY}
 */
public class DistributeReplicasConditional extends RegionPlanConditional {

  private static final Logger LOG = LoggerFactory.getLogger(DistributeReplicasConditional.class);

  private final BalancerClusterState cluster;

  public DistributeReplicasConditional(BalancerClusterState cluster) {
    super(cluster);
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
      checkViolation(destinationServerIndex, cluster.serversPerHost, cluster.serverIndexToHostIndex,
        cluster.regionsPerServer, primaryRegionIndex, cluster.regionIndexToPrimaryIndex, "host")
    ) {
      return true;
    }

    if (
      checkViolation(destinationServerIndex, cluster.serversPerRack, cluster.serverIndexToRackIndex,
        cluster.regionsPerServer, primaryRegionIndex, cluster.regionIndexToPrimaryIndex, "rack")
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
  static boolean checkViolation(int destinationServerIndex, int[][] serversPerLocation,
    int[] serverToLocationIndex, int[][] regionsPerServer, int primaryRegionIndex,
    int[] regionIndexToPrimaryIndex, String locationType) {
    if (serversPerLocation == null || serversPerLocation.length <= 1) {
      LOG.debug("{} violation check skipped: serversPerLocation is null or has <= 1 location",
        locationType);
      return false;
    }

    int destinationLocationIndex = serverToLocationIndex[destinationServerIndex];
    LOG.debug("Checking {} violations for destination server index {} at location index {}",
      locationType, destinationServerIndex, destinationLocationIndex);

    for (int serverIndex : serversPerLocation[destinationLocationIndex]) {
      for (int hostedRegion : regionsPerServer[serverIndex]) {
        if (regionIndexToPrimaryIndex[hostedRegion] == primaryRegionIndex) {
          LOG.debug("{} violation detected: region {} on {} {}", locationType, primaryRegionIndex,
            locationType, destinationLocationIndex);
          return true;
        }
      }
    }
    return false;
  }

}
