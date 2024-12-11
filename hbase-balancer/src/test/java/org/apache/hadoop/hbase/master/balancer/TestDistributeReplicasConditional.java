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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDistributeReplicasConditional {

  /**
   * Test that no violation is detected when there is only one location. In this setup: -
   * serversPerLocation: All servers belong to the same location. - serverToLocationIndex: Maps each
   * server to the same location (index 0). - regionsPerServer: Regions are distributed between
   * servers, but since there's only one location, no violation occurs.
   */
  @Test
  public void testNoViolationWithSingleLocation() {
    int[][] serversPerLocation = { { 0, 1 } }; // Both Server 0 and Server 1 are in the same
                                               // location.
    int[] serverToLocationIndex = { 0, 0 }; // Both servers are mapped to location index 0.
    int[][] regionsPerServer = { { 0 }, { 1 } }; // Region 0 is assigned to Server 0; Region 1 is
                                                 // assigned to Server 1.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 1; // The region is being moved to Server 1.
    int[] regionIndexToPrimaryIndex = { 0, 1 }; // Region 0 is its own primary; Region 1 is also
                                                // distinct.

    assertFalse("No violation when only one location exists",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }

  /**
   * Test that a violation is detected when a replica is placed on the same host as its primary. In
   * this setup: - serversPerLocation: Each host has one server. - serverToLocationIndex: Maps each
   * server to its host. - regionsPerServer: Region 0 and Region 1 are on separate servers but
   * belong to the same host.
   */
  @Test
  public void testReplicaPlacedInSameHostAsPrimary() {
    int[][] serversPerLocation = { { 0 }, { 1 } }; // Server 0 is in Host 0; Server 1 is in Host 1.
    int[] serverToLocationIndex = { 0, 1 }; // Server 0 maps to Host 0; Server 1 maps to Host 1.
    int[][] regionsPerServer = { { 0 }, { 1 } }; // Region 0 is assigned to Server 0; Region 1 is
                                                 // assigned to Server 1.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 1; // The region is being moved to Server 1.
    int[] regionIndexToPrimaryIndex = { 0, 0 }; // Both Region 0 and Region 1 share the same primary
                                                // region.

    assertTrue("Host violation detected when replica is placed on the same host as its primary",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }

  /**
   * Test that a violation is detected when a primary is placed on the same host as its replica. In
   * this setup: - serversPerLocation: Host 0 contains Server 0 and Server 1; Host 1 contains Server
   * 2. - serverToLocationIndex: Maps each server to its corresponding host. - regionsPerServer:
   * Region 0 is hosted on Server 1 (replica), and Region 0 (primary) is being moved to Server 0.
   */
  @Test
  public void testPrimaryPlacedInSameHostAsReplica() {
    int[][] serversPerLocation = { { 0, 1 }, // Host 0 contains Server 0 and Server 1.
      { 2 } // Host 1 contains Server 2.
    };
    int[] serverToLocationIndex = { 0, // Server 0 is in Host 0.
      0, // Server 1 is in Host 0.
      1 // Server 2 is in Host 1.
    };
    int[][] regionsPerServer = { {}, // Server 0 has no regions initially.
      { 0 }, // Server 1 hosts Region 0 (replica).
      {} // Server 2 hosts no regions.
    };
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 0; // The primary region is being moved to Server 0.
    int[] regionIndexToPrimaryIndex = { 0 // Region 0 maps to its own primary.
    };

    assertTrue(
      "A violation should be detected when the primary is placed on the same host as its replica",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }

  /**
   * Test that a violation is detected when a replica is placed on the same rack as its primary. In
   * this setup: - serversPerLocation: One rack contains Server 0 and Server 1; another rack
   * contains Server 2. - serverToLocationIndex: Maps each server to its rack. - regionsPerServer:
   * Regions are distributed across servers on the same rack.
   */
  @Test
  public void testReplicaPlacedInSameRackAsPrimary() {
    int[][] serversPerLocation = { { 0, 1 }, { 2 } }; // Rack 0 contains Server 0 and 1; Rack 1
                                                      // contains Server 2.
    int[] serverToLocationIndex = { 0, 0, 1 }; // Servers 0 and 1 map to Rack 0; Server 2 maps to
                                               // Rack 1.
    int[][] regionsPerServer = { { 0 }, { 1 }, { 2 } }; // Region 0 is on Server 0; Region 1 is on
                                                        // Server 1; Region 2 is on Server 2.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 1; // The region is being moved to Server 1.
    int[] regionIndexToPrimaryIndex = { 0, 0, 1 }; // Regions 0 and 1 share the same primary region;
                                                   // Region 2 has a different primary.

    assertTrue("Rack violation detected when replica is placed on the same rack as its primary",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Rack"));
  }

  /**
   * Test that a violation is detected when a primary is placed in the same rack as its replica. In
   * this setup: - serversPerLocation: Rack 0 contains Server 0 and Server 1; Rack 1 contains Server
   * 2. - serverToLocationIndex: Maps each server to its corresponding rack. - regionsPerServer:
   * Region 0 is hosted on Server 1 (replica), and Region 0 (primary) is being moved to Server 0.
   */
  @Test
  public void testPrimaryPlacedInSameRackAsReplica() {
    int[][] serversPerLocation = { { 0, 1 }, // Rack 0 contains Server 0 and Server 1.
      { 2 } // Rack 1 contains Server 2.
    };
    int[] serverToLocationIndex = { 0, // Server 0 is in Rack 0.
      0, // Server 1 is in Rack 0.
      1 // Server 2 is in Rack 1.
    };
    int[][] regionsPerServer = { {}, // Server 0 has no regions initially.
      { 0 }, // Server 1 hosts Region 0 (replica).
      {} // Server 2 hosts no regions.
    };
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 0; // The primary region is being moved to Server 0.
    int[] regionIndexToPrimaryIndex = { 0 // Region 0 maps to its own primary.
    };

    assertTrue(
      "A violation should be detected when the primary is placed in the same rack as its replica",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Rack"));
  }

  /**
   * Test that no violation is detected when a replica is placed on a different host and rack. In
   * this setup: - serversPerLocation: Each host and rack contains one server. -
   * serverToLocationIndex: Maps each server to its unique host and rack. - regionsPerServer: Each
   * server has a distinct region.
   */
  @Test
  public void testNoViolationOnDifferentHostAndRack() {
    int[][] serversPerLocation = { { 0 }, { 1 } }; // Host/Rack 0 contains Server 0; Host/Rack 1
                                                   // contains Server 1.
    int[] serverToLocationIndex = { 0, 1 }; // Server 0 maps to Host/Rack 0; Server 1 maps to
                                            // Host/Rack 1.
    int[][] regionsPerServer = { { 0 }, { 1 } }; // Region 0 is assigned to Server 0; Region 1 is
                                                 // assigned to Server 1.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 1; // The region is being moved to Server 1.
    int[] regionIndexToPrimaryIndex = { 0, 1 }; // Region 0 and Region 1 have distinct primary
                                                // regions.

    assertFalse("No violation when replica is placed on a different host and rack",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }

  /**
   * Test that no violation is detected when serversPerLocation is empty.
   */
  @Test
  public void testEmptyServersPerLocation() {
    int[][] serversPerLocation = {}; // No servers in any location.
    int[] serverToLocationIndex = {}; // No server-to-location mapping.
    int[][] regionsPerServer = {}; // No regions assigned to servers.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 0; // The region is being moved to Server 0.
    int[] regionIndexToPrimaryIndex = {}; // No primary region mapping.

    assertFalse("No violation when serversPerLocation is empty",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }

  /**
   * Test that no violation is detected when serversPerLocation is null.
   */
  @Test
  public void testNoViolationWithNullServersPerLocation() {
    int[][] serversPerLocation = null; // Null serversPerLocation array.
    int[] serverToLocationIndex = {}; // No server-to-location mapping.
    int[][] regionsPerServer = {}; // No regions assigned to servers.
    int primaryRegionIndex = 0; // Primary region is Region 0.
    int destinationServerIndex = 0; // The region is being moved to Server 0.
    int[] regionIndexToPrimaryIndex = {}; // No primary region mapping.

    assertFalse("No violation when serversPerLocation is null",
      DistributeReplicasConditional.checkViolation(destinationServerIndex, serversPerLocation,
        serverToLocationIndex, regionsPerServer, primaryRegionIndex, regionIndexToPrimaryIndex,
        "Host"));
  }
}
