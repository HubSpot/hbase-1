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

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@InterfaceAudience.Private
class HubSpotCellBasedCandidateGenerator extends CandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(HubSpotCellBasedCandidateGenerator.class);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    int[][] regionsPerServer = cluster.regionsPerServer;

    int serverWithMostCells = -1;
    int mostCellsPerServerSoFar = 0;
    double mostCellsReservoirRandom = -1;

    int serverWithFewestCells = -1;
    int fewestCellsPerServerSoFar = 360;
    double fewestCellsReservoirRandom = -1;

    for (int serverIndex = 0; serverIndex < regionsPerServer.length; serverIndex++) {
      int cellsOnServer = numCells(cluster, regionsPerServer[serverIndex]);

      if (cellsOnServer > mostCellsPerServerSoFar) {
        mostCellsPerServerSoFar = cellsOnServer;
        mostCellsReservoirRandom = -1;
      } else if ( cellsOnServer == mostCellsPerServerSoFar) {
        // we don't know how many servers have the same cell count, so use a simplified online
        // reservoir sampling approach (http://gregable.com/2007/10/reservoir-sampling.html)
        double maxCellRandom = ThreadLocalRandom.current().nextDouble();
        if (maxCellRandom > mostCellsReservoirRandom) {
          serverWithMostCells = serverIndex;
          mostCellsReservoirRandom = maxCellRandom;
        }
      }

      if (cellsOnServer < fewestCellsPerServerSoFar) {
        fewestCellsPerServerSoFar = cellsOnServer;
        fewestCellsReservoirRandom = -1;
      } else if ( cellsOnServer == fewestCellsPerServerSoFar) {
        // we don't know how many servers have the same cell count, so use a simplified online
        // reservoir sampling approach (http://gregable.com/2007/10/reservoir-sampling.html)
        double minCellRandom = ThreadLocalRandom.current().nextDouble();
        if (minCellRandom > fewestCellsReservoirRandom) {
          serverWithFewestCells = serverIndex;
          fewestCellsReservoirRandom = minCellRandom;
        }
      }
    }

    BalanceAction action =
      maybeMoveRegionFromHeaviestToLightest(cluster, serverWithMostCells, serverWithFewestCells);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting {} ({} cells --> {} cells)",
        action.toString(),
        mostCellsPerServerSoFar,
        fewestCellsPerServerSoFar
      );
    }

    return action;
  }

  private int numCells(BalancerClusterState cluster, int[] regions) {
    Set<Short> cells = new HashSet<>(regions.length);

    for (int regionIndex : regions) {
      RegionInfo region = cluster.regions[regionIndex];
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

      for (short i = startCellId; i < endCellId; i++) {
        cells.add(i);
      }

      if (!HubSpotCellCostFunction.isStopExclusive(endKey)) {
        cells.add(endCellId);
      }
    }

    return cells.size();
  }

  BalanceAction maybeMoveRegionFromHeaviestToLightest(BalancerClusterState cluster, int fromServer, int toServer) {
    if (fromServer < 0 || toServer < 0) {
      return BalanceAction.NULL_ACTION;
    }

    return getAction(fromServer, pickRandomRegion(cluster, fromServer, 0.5), toServer, -1);
  }
}
