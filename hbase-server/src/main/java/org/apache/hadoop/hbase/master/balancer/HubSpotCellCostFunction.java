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

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HubSpot addition: Cost function for balancing regions based on their cell prefix. This should not
 * be upstreamed, and our upstream solution should instead focus on introduction of balancer
 * conditionals; see <a href="https://issues.apache.org/jira/browse/HBASE-28513">HBASE-28513</a>
 */
public class HubSpotCellCostFunction extends CostFunction {

  private static final String HUBSPOT_CELL_COST_MULTIPLIER =
    "hbase.master.balancer.stochastic.hubspotCellCost";
  private static final float DEFAULT_HUBSPOT_CELL_COST = 0;

  private int numCells;
  private int numServers;
  private RegionInfo[] regions; // not necessarily sorted
  private int[][] regionLocations;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions);
    regions = cluster.regions;
    regionLocations = cluster.regionLocations;
    super.prepare(cluster);
  }

  @Override
  protected double cost() {
    return calculateCurrentCellCost(numCells, numServers, regions, regionLocations);
  }

  static int calculateCurrentCellCost(int numCells, int numServers, RegionInfo[] regions,
    int[][] regionLocations) {
    int bestCaseMaxCellsPerServer = (int) Math.ceil((double) numCells / numServers);

    int[] cellsPerServer = new int[numServers];
    Set<Integer> cellsAccountedFor = new HashSet<>(numCells);

    for (int i = 0; i < regions.length; i++) {
      int serverIndex = regionLocations[i][0];
      RegionInfo region = regions[i];
      Integer startCell = toCell(region.getStartKey());
      Integer stopCell = toCell(region.getEndKey());
      if (startCell == null) {
        // first region. for lack of info, assume one cell
        if (!cellsAccountedFor.contains(stopCell)) {
          cellsAccountedFor.add(stopCell);
          cellsPerServer[serverIndex] += 1;
        }
      } else if (stopCell == null) {
        // last region. for lack of info, assume one cell
        if (!cellsAccountedFor.contains(startCell)) {
          cellsAccountedFor.add(startCell);
          cellsPerServer[serverIndex] += 1;
        }
      } else {
        // middle regions
        for (int cell = startCell; cell <= stopCell; cell++) {
          if (!cellsAccountedFor.contains(cell)) {
            cellsAccountedFor.add(cell);
            cellsPerServer[serverIndex] += 1;
          }
        }
      }
    }

    int currentMaxCellsPerServer = bestCaseMaxCellsPerServer;
    for (int cells : cellsPerServer) {
      currentMaxCellsPerServer = Math.max(currentMaxCellsPerServer, cells);
    }

    return Math.max(0, currentMaxCellsPerServer - bestCaseMaxCellsPerServer);
  }

  /**
   * This method takes the smallest and greatest start/stop keys of all regions. From this, we can
   * determine the number of two-byte cell prefixes that can exist between the start and stop keys.
   * This won't work exactly correctly for the edge-case where the final region contains multiple
   * cells, but it's a good enough approximation.
   */
  static int calcNumCells(RegionInfo[] regionInfos) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    int leastCell = Integer.MAX_VALUE;
    int greatestCell = Integer.MIN_VALUE;

    for (RegionInfo regionInfo : regionInfos) {
      Integer startCell = toCell(regionInfo.getStartKey());
      Integer stopCell = toCell(regionInfo.getEndKey());

      if (startCell != null) {
        if (startCell < leastCell) {
          leastCell = startCell;
        }
        if (startCell > greatestCell) {
          greatestCell = startCell;
        }
      }

      if (stopCell != null) {
        if (stopCell < leastCell) {
          leastCell = stopCell;
        }
        if (stopCell > greatestCell) {
          greatestCell = stopCell;
        }
      }
    }

    return greatestCell - leastCell + 1;
  }

  private static Integer toCell(byte[] key) {
    if (key == null || key.length < 2) {
      return null;
    }
    return Bytes.readAsInt(key, 0, 2);
  }

}
