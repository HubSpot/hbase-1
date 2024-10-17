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
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.primitives.Shorts;

/**
 * HubSpot addition: Cost function for balancing regions based on their (reversed) cell prefix.
 * This should not be upstreamed, and our upstream solution should instead focus on introduction of balancer
 * conditionals; see <a href="https://issues.apache.org/jira/browse/HBASE-28513">HBASE-28513</a>
 */
@InterfaceAudience.Private public class HubSpotCellCostFunction extends CostFunction {

  private static final String HUBSPOT_CELL_COST_MULTIPLIER =
    "hbase.master.balancer.stochastic.hubspotCellCost";
  private static final float DEFAULT_HUBSPOT_CELL_COST = 0;
  // hack - hard code this for now
  private static final short MAX_CELL_COUNT = 360;

  private int numServers;
  private short numCells;
  private RegionInfo[] regions; // not necessarily sorted
  private int[][] regionLocations;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions, MAX_CELL_COUNT);
    regions = cluster.regions;
    regionLocations = cluster.regionLocations;
    super.prepare(cluster);
  }

  @Override protected double cost() {
    return calculateCurrentCellCost(numCells, numServers, regions, regionLocations);
  }

  static int calculateCurrentCellCost(short numCells, int numServers, RegionInfo[] regions,
    int[][] regionLocations) {
    int bestCaseMaxCellsPerServer = (int) Math.min(1, Math.ceil((double) numCells / numServers));

    int[] cellsPerServer = new int[numServers];
    for (int i = 0; i < regions.length; i++) {
      int serverIndex = regionLocations[i][0];
      RegionInfo region = regions[i];
      Set<Short> regionCells = toCells(region.getStartKey(), region.getEndKey(), numCells);
      cellsPerServer[serverIndex] += regionCells.size();
    }

    int currentMaxCellsPerServer =
      Arrays.stream(cellsPerServer).max().orElseGet(() -> bestCaseMaxCellsPerServer);

    return Math.max(0, currentMaxCellsPerServer - bestCaseMaxCellsPerServer);
  }

  static short calcNumCells(RegionInfo[] regionInfos, short totalCellCount) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    Set<Short> cellsInRegions =
      Arrays.stream(regionInfos).map(region -> toCells(region.getStartKey(), region.getEndKey(), totalCellCount))
        .flatMap(Set::stream).collect(Collectors.toSet());
    return Shorts.checkedCast(cellsInRegions.size());
  }

  private static Set<Short> toCells(byte[] start, byte[] stop, short numCells) {
    if (start == null && stop == null) {
      return Collections.emptySet();
    }

    if (stop == null || stop.length == 0) {
      Set<Short> result = IntStream.range(toCell(start), numCells).mapToObj(x -> (short) x)
        .collect(Collectors.toSet());
      return result;
    }

    if (start == null || start.length == 0) {
      return IntStream.range(0, toCell(stop)).mapToObj(x -> (short) x)
        .collect(Collectors.toSet());
    }

    return range(start, stop);
  }

  private static Set<Short> range(byte[] start, byte[] stop) {
    return IntStream.range(toCell(start), toCell(stop)).mapToObj(val -> (short) val)
      .collect(Collectors.toSet());
  }

  private static Short toCell(byte[] key) {
    if (key == null || key.length < 2) {
      return null;
    }

    return Bytes.toShort(key, 0, 2);
  }
}
