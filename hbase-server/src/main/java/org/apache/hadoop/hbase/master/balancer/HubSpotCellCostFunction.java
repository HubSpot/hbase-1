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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HubSpot addition: Cost function for balancing regions based on their (reversed) cell prefix.
 * This should not be upstreamed, and our upstream solution should instead focus on introduction of balancer
 * conditionals; see <a href="https://issues.apache.org/jira/browse/HBASE-28513">HBASE-28513</a>
 */
@InterfaceAudience.Private
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

  @Override void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions);
    regions = cluster.regions;
    regionLocations = cluster.regionLocations;
    super.prepare(cluster);
  }

  @Override protected double cost() {
    return calculateCurrentCellCost(numCells, numServers, regions, regionLocations);
  }

  static int calculateCurrentCellCost(int numCells, int numServers, RegionInfo[] regions,
    int[][] regionLocations) {
    int bestCaseMaxCellsPerServer = (int) Math.ceil((double) numCells / numServers);

    int[] cellsPerServer = new int[numServers];
    for (int i = 0; i < regions.length; i++) {
      int serverIndex = regionLocations[i][0];
      RegionInfo region = regions[i];
      Set<Short> regionCells = toCells(region.getStartKey(), region.getEndKey());
      cellsPerServer[serverIndex] += regionCells.size();
    }

    int currentMaxCellsPerServer = bestCaseMaxCellsPerServer;
    for (int cells : cellsPerServer) {
      currentMaxCellsPerServer = Math.max(currentMaxCellsPerServer, cells);
    }

    return Math.max(0, currentMaxCellsPerServer - bestCaseMaxCellsPerServer);
  }

  static int calcNumCells(RegionInfo[] regionInfos) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    return Ints.checkedCast(
      Arrays.stream(regionInfos).map(region -> toCells(region.getStartKey(), region.getEndKey()))
        .flatMap(Set::stream).distinct().count());
  }

  private static Set<Short> toCells(byte[] start, byte[] stop) {
    if (start == null && stop == null) {
      return Collections.emptySet();
    }

    if (start == null) {
      return Collections.singleton(toCell(stop));
    }

    if (stop == null) {
      return Collections.singleton(toCell(start));
    }

    return range(start, stop);
  }

  private static Set<Short> range(byte[] start, byte[] stop) {
    Set<Short> cells = new HashSet<>();

    for (byte[] current = start;
         Bytes.compareTo(current, stop) <= 0; current = Bytes.unsignedCopyAndIncrement(current)) {
      cells.add(toCell(current));
    }

    return cells;
  }

  private static Short toCell(byte[] key) {
    if (key == null || key.length < 2) {
      return null;
    }

    return ByteBuffer.wrap(key, 0, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
  }
}
