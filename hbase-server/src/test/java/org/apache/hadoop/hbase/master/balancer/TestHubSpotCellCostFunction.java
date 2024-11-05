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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.Function;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestHubSpotCellCostFunction {

  private static final Function<Integer, Integer> ALL_REGIONS_SIZE_1_MB = x -> 1;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHubSpotCellCostFunction.class);

  @Test
  public void testCellCountTypical() {
    int numCells =
      HubSpotCellCostFunction.calcNumCells(
        new RegionInfo[] { buildRegionInfo((short) 0, (short) 1),
          buildRegionInfo((short) 1, (short) 2), buildRegionInfo((short) 2, (short) 3) },
        (short) 3);
    assertEquals(3, numCells);
  }

  @Test
  public void testCellCountMultipleInRegion() {
    int numCells = HubSpotCellCostFunction.calcNumCells(new RegionInfo[] {
      buildRegionInfo((short) 0, (short) 1), buildRegionInfo((short) 1, (short) 2),
      buildRegionInfo((short) 2, (short) 4), buildRegionInfo((short) 4, (short) 5) }, (short) 5);
    assertEquals(5, numCells);
  }

  @Test
  public void testCellCountMultipleInLastRegion() {
    int numCells = HubSpotCellCostFunction.calcNumCells(new RegionInfo[] {
      buildRegionInfo((short) 0, (short) 1), buildRegionInfo((short) 1, (short) 2),
      buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, (short) 5) }, (short) 5);
    assertEquals(5, numCells);
  }

  @Test
  public void testCellCountMultipleInFirstRegion() {
    int numCells = HubSpotCellCostFunction.calcNumCells(new RegionInfo[] {
      buildRegionInfo((short) 0, (short) 2), buildRegionInfo((short) 2, (short) 3),
      buildRegionInfo((short) 3, (short) 4), buildRegionInfo((short) 4, (short) 5) }, (short) 5);
    assertEquals(5, numCells);
  }

  @Test
  public void testCellCountLastKeyNull() {
    int numCells = HubSpotCellCostFunction.calcNumCells(new RegionInfo[] {
      buildRegionInfo((short) 0, (short) 1), buildRegionInfo((short) 1, (short) 2),
      buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, null) }, (short) 4);
    assertEquals(4, numCells);
  }

  @Test
  public void testCellCountFirstKeyNull() {
    int numCells =
      HubSpotCellCostFunction.calcNumCells(
        new RegionInfo[] { buildRegionInfo(null, (short) 1), buildRegionInfo((short) 1, (short) 2),
          buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, (short) 4) },
        (short) 4);
    assertEquals(4, numCells);
  }

  @Test
  public void testCellCountBothEndsNull() {
    int numCells = HubSpotCellCostFunction.calcNumCells(
      new RegionInfo[] { buildRegionInfo(null, (short) 1), buildRegionInfo((short) 1, (short) 2),
        buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, null) },
      (short) 4);
    assertEquals(4, numCells);
  }

  @Test
  public void testCostBalanced() {
    // 4 cells, 4 servers, perfectly balanced
    int cost = HubSpotCellCostFunction.calculateCurrentCellCost((short) 4, 4,
      new RegionInfo[] { buildRegionInfo(null, (short) 1), buildRegionInfo((short) 1, (short) 2),
        buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, null) },
      new int[][] { { 0 }, { 1 }, { 2 }, { 3 } }, ALL_REGIONS_SIZE_1_MB);

    assertEquals(0, cost);
  }

  @Test
  public void testCostImbalanced() {
    // 4 cells, 4 servers, perfectly balanced
    int cost = HubSpotCellCostFunction.calculateCurrentCellCost((short) 4, 4,
      new RegionInfo[] { buildRegionInfo(null, (short) 1), buildRegionInfo((short) 1, (short) 2),
        buildRegionInfo((short) 2, (short) 3), buildRegionInfo((short) 3, null) },
      new int[][] { { 0 }, { 0 }, { 0 }, { 0 } }, ALL_REGIONS_SIZE_1_MB);
    assertTrue(cost > 0);
  }

  private RegionInfo buildRegionInfo(Short startCell, Short stopCell) {
    RegionInfo result = RegionInfoBuilder.newBuilder(TableName.valueOf("table"))
      .setStartKey(startCell == null ? null : Bytes.toBytes(startCell))
      .setEndKey(stopCell == null ? null : Bytes.toBytes(stopCell)).build();
    return result;
  }
}
