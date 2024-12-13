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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerRegionReplicaMidCluster extends StochasticBalancerTestBase2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStochasticLoadBalancerRegionReplicaMidCluster.class);

  @Test
  public void testRegionReplicasOnMidCluster() {
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 100000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 10_000);
    loadBalancer.onConfigurationChange(conf);
    int numNodes = 200;
    int numRegions = 40 * 200;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 30; // all regions are mostly balanced
    int numTables = 10;
    testWithClusterWithIteration(numNodes, numRegions, numRegionsPerServer, replication, numTables,
      true, true);
  }
}
