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
package org.apache.hadoop.hbase.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.balancer.BalancerConditionals;
import org.apache.hadoop.hbase.master.balancer.DistributeReplicasConditional;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestReplicaDistributionBalancerConditional {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReplicaDistributionBalancerConditional.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int REPLICAS = 3;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration()
      .setBoolean(BalancerConditionals.DISTRIBUTE_REPLICAS_CONDITIONALS_KEY, true);
    TEST_UTIL.getConfiguration()
      .setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_BALANCER_PERIOD, 1000L);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);

    // turn off replica cost functions
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaRackCostKey", 0);
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaHostCostKey", 0);

    DistributeReplicasConditional.TEST_MODE_ENABLED = true;

    TEST_UTIL.startMiniCluster(REPLICAS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReplicaDistribution() throws Exception {
    Connection connection = TEST_UTIL.getConnection();
    Admin admin = connection.getAdmin();

    // Create a "replicated_table" with region replicas
    TableName replicatedTableName = TableName.valueOf("replicated_table");
    TableDescriptor replicatedTableDescriptor =
      TableDescriptorBuilder.newBuilder(replicatedTableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build())
        .setRegionReplication(REPLICAS).build();
    admin.createTable(replicatedTableDescriptor, BalancerConditionalsTestUtil.generateSplits(3));

    // Pause the balancer
    admin.balancerSwitch(false, true);

    // Collect all region replicas and place them on one RegionServer
    List<RegionInfo> allRegions = admin.getRegions(replicatedTableName);
    String targetServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName().getServerName();

    for (RegionInfo region : allRegions) {
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
    }

    validateReplicaDistributionWithRetries(connection, replicatedTableName, false, false);

    // Unpause the balancer and trigger balancing
    admin.balancerSwitch(true, true);
    admin.balance();

    validateReplicaDistributionWithRetries(connection, replicatedTableName, true, true);
  }

  private static void validateReplicaDistributionWithRetries(Connection connection,
    TableName tableName, boolean shouldBeDistributed, boolean runBalancerOnFailure)
    throws InterruptedException, IOException {
    for (int i = 0; i < 100; i++) {
      try {
        validateReplicaDistribution(connection, tableName, shouldBeDistributed);
      } catch (AssertionError e) {
        if (i == 99) {
          throw e;
        }
        LOG.warn("Failed to validate region locations. Will retry", e);
        BalancerConditionalsTestUtil.printRegionLocations(TEST_UTIL.getConnection());
        if (runBalancerOnFailure) {
          connection.getAdmin().balance();
        }
        Thread.sleep(1000);
      }
    }
  }

  private static void validateReplicaDistribution(Connection connection, TableName tableName,
    boolean shouldBeDistributed) throws IOException {
    Map<ServerName, List<RegionInfo>> serverToRegions =
      connection.getRegionLocator(tableName).getAllRegionLocations().stream()
        .collect(Collectors.groupingBy(location -> location.getServerName(),
          Collectors.mapping(location -> location.getRegion(), Collectors.toList())));

    if (shouldBeDistributed) {
      // Ensure no server hosts more than one replica of any region
      for (Map.Entry<ServerName, List<RegionInfo>> serverAndRegions : serverToRegions.entrySet()) {
        List<RegionInfo> regionInfos = serverAndRegions.getValue();
        Set<byte[]> startKeys = new HashSet<>();
        for (RegionInfo regionInfo : regionInfos) {
          // each region should have a distinct start key
          assertFalse(
            "Each region should have its own start key, demonstrating it is not a replica of any others on this host",
            startKeys.contains(regionInfo.getStartKey()));
          startKeys.add(regionInfo.getStartKey());
        }
      }
    } else {
      // Ensure all replicas are on the same server
      assertEquals("All regions should share one server", 1, serverToRegions.size());
    }
  }
}
