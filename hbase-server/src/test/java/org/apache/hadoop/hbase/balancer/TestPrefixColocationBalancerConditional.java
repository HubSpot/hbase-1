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

import static org.apache.hadoop.hbase.balancer.BalancerConditionalsTestUtil.validateAssertionsWithRetries;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import org.apache.hadoop.hbase.master.balancer.PrefixColocationConditional;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Category(LargeTests.class)
public class TestPrefixColocationBalancerConditional {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefixColocationBalancerConditional.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestPrefixColocationBalancerConditional.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int REPLICAS = 1; // todo rmattingly see how it plays with read replicas
  private static final int NUM_SERVERS = 3;
  private static final int PREFIX_LENGTH = 2; // length of short
  private static final int PREFIXES_PER_SERVER = 5;
  private static final int PREFIX_CARDINALITY = NUM_SERVERS * PREFIXES_PER_SERVER;
  private static final int REGIONS_PER_PREFIX = 5;
  private static final double
    MAX_PROPORTION_PER_NODE = (double) PREFIXES_PER_SERVER / PREFIX_CARDINALITY;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration()
      .setInt(BalancerConditionals.PREFIX_COLOCATION_LENGTH_KEY, PREFIX_LENGTH);
    TEST_UTIL.getConfiguration()
      .setDouble(BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE,
        MAX_PROPORTION_PER_NODE);
    TEST_UTIL.getConfiguration()
      .setBoolean(BalancerConditionals.DISTRIBUTE_REPLICAS_CONDITIONALS_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(DistributeReplicasConditional.TEST_MODE_ENABLED_KEY,
      true);
    TEST_UTIL.getConfiguration()
      .setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_BALANCER_PERIOD, 1000L);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);

    // turn off replica cost functions
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaRackCostKey", 0);
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaHostCostKey", 0);

    TEST_UTIL.startMiniCluster(NUM_SERVERS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPrefixColocation() throws Exception {
    Connection connection = TEST_UTIL.getConnection();
    Admin admin = connection.getAdmin();

    // Create a "replicated_table" with region replicas
    TableName replicatedTableName = TableName.valueOf("replicated_table");
    TableDescriptor replicatedTableDescriptor =
      TableDescriptorBuilder.newBuilder(replicatedTableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build())
        .setRegionReplication(REPLICAS).build();

    byte[][] splits = new byte[PREFIX_CARDINALITY * REGIONS_PER_PREFIX][];
    for (short i = 0; i < PREFIX_CARDINALITY * REGIONS_PER_PREFIX; i++) {
      short prefix = (short) (double) (i / REGIONS_PER_PREFIX);
      int suffix = i % REGIONS_PER_PREFIX;
      byte[] prefixBytes = Bytes.toBytes(prefix);
      byte[] suffixBytes = Bytes.toBytes(suffix);
      splits[i] = Bytes.add(prefixBytes, suffixBytes);
    }

    admin.createTable(replicatedTableDescriptor, splits);

    // Pause the balancer
    admin.balancerSwitch(false, true);

    // Collect all region replicas and place them on one RegionServer
    List<RegionInfo> allRegions = admin.getRegions(replicatedTableName);
    String targetServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName().getServerName();

    for (RegionInfo region : allRegions) {
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
    }

    BalancerConditionalsTestUtil.printRegionLocations(TEST_UTIL.getConnection(), this::decodeKey);
    validateAssertionsWithRetries(TEST_UTIL, false, () -> BalancerConditionalsTestUtil
      .validateReplicaDistribution(connection, replicatedTableName, false));
    validateAssertionsWithRetries(
      TEST_UTIL,
      false,
      ImmutableSet.of(() -> validatePrefixColocation(connection, replicatedTableName, false)),
      key -> Short.toString(Bytes.toShort(key))
    );

    // Unpause the balancer and trigger balancing
    admin.balancerSwitch(true, true);
    admin.balance();

    validateAssertionsWithRetries(TEST_UTIL, true, () -> BalancerConditionalsTestUtil
      .validateReplicaDistribution(connection, replicatedTableName, true));
    validateAssertionsWithRetries(
      TEST_UTIL,
      true,
      ImmutableSet.of(() -> validatePrefixColocation(connection, replicatedTableName, true)),
      this::decodeKey
    );
    BalancerConditionalsTestUtil.printRegionLocations(TEST_UTIL.getConnection(), this::decodeKey);
  }

  private String decodeKey(byte[] key) {
    if (key.length == 0) {
      return "null";
    }
    return Short.toString(Bytes.toShort(key));
  }

  private void validatePrefixColocation(Connection connection, TableName tableName, boolean shouldBeColocated) throws AssertionError {
    Map<ServerName, List<RegionInfo>> serverToRegions = null;
    try {
      serverToRegions = connection.getRegionLocator(tableName).getAllRegionLocations().stream()
        .collect(Collectors.groupingBy(location -> location.getServerName(),
          Collectors.mapping(location -> location.getRegion(), Collectors.toList())));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    double maxPrefixes = MAX_PROPORTION_PER_NODE * PREFIX_CARDINALITY;
    for (Map.Entry<ServerName, List<RegionInfo>> serverAndRegions : serverToRegions.entrySet()) {
      Set<byte[]> prefixes = new HashSet<>();
      for (RegionInfo region : serverAndRegions.getValue()) {
        byte[] prefix = PrefixColocationConditional.getPrefix(region, PREFIX_LENGTH);
        if (prefixes.stream().noneMatch(p -> Arrays.equals(p, prefix))) {
          prefixes.add(prefix);
        }
      }
      if (shouldBeColocated) {
        assertTrue(String.format("Expected server %s to have LTE %s prefixes, but had %s", serverAndRegions.getKey().getHostname(), maxPrefixes, prefixes.size()), prefixes.size() <= maxPrefixes);
      } else {
        assertTrue(String.format("Expected server %s to have GT %s prefixes, but had %s", serverAndRegions.getKey().getHostname(), maxPrefixes, prefixes.size()), prefixes.size() > maxPrefixes);
      }
    }
  }
}
