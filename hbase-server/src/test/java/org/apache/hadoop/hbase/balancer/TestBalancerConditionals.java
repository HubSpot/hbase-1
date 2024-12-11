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
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.balancer.BalancerConditionals;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestBalancerConditionals {

  private static final Logger LOG = LoggerFactory.getLogger(TestBalancerConditionals.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(3); // Start a cluster with 3 RegionServers
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBalancerConditionals() throws Exception {
    Connection connection = TEST_UTIL.getConnection();
    Admin admin = connection.getAdmin();

    // Create "product" table with 3 regions
    TableName productTableName = TableName.valueOf("product");
    TableDescriptor productTableDescriptor = TableDescriptorBuilder.newBuilder(productTableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build())
      .build();
    byte[][] splitKeys = { Bytes.toBytes("region1"), Bytes.toBytes("region2") };
    admin.createTable(productTableDescriptor, splitKeys);

    Set<TableName> tablesToBeSeparated = ImmutableSet.<TableName> builder()
      .add(TableName.META_TABLE_NAME).add(QuotaUtil.QUOTA_TABLE_NAME).add(productTableName).build();

    // Pause the balancer
    admin.balancerSwitch(false, true);

    // Move all regions (product, meta, and quotas) to one RegionServer
    List<RegionInfo> allRegions = tablesToBeSeparated.stream().map(t -> {
      try {
        return admin.getRegions(t);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).flatMap(Collection::stream).toList();
    String targetServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName().getServerName();

    for (RegionInfo region : allRegions) {
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
    }

    validateRegionLocationsWithRetry(connection, tablesToBeSeparated, productTableName, false);

    // Unpause the balancer and run it
    admin.balancerSwitch(true, true);
    admin.balance();

    validateRegionLocationsWithRetry(connection, tablesToBeSeparated, productTableName, true);
  }

  private boolean validateRegionLocationsWithRetry(Connection connection, Set<TableName> tableNames,
    TableName productTableName, boolean areDistributed) throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      try {
        validateRegionLocations(connection, tableNames, productTableName, areDistributed);
        return true;
      } catch (Exception e) {
        LOG.warn("Failed to validate region locations. Might retry", e);
        Thread.sleep(1000);
      }
    }
    return false;
  }

  private void validateRegionLocations(Connection connection, Set<TableName> tableNames,
    TableName productTableName, boolean areDistributed) {
    Map<TableName, Set<ServerName>> tableToServers =
      tableNames.stream().collect(Collectors.toMap(t -> t, t -> {
        try {
          return connection.getRegionLocator(t).getAllRegionLocations().stream()
            .map(HRegionLocation::getServerName).collect(Collectors.toSet());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

    // Validate that the region assignments
    ServerName metaServer =
      tableToServers.get(TableName.META_TABLE_NAME).stream().findFirst().orElseThrow();
    ServerName quotaServer =
      tableToServers.get(QuotaUtil.QUOTA_TABLE_NAME).stream().findFirst().orElseThrow();
    Set<ServerName> productServers = tableToServers.get(productTableName);

    if (areDistributed) {
      for (ServerName server : productServers) {
        assertNotEquals("Meta table and product table should not share servers", server,
          metaServer);
        assertNotEquals("Quota table and product table should not share servers", server,
          quotaServer);
      }
      assertNotEquals("The meta server and quotas server should be different", metaServer,
        quotaServer);
    } else {
      for (ServerName server : productServers) {
        assertEquals("Meta table and product table must share servers", server, metaServer);
        assertEquals("Quota table and product table must share servers", server, quotaServer);
      }
      assertEquals("The meta server and quotas server must be the same", metaServer, quotaServer);
    }
  }
}
