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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalancerConditionalsTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerConditionalsTestUtil.class);

  static byte[][] generateSplits(int numRegions) {
    byte[][] splitKeys = new byte[numRegions - 1][];
    for (int i = 0; i < numRegions - 1; i++) {
      splitKeys[i] =
        Bytes.toBytes(String.format("%09d", (i + 1) * (Integer.MAX_VALUE / numRegions)));
    }
    return splitKeys;
  }

  static void printRegionLocations(Connection connection) throws IOException {
    Admin admin = connection.getAdmin();

    // Get all table names in the cluster
    Set<TableName> tableNames = admin.listTableDescriptors(true).stream()
      .map(TableDescriptor::getTableName).collect(Collectors.toSet());

    // Group regions by server
    Map<ServerName, Map<TableName, List<RegionInfo>>> serverToRegions =
      admin.getClusterMetrics().getLiveServerMetrics().keySet().stream()
        .collect(Collectors.toMap(server -> server, server -> {
          try {
            return listRegionsByTable(connection, server, tableNames);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));

    // Pretty print region locations
    LOG.info("Pretty printing region locations...");
    serverToRegions.forEach((server, tableRegions) -> {
      LOG.info("Server: " + server.getServerName());
      tableRegions.forEach((table, regions) -> {
        if (regions.isEmpty()) {
          return;
        }
        LOG.info("  Table: " + table.getNameAsString());
        regions.forEach(region -> LOG.info("    Region: " + region.getRegionNameAsString()));
      });
    });
  }

  private static Map<TableName, List<RegionInfo>> listRegionsByTable(Connection connection,
    ServerName server, Set<TableName> tableNames) throws IOException {
    Admin admin = connection.getAdmin();

    // Find regions for each table
    return tableNames.stream().collect(Collectors.toMap(tableName -> tableName, tableName -> {
      List<RegionInfo> allRegions = null;
      try {
        allRegions = admin.getRegions(server);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return allRegions.stream().filter(region -> region.getTable().equals(tableName))
        .collect(Collectors.toList());
    }));
  }

}
