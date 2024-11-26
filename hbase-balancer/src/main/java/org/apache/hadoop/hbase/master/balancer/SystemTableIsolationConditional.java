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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;

/**
 * If enabled, this class will help the balancer ensure that system tables live on their own
 * RegionServer. System tables will share one RegionServer! This conditional can be used in tandem
 * with {@link MetaTableIsolationConditional} to add a second RegionServer specifically for meta
 * table hosting. Configure this via {@link BalancerConditionals#ISOLATE_SYSTEM_TABLES_KEY}
 */
class SystemTableIsolationConditional extends RegionPlanConditional {

  private final Set<ServerName> serversHostingSystemTables;

  public SystemTableIsolationConditional(BalancerClusterState cluster) {
    super(cluster);
    Set<ServerName> serversHostingSystemTables = new HashSet<>();
    for (int i = 0; i < cluster.regions.length; i++) {
      RegionInfo regionInfo = cluster.regions[i];
      if (regionInfo.getTable().isSystemTable()) {
        serversHostingSystemTables.add(cluster.servers[cluster.regionIndexToServerIndex[i]]);
      }
    }
    this.serversHostingSystemTables = serversHostingSystemTables;
  }

  @Override
  public boolean isViolating(RegionPlan regionPlan) {
    return checkViolation(regionPlan, serversHostingSystemTables);
  }

  protected static boolean checkViolation(RegionPlan regionPlan,
    Set<ServerName> serversHostingSystemTables) {
    boolean isSystemTable = regionPlan.getRegionInfo().getTable().isSystemTable();
    if (isSystemTable) {
      // Ensure the destination server has only system tables
      return !serversHostingSystemTables.contains(regionPlan.getDestination());
    } else {
      // Ensure the destination server has no system tables
      return serversHostingSystemTables.contains(regionPlan.getDestination());
    }
  }
}
