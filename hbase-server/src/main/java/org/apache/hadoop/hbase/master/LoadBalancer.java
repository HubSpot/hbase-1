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
package org.apache.hadoop.hbase.master;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Makes decisions about the placement and movement of Regions across RegionServers.
 * <p/>
 * Cluster-wide load balancing will occur only when there are no regions in transition and according
 * to a fixed period of a time using {@link #balanceCluster(Map)}.
 * <p/>
 * On cluster startup, bulk assignment can be used to determine locations for all Regions in a
 * cluster.
 * <p/>
 * This class produces plans for the {@code AssignmentManager} to execute.
 * <p/>
 * About locking:
 * <ul>
 * <li>We will first call {@link #setMasterServices(MasterServices)} and then {@link #initialize()}
 * to initialize the balancer, and before calling {@link #initialize()}, we will never call any
 * methods of this balancer. So these two methods do not need to be synchronized.</li>
 * <li>The {@link #balanceCluster(Map)} method will use the {@link ClusterMetrics} which is set by
 * {@link #updateClusterMetrics(ClusterMetrics)}, and also lots of configurations, which could be
 * changed by {@link #onConfigurationChange(Configuration)}, so the easier way is to make these
 * three methods synchronized. And since there will be only one balancing thread, this will not
 * impact performance too much.</li>
 * <li>The {@link #roundRobinAssignment(List, List)}, {@link #retainAssignment(Map, List)} and
 * {@link #randomAssignment(RegionInfo, List)} could be called from multiple threads concurrently,
 * so these three methods should not be synchronized, the implementation classes need to make sure
 * that they are thread safe.</li>
 * </ul>
 */
@InterfaceAudience.Private
public interface LoadBalancer extends Stoppable, ConfigurationObserver {
  /**
   * Master can carry regions as of hbase-2.0.0. By default, it carries no tables. TODO: Add any |
   * system as flags to indicate what it can do.
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  String TABLES_ON_MASTER = "hbase.balancer.tablesOnMaster";

  /**
   * Master carries system tables.
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  String SYSTEM_TABLES_ON_MASTER = "hbase.balancer.tablesOnMaster.systemTablesOnly";

  // Used to signal to the caller that the region(s) cannot be assigned
  // We deliberately use 'localhost' so the operation will fail fast
  ServerName BOGUS_SERVER_NAME = ServerName.valueOf("localhost,1,1");

  /**
   * Set the current cluster status. This allows a LoadBalancer to map host name to a server
   */
  void updateClusterMetrics(ClusterMetrics metrics);

  /**
   * Set the master service.
   */
  void setMasterServices(MasterServices masterServices);

  /**
   * Perform the major balance operation for cluster.
   * @param loadOfAllTable region load of servers for all table
   * @return a list of regions to be moved, including source and destination, or null if cluster is
   *         already balanced
   */
  List<RegionPlan> balanceCluster(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable)
    throws IOException;

  /**
   * Perform a Round Robin assignment of regions.
   * @return Map of servername to regioninfos
   */
  @NonNull
  Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
    List<ServerName> servers) throws HBaseIOException;

  /**
   * Assign regions to the previously hosting region server
   * @return List of plans
   */
  @NonNull
  Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
    List<ServerName> servers) throws HBaseIOException;

  /**
   * Get a random region server from the list
   * @param regionInfo Region for which this selection is being done.
   */
  ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
    throws HBaseIOException;

  /**
   * Initialize the load balancer. Must be called after setters.
   */
  void initialize() throws HBaseIOException;

  /**
   * Marks the region as online at balancer.
   */
  void regionOnline(RegionInfo regionInfo, ServerName sn);

  /**
   * Marks the region as offline at balancer.
   */
  void regionOffline(RegionInfo regionInfo);

  /*
   * Notification that config has changed
   */
  @Override
  void onConfigurationChange(Configuration conf);

  /**
   * If balancer needs to do initialization after Master has started up, lets do that here.
   */
  void postMasterStartupInitialize();

  /* Updates balancer status tag reported to JMX */
  void updateBalancerStatus(boolean status);

  /**
   * In some scenarios, Balancer needs to update internal status or information according to the
   * current tables load
   * @param loadOfAllTable region load of servers for all table
   */
  default void
    updateBalancerLoadInfo(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
  }

  /**
   * @return true if Master carries regions
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  static boolean isTablesOnMaster(Configuration conf) {
    return conf.getBoolean(TABLES_ON_MASTER, false);
  }

  /**
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  static boolean isSystemTablesOnlyOnMaster(Configuration conf) {
    return conf.getBoolean(SYSTEM_TABLES_ON_MASTER, false);
  }

  /**
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  static boolean isMasterCanHostUserRegions(Configuration conf) {
    return isTablesOnMaster(conf) && !isSystemTablesOnlyOnMaster(conf);
  }
}
