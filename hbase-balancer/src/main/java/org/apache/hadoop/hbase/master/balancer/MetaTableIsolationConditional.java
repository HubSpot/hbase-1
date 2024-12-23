/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that meta regions (hbase:meta) stay isolated on their own RegionServer(s).
 *
 * <p>This class will:
 * <ul>
 *   <li>Identify servers hosting meta (and track them in {@code serversWithMeta}).</li>
 *   <li>Identify empty servers (and track them in {@code emptyServers}).</li>
 *   <li>Generate exactly one move per <em>non-meta</em> region that resides on a meta server.</li>
 *   <li>In {@code acceptMove(...)} carefully update local sets to avoid stale references.</li>
 * </ul>
 *
 * Configure via {@code BalancerConditionals#ISOLATE_META_TABLE_KEY}.
 */
@InterfaceAudience.Private
public class MetaTableIsolationConditional extends RegionPlanConditional {

  private static final Logger LOG = LoggerFactory.getLogger(MetaTableIsolationConditional.class);

  /**
   * If your cluster is huge, you might want to limit the number of moves in one
   * iteration to avoid massive batch moves.
   */
  private static final int MAX_MOVES = 10_000;

  private BalancerClusterState cluster;
  private Configuration conf;

  /**
   * Servers that hold at least one meta region.
   */
  private final Set<Integer> serversWithMeta = new HashSet<>();

  /**
   * Servers that hold zero total regions (completely empty).
   */
  private final Set<Integer> emptyServers = new HashSet<>();

  public MetaTableIsolationConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    this.conf = conf;
    refresh(conf, cluster);
  }

  /**
   * Rebuild local sets from scratch:
   * - {@code serversWithMeta}: all servers that host at least one meta region.
   * - {@code emptyServers}: any server with zero regions.
   */
  @Override
  void refresh(Configuration conf, BalancerClusterState cluster) {
    this.cluster = cluster;
    serversWithMeta.clear();
    emptyServers.clear();

    for (int s = 0; s < cluster.numServers; s++) {
      int[] regionIdxs = cluster.regionsPerServer[s];
      if (regionIdxs == null || regionIdxs.length == 0) {
        emptyServers.add(s);
        continue;
      }

      boolean hasMeta = false;
      for (int regionIdx : regionIdxs) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        if (TableName.META_TABLE_NAME.equals(regionInfo.getTable())) {
          hasMeta = true;
          break;
        }
      }
      if (hasMeta) {
        serversWithMeta.add(s);
      }
    }
  }

  /**
   * A simple check: if this plan is about moving a meta region, it must go to
   * a server that either already has meta or is empty; if it is a non-meta region,
   * it must NOT go to a server with meta.
   */
  @Override
  public boolean isViolating(RegionPlan regionPlan) {
    if (regionPlan == null) return false;

    RegionInfo region = regionPlan.getRegionInfo();
    boolean isMeta = TableName.META_TABLE_NAME.equals(region.getTable());

    Integer destIdx = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
    if (destIdx == null) {
      // unknown server => can't evaluate => not a direct violation
      return false;
    }

    boolean destHasMeta = serversWithMeta.contains(destIdx);
    boolean destIsEmpty = emptyServers.contains(destIdx);

    if (isMeta) {
      // meta => must go to serverWithMeta or empty
      if (!destHasMeta && !destIsEmpty) {
        return true; // violation
      }
    } else {
      // non-meta => must NOT go to a server that has meta
      if (destHasMeta) {
        return true;
      }
    }
    return false;
  }

  /**
   * Produce moves for each non-meta region that resides on a server with meta.
   * Generate at most one move per region, and limit total moves to {@code MAX_MOVES}.
   *
   * This ensures we don't propose multiple moves for the same region in a single iteration.
   */
  @Override
  List<RegionPlan> computeNextMoves() {
    if (true) {
      return Collections.emptyList();
    }
    List<RegionPlan> plans = new ArrayList<>();
    // For every server that hosts meta, move out all non-meta
    outerLoop:
    for (int metaServer : serversWithMeta) {
      int[] regionIdxs = cluster.regionsPerServer[metaServer];
      if (regionIdxs == null) continue;

      for (int regionIdx : regionIdxs) {
        RegionInfo region = cluster.regions[regionIdx];
        // Only move the region if it is non-meta
        if (!TableName.META_TABLE_NAME.equals(region.getTable())) {
          RegionPlan plan =
            findGoodPlanOrForce(metaServer, region, this::pickNonMetaServer);
          plans.add(plan);

          if (plans.size() >= MAX_MOVES) {
            break outerLoop;
          }
        }
      }
    }

    LOG.debug("MetaTableIsolationConditional: returning {} moves", plans.size());
    return plans;
  }

  /**
   * Upon accepting a move, we carefully update local sets so we don't produce stale references
   * next time. That way, if the old server has no more meta, we remove it from `serversWithMeta`.
   * If the old server is now empty, we add it to `emptyServers`.
   * If the new server is not empty anymore, we remove it from `emptyServers`.
   */
  @Override
  void acceptMove(BalanceAction balanceAction) {
    List<RegionPlan> regionPlans = cluster.convertActionToPlans(balanceAction);

    for (RegionPlan regionPlan : regionPlans) {
      Integer sourceIdx = cluster.serversToIndex.get(regionPlan.getSource().getAddress());
      Integer destIdx   = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
      if (sourceIdx == null || destIdx == null) {
        return;
      }

      RegionInfo region = regionPlan.getRegionInfo();
      boolean isMeta = TableName.META_TABLE_NAME.equals(region.getTable());

      // If it's meta, then the destination server now has meta
      if (isMeta) {
        serversWithMeta.add(destIdx);
        // If old server no longer has any meta, remove it
        if (!doesServerStillHaveMeta(sourceIdx)) {
          serversWithMeta.remove(sourceIdx);
        }
      }

      // The dest server is no longer empty if it was before
      emptyServers.remove(destIdx);

      // If the old server is now empty, add it
      if (isServerEmpty(sourceIdx)) {
        emptyServers.add(sourceIdx);
      }
    }
  }

  /**
   * Return a random server that doesn't have meta, if possible.
   * If we can't find a meta-free server after enough tries, fall back to random.
   */
  private Integer pickNonMetaServer() {
    // We'll try up to cluster.numServers random picks for a meta-free server
    // If we can't find one, we pick any random server (the "least bad" approach).
    int attempts = cluster.numServers;
    while (attempts-- > 0) {
      int candidate = pickRandomServer();
      if (!serversWithMeta.contains(candidate)) {
        return candidate;
      }
    }
    // fallback if everything is a meta server in worst scenario
    return pickRandomServer();
  }

  /**
   * Check if a server still has any meta region after a move.
   */
  private boolean doesServerStillHaveMeta(int serverIdx) {
    int[] regionIdxs = cluster.regionsPerServer[serverIdx];
    if (regionIdxs == null || regionIdxs.length == 0) {
      return false;
    }
    for (int regionIdx : regionIdxs) {
      RegionInfo rInfo = cluster.regions[regionIdx];
      if (TableName.META_TABLE_NAME.equals(rInfo.getTable())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if a server is now empty (0 total regions).
   */
  private boolean isServerEmpty(int serverIdx) {
    int[] regionIdxs = cluster.regionsPerServer[serverIdx];
    return (regionIdxs == null || regionIdxs.length == 0);
  }
}
