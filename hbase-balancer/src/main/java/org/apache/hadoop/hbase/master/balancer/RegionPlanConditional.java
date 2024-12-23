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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RegionPlanConditional {

  private static final Logger LOG = LoggerFactory.getLogger(RegionPlanConditional.class);
  private static final int TRIES_BEFORE_FORCE = 10;

  private final BalancerClusterState cluster;

  public RegionPlanConditional(Configuration conf, BalancerClusterState cluster) {
    this.cluster = cluster;
  }

  int pickRandomServer() {
    if (cluster.numServers < 1) {
      return -1;
    }

    return ThreadLocalRandom.current().nextInt(cluster.numServers);
  }

  int pickOtherRandomServer(int serverIndex) {
    if (cluster.numServers < 2) {
      return -1;
    }
    while (true) {
      int otherServerIndex = pickRandomServer();
      if (otherServerIndex != serverIndex) {
        return otherServerIndex;
      }
    }
  }

  RegionPlan findGoodPlanOrForce(int serverIndex, int regionIndex) {
    return findGoodPlanOrForce(serverIndex, cluster.regions[regionIndex], () -> pickOtherRandomServer(serverIndex));
  }

  RegionPlan findGoodPlanOrForce(int serverIndex, RegionInfo regionInfo, Callable<Integer> targetCallable) {
    RegionPlan plan = null;
    for (int i = 0; i < TRIES_BEFORE_FORCE; i++) {
      int maybeTarget = -1;
      try {
        maybeTarget = targetCallable.call();
      } catch (Exception e) {
        LOG.warn("Failed to fetch target server! Falling back on random server.", e);
        maybeTarget = pickOtherRandomServer(serverIndex);
      }
      plan = new RegionPlan(regionInfo, cluster.servers[serverIndex], cluster.servers[maybeTarget]);
      if (BalancerConditionals.INSTANCE.isAcceptable(plan)) {
        return plan;
      }
    }
    return plan;
  }

  /**
   * This may be called frequently by the balancer depending on the rate at which
   * we're accepting moves. This method should do everything necessary to keep your
   * conditional up-to-date with the changing state of the cluster.
   * todo rmattingly can we remove this and rarely reconstruct?
   * @param conf the cluster's configuration
   * @param cluster the current state of the cluster
   */
  abstract void refresh(Configuration conf, BalancerClusterState cluster);

  /**
   * Check if the conditional is violated by the given region plan.
   * @param regionPlan the region plan to check
   * @return true if the conditional is violated
   */
  abstract boolean isViolating(RegionPlan regionPlan);

  /**
   * This method generates moves that will move us towards compliance with
   * the given conditional. This method should not modify the state of the conditional.
   * With an appropriate implementation for this method, you should not need an
   * additional candidate generator to support your conditional.
   * Moves suggested by this method will be accepted by the balancer regardless of
   * their affect on the cost functions.
   * @return a list of moves that will move us towards compliance with the conditional
   */
  abstract List<RegionPlan> computeNextMoves();

  /**
   * Accept the move, updating the conditional's state as necessary.
   * This should be called after the move has been accepted by the balancer.
   * @param balanceAction The move that has been accepted
   */
  abstract void acceptMove(BalanceAction balanceAction);
}
