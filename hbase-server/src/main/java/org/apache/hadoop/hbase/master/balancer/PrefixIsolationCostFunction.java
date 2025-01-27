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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private public class PrefixIsolationCostFunction extends PrefixCostFunction {

  private static final String PREFIX_ISOLATION_COST =
    "hbase.master.balancer.stochastic.prefixIsolationCost";

  private static final float DEFAULT_PREFIX_ISOLATION_COST = 0;

  PrefixIsolationCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(PREFIX_ISOLATION_COST, DEFAULT_PREFIX_ISOLATION_COST));
    this.setIsolationToPerformanceRatio(conf.getFloat(PREFIX_ISOLATION_TO_PERFORMANCE_RATIO, DEFAULT_PREFIX_ISOLATION_TO_PERFORMANCE_RATIO));
  }

  @Override double computeServerCost(double serverRatio, double targetRatio) {
    return Math.max(0, serverRatio - targetRatio);
  }

  @Override public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.RANDOM.ordinal()] += cost();
  }
}
