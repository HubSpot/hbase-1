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

@InterfaceAudience.Private public class PrefixPerformanceCostFunction extends PrefixCostFunction {

  private static final String PREFIX_PERFORMANCE_COST =
    "hbase.master.balancer.stochastic.prefixPerformanceCost";

  private static final float DEFAULT_PREFIX_PERFORMANCE_COST = 0;

  PrefixPerformanceCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(PREFIX_PERFORMANCE_COST, DEFAULT_PREFIX_PERFORMANCE_COST));
    this.setTargetPrefixDispersion(conf.getFloat(PREFIX_DISPERSION, DEFAULT_PREFIX_DISPERSION));
  }

  @Override double computeServerCost(double serverDispersion, double targetDispersion) {
    return Math.max(0, targetDispersion - serverDispersion);
  }
}
