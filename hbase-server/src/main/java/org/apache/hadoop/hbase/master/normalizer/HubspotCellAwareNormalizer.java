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
package org.apache.hadoop.hbase.master.normalizer;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HubspotCellAwareNormalizer extends SimpleRegionNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(HubspotCellAwareNormalizer.class);

  @Override
  public List<NormalizationPlan> computePlansForTable(TableDescriptor tableDescriptor) {
    List<NormalizationPlan> allPlans = super.computePlansForTable(tableDescriptor);
    List<NormalizationPlan> filteredPlans = new ArrayList<>(allPlans.size());

    for (NormalizationPlan plan : allPlans) {
      boolean shouldInclude = shouldIncludePlan(plan);
      if (shouldInclude) {
        filteredPlans.add(plan);
      } else {
        LOG.info("Skipping plan: {}", plan);
      }
    }

    return filteredPlans;
  }

  private static boolean shouldIncludePlan(NormalizationPlan plan) {
    switch (plan.getType()) {
      case MERGE:
        return shouldIncludeMergePlan((MergeNormalizationPlan) plan);
      case NONE:
      case SPLIT:
        return true;
      default:
        throw new RuntimeException("Unknown plan type: " + plan.getType());
    }
  }

  private static boolean shouldIncludeMergePlan(MergeNormalizationPlan plan) {
    List<NormalizationTarget> targets = plan.getNormalizationTargets();

    if (targets.size() <= 1) {
      return true;
    }

    byte[] endKey = targets.get(0).getRegionInfo().getEndKey();
    short cell = Bytes.toShort(endKey);

    for (int i = 1; i < targets.size(); ++i) {
      endKey = targets.get(i).getRegionInfo().getEndKey();
      if (cell != Bytes.toShort(endKey)) {
        return false;
      }
    }

    return true;
  }
}
