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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

@Category(SmallTests.class)
public class TestTimeBasedLimiter {
  private static final int LIMIT = 1000;
  private static final QuotaProtos.TimedQuota REQUEST_THROTTLE =
    QuotaProtos.TimedQuota.newBuilder().setScope(QuotaProtos.QuotaScope.MACHINE).setSoftLimit(LIMIT)
      .setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build();
  private static final QuotaProtos.Throttle THROTTLE =
    QuotaProtos.Throttle.newBuilder().setReqNum(REQUEST_THROTTLE).build();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTimeBasedLimiter.class);

  @Test
  public void itPassesReqNumLimiter() {
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(THROTTLE);
    assertFalse(getRTE(limiter, 0).isPresent());
  }

  @Test
  public void itRejectsWithTinyWaitIntervalByDefault() {
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(THROTTLE);
    Optional<RpcThrottlingException> rte = getRTE(limiter, 1001);
    assertTrue(rte.isPresent());
    assertEquals(rte.get().getWaitInterval(), 1);
  }

  @Test
  public void itRejectsWithLargerWaitIntervalIfConfigured() {
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(THROTTLE);
    long expectedWaitInterval = 500;
    limiter.refreshMinWaitInterval(expectedWaitInterval);
    Optional<RpcThrottlingException> rte = getRTE(limiter, 1001);
    assertTrue(rte.isPresent());
    assertEquals(rte.get().getWaitInterval(), expectedWaitInterval);
  }

  private Optional<RpcThrottlingException> getRTE(QuotaLimiter limiter, int reqNum) {
    try {
      limiter.checkQuota(0, 0, reqNum, 0, 0, 0);
    } catch (RpcThrottlingException e) {
      return Optional.of(e);
    }
    return Optional.empty();
  }
}
