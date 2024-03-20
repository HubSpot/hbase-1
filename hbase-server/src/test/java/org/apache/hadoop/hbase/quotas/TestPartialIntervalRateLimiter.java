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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestPartialIntervalRateLimiter {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPartialIntervalRateLimiter.class);

  @Test
  public void itRunsFullRefills() {
    RateLimiter limiter = new PartialIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());

    // Consume the quota
    limiter.consume(10);

    // Need to wait 1s to acquire another resource
    assertEquals(1000, limiter.waitInterval(10));
    // We need to wait 2s to acquire more than 10 resources
    assertEquals(2000, limiter.waitInterval(20));

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    // We've waited the full interval, so we should now have 10
    assertEquals(0, limiter.getWaitIntervalMs(10));
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void itRunsPartialRefills() {
    RateLimiter limiter = new PartialIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());

    // Consume the quota
    limiter.consume(10);

    // Need to wait 1s to acquire another resource
    long waitInterval = limiter.waitInterval(10);
    assertTrue(900 < waitInterval);
    assertTrue(1000 >= waitInterval);
    // We need to wait 2s to acquire more than 10 resources
    waitInterval = limiter.waitInterval(20);
    assertTrue(1900 < waitInterval);
    assertTrue(2000 >= waitInterval);
    // We need to wait 0<=x<=100ms to acquire 1 resource
    waitInterval = limiter.waitInterval(1);
    assertTrue(0 < waitInterval);
    assertTrue(100 >= waitInterval);

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    // We've waited half the interval, so we should now have half available
    assertEquals(0, limiter.getWaitIntervalMs(5));
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void itRunsRepeatedPartialRefills() {
    RateLimiter limiter = new PartialIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);
    assertEquals(0, limiter.getWaitIntervalMs());
    // Consume the quota
    limiter.consume(10);
    for (int i = 0; i < 100; i++) {
      limiter.setNextRefillTime(limiter.getNextRefillTime() - 100); // free 1 resource
      limiter.consume(1);
      limiter.waitInterval();
      assertEquals(0, limiter.getAvailable());
    }
  }
}
