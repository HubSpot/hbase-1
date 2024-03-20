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

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * With this limiter resources will be refilled after a fixed interval, or in partial chunks of a
 * minimum size
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PartialIntervalRateLimiter extends RateLimiter {

  private static final double MINIMUM_REFILL_PROPORTION = 0.1;

  private long nextRefillTime = -1L;

  @Override
  public long refill(long limit) {
    final long now = EnvironmentEdgeManager.currentTime();
    final long timeUnitInMillis = getTimeUnitInMillis();
    if (now >= nextRefillTime) {
      nextRefillTime = now + timeUnitInMillis;
      return limit;
    }

    long msUntilNextRefill = nextRefillTime - now;
    long msSinceLastRefill = timeUnitInMillis - msUntilNextRefill;
    long minMsForPartialRefill = getMinMsForPartialRefill(timeUnitInMillis);

    if (msSinceLastRefill >= minMsForPartialRefill) {
      long partialRefillChunks = (long) (msSinceLastRefill / (double) minMsForPartialRefill);
      long partialRefill = (long) (limit * MINIMUM_REFILL_PROPORTION * partialRefillChunks);

      double ratioOfRefillToLimit = partialRefill / (double) limit;
      nextRefillTime += (timeUnitInMillis * ratioOfRefillToLimit);
      return partialRefill;
    }

    return 0;
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    if (isAvailable(amount) || nextRefillTime == -1) {
      return 0;
    }
    final long now = EnvironmentEdgeManager.currentTime();
    final long timeUnitInMillis = getTimeUnitInMillis();
    final long refillTime = nextRefillTime;
    long nextRefillInterval = refillTime - now;

    // If the requested amount is less than or equal to the available amount, no need to wait
    if (amount <= available) {
      return 0;
    }

    long diff = amount - available;
    if (diff <= limit) {
      // If the diff is less than the limit,
      // return a wait interval based on the required partial refills
      double partialRefillsNeeded =
        Math.ceil(Math.abs(diff / (double) limit / MINIMUM_REFILL_PROPORTION));
      long minMsForPartialRefill = getMinMsForPartialRefill(timeUnitInMillis);
      long waitIntervalForPartialRefill = (long) (partialRefillsNeeded * minMsForPartialRefill);
      return Math.min(nextRefillInterval, waitIntervalForPartialRefill);
    }

    // For requests larger than the limit, calculate additional full refills required
    long extraRefillsNecessary = (diff - 1) / limit;
    return nextRefillInterval + (extraRefillsNecessary * timeUnitInMillis);
  }

  // This method is for strictly testing purpose only
  @Override
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }

  @Override
  public long getNextRefillTime() {
    return this.nextRefillTime;
  }

  private static long getMinMsForPartialRefill(long timeUnitMs) {
    return Math.round(MINIMUM_REFILL_PROPORTION * timeUnitMs);
  }
}
