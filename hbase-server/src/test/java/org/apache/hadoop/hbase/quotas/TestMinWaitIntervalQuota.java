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

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerUserCacheRefresh;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.shaded.com.google.common.base.Throwables;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestMinWaitIntervalQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMinWaitIntervalQuota.class);

  private static final int REFRESH_TIME = 5000;
  private static final long MIN_WAIT_INTERVAL = 500L;
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private static final TableName TABLE_NAME = TableName.valueOf("MinWaitIntervalQuotaTest");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // client should fail fast
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setClass(RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY,
      AverageIntervalRateLimiter.class, RateLimiter.class);
    TEST_UTIL.getConfiguration().setLong(TimeBasedLimiter.MIN_WAIT_INTERVAL_KEY, MIN_WAIT_INTERVAL);

    // quotas enabled, using block bytes scanned
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
  }

  @Test
  public void testQuotasMinWaitInterval() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    int blockSize = admin.getDescriptor(TABLE_NAME).getColumnFamily(FAMILY).getBlocksize();
    Table table = admin.getConnection().getTable(TABLE_NAME);

    // Add <1 block / sec
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_SIZE,
      blockSize - 1000, TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);

    List<Get> gets = new ArrayList<>(1);
    for (int i = 0; i < 1; i++) {
      gets.add(new Get(Bytes.toBytes(i)));
    }
    boolean threwAppropriateException = false;
    try {
      table.get(gets);
    } catch (Exception e) {
      threwAppropriateException =
        Throwables.getCausalChain(e).stream().anyMatch(t -> t instanceof RpcThrottlingException
          && ((RpcThrottlingException) t).getWaitInterval() == MIN_WAIT_INTERVAL);
    }
    assertTrue(threwAppropriateException);
  }

}
