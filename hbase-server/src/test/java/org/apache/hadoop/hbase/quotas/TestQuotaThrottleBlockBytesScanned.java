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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestQuotaThrottleBlockBytesScanned {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestQuotaThrottleBlockBytesScanned.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.USE_BLOCK_BYTES_SCANNED_KEY, true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;
  }

  /**
   * Test proves that blockBytesScanned quotas work because we write a bunch of very small rows,
   * filling up a default block size. We set quota size to much greater than 1 row size, then we try
   * fetching the same row twice. Fetching the row loads the entire block, so despite only fetching
   * a small value we increment quota consumed by the total block. Therefore, the next request for
   * the same row (or any other row) fails with throttle exception.
   */
  @Test
  public void testBlockBytesScanned() throws IOException {
    TableName tableName = TableName.valueOf("testBlockBytesScanned");
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory.throttleTable(tableName,
      ThrottleType.READ_SIZE, 2048, TimeUnit.MINUTES));
    byte[] family = Bytes.toBytes("0");
    Table table = TEST_UTIL.createTable(tableName, family);

    int totalSize = 0;
    int i = 0;
    while (totalSize < HConstants.DEFAULT_BLOCKSIZE) {
      Put put = new Put(Bytes.toBytes(i++)).addColumn(family, Bytes.toBytes(0), new byte[0]);
      totalSize += put.getFamilyCellMap().firstEntry().getValue().stream()
        .mapToInt(Cell::getSerializedSize).sum();
      table.put(put);
    }

    // flush so reads actually read a block (rather than read from memstore, which is more free)
    TEST_UTIL.flush(tableName);

    table.get(new Get(Bytes.toBytes(0)));
    RetriesExhaustedException exc =
      assertThrows(RetriesExhaustedException.class, () -> table.get(new Get(Bytes.toBytes(0))));
    assertTrue(exc.getCause() instanceof RpcThrottlingException);
  }
}
