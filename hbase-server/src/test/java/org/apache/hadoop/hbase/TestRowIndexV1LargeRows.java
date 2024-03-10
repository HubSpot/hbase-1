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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, MediumTests.class })
public class TestRowIndexV1LargeRows {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowIndexV1LargeRows.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRowIndexV1LargeRows.class);
  private static final TableName RIV1_TABLE_NAME = TableName.valueOf("RIV1Table");
  private static final TableName PREFIX_TABLE_NAME = TableName.valueOf("PrefixTable");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_COLS = 64 * 1024 * 150; // a 150+ block row
  private static final int MIN_LEN = 1;
  private static final int MAX_LEN = 10;
  protected static final LoadTestKVGenerator GENERATOR = new LoadTestKVGenerator(MIN_LEN, MAX_LEN);
  protected static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @BeforeClass
  public static void setupClass() throws Exception {
    // don't cache blocks to make IO predictable
    TEST_UTIL.getConfiguration().setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);

    TEST_UTIL.startMiniCluster();

    TEST_UTIL.createTable(RIV1_TABLE_NAME, FAMILY);
    TEST_UTIL.createTable(PREFIX_TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(RIV1_TABLE_NAME);
    TEST_UTIL.waitTableAvailable(PREFIX_TABLE_NAME);
    setDBE(TEST_UTIL.getAdmin(), RIV1_TABLE_NAME, DataBlockEncoding.ROW_INDEX_V1);
    setDBE(TEST_UTIL.getAdmin(), PREFIX_TABLE_NAME, DataBlockEncoding.PREFIX);
  }

  private static void setDBE(Admin admin, TableName tableName, DataBlockEncoding dbe)
    throws IOException, InterruptedException {
    ColumnFamilyDescriptor cf = admin.getDescriptor(tableName).getColumnFamily(FAMILY);
    ColumnFamilyDescriptor modifiedCf = ColumnFamilyDescriptorBuilder.newBuilder(cf).setDataBlockEncoding(dbe).build();
    admin.modifyColumnFamily(tableName, modifiedCf);
    Thread.sleep(1000);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLargeRowSmallCellRIV1GetPerformance() throws Exception {
    try (
      Table riv1Table = TEST_UTIL.getConnection().getTable(RIV1_TABLE_NAME);
      Table prefixTable = TEST_UTIL.getConnection().getTable(PREFIX_TABLE_NAME)
    ) {
      List<Long> riv1Results = new ArrayList<>(10);
      List<Long> prefixResults = new ArrayList<>(10);
      for (int j = 0; j < 10; j++) {
        final byte[] rowKey = Bytes.toBytes(LoadTestKVGenerator.md5PrefixedKey(j));
        Put put = new Put(rowKey);
        for (int i = 0; i < NUM_COLS; i++) {
          final byte[] v = GENERATOR.generateRandomSizeValue(rowKey, QUALIFIER);
          put.addColumn(FAMILY, QUALIFIER, v);
          if (i % 10000 == 0) {
            riv1Table.put(put);
            prefixTable.put(put);
            put = new Put(rowKey);
          }
        }
        riv1Table.put(put);
        prefixTable.put(put);

        TEST_UTIL.flush(riv1Table.getName());
        TEST_UTIL.flush(prefixTable.getName());

        Get get = new Get(rowKey).setCacheBlocks(false);
        riv1Results.add(testGet("RIV1", riv1Table, get));
        prefixResults.add(testGet("Prefix", prefixTable, get));
      }
      LOG.info("RIV1 results: {}\nAverage: {}", riv1Results.stream().map(String::valueOf).collect(
        Collectors.joining(", ")), riv1Results.stream().mapToLong(l -> l).average().getAsDouble());
      LOG.info("Prefix results: {}\nAverage: {}", prefixResults.stream().map(String::valueOf).collect(
        Collectors.joining(", ")), prefixResults.stream().mapToLong(l -> l).average().getAsDouble());
    }
  }

  private long testGet(String name, Table table, Get get) throws IOException {
    long start = System.currentTimeMillis();
    table.get(get);
    long end = System.currentTimeMillis();
    LOG.info("{} took {}ms", name, end - start);
    return end - start;
  }

}
