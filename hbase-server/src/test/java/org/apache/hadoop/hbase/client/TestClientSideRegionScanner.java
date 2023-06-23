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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.IndexOnlyLruBlockCache;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestClientSideRegionScanner {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientSideRegionScanner.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final byte[] FAM_NAME = Bytes.toBytes("f");
  private static final byte[] FAM_NAME_2 = Bytes.toBytes("f2");

  private Configuration conf;
  private Path rootDir;
  private FileSystem fs;
  private TableDescriptor htd;
  private RegionInfo hri;
  private Scan scan;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = TEST_UTIL.getTestFileSystem();
    htd = TEST_UTIL.getAdmin().getDescriptor(TableName.META_TABLE_NAME);
    hri = TEST_UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME).get(0);
    scan = new Scan();
  }

  @Test
  public void testDefaultBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNotNull(blockCache);
    assertTrue(blockCache instanceof IndexOnlyLruBlockCache);
    assertTrue(HConstants.HBASE_CLIENT_SCANNER_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT
        == blockCache.getMaxSize());
  }

  @Test
  public void testConfiguredBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    // tiny 1MB fixed cache size
    long blockCacheFixedSize = 1024 * 1024L;
    copyConf.setLong(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY, blockCacheFixedSize);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNotNull(blockCache);
    assertTrue(blockCache instanceof IndexOnlyLruBlockCache);
    assertTrue(blockCacheFixedSize == blockCache.getMaxSize());
  }

  @Test
  public void testNoBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    copyConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNull(blockCache);
  }

  private SingleColumnValueFilter createFilter(int row) {
    SingleColumnValueFilter filter = new SingleColumnValueFilter(FAM_NAME, Bytes.toBytes(row),
      CompareOperator.EQUAL, Bytes.toBytes(row));
    filter.setFilterIfMissing(true);
    return filter;
  }

  @Test
  public void testContinuesToScanIfHasMore() throws IOException {
    // In order to hit this bug, we need RegionScanner to exit out early despite having retrieved
    // no values. By default, RegionScannerImpl will continue until it gets values or a limit is
    // exceeded. In the context of this test, the easiest way to fail one of the limit checks is
    // to use joinedContinuationRow (essential family). To do that, we use a SingleColumnValueFilter
    // with setFilterIfMissing(true). This causes FAM_NAME_2 to be non-essential, and only brought
    // in when the filter passes. This causes us to go through one of our limit checks.
    // Prior to HBASE-27950, the first call to ClientSideRegionScanner.next() below would
    // return null, and there would further be nulls in between row 5 and 8. With the fix, we
    // appropriately return the rows 5 and 8, then null because the scanner is exhausted.

    Configuration copyConf = new Configuration(conf);
    int[] filteredRows = new int[] { 5, 8 };
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    for (int filteredRow : filteredRows) {
      filterList.addFilter(createFilter(filteredRow));
    }

    Scan scan = new Scan();
    scan.setFilter(filterList);
    scan.setLimit(1);

    try (Table table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAM_NAME, FAM_NAME_2 })) {
      TableDescriptor htd = TEST_UTIL.getAdmin().getDescriptor(TABLE_NAME);
      RegionInfo hri = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).get(0);

      for (int i = 0; i < 10; ++i) {
        table.put(createPut(i));
      }

      // Flush contents to disk so we can scan the fs
      TEST_UTIL.getAdmin().flush(TABLE_NAME);

      ClientSideRegionScanner clientSideRegionScanner =
        new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);
      RegionScanner scannerSpy = spy(clientSideRegionScanner.scanner);
      clientSideRegionScanner.scanner = scannerSpy;

      Result result;

      for (int filteredRow : filteredRows) {
        result = clientSideRegionScanner.next();
        assertNotNull(result);
        assertEquals(Bytes.toInt(result.getRow()), filteredRow);
        assertTrue(clientSideRegionScanner.hasMore);
      }

      result = clientSideRegionScanner.next();
      assertNull(result);
      assertFalse(clientSideRegionScanner.hasMore);
    }
  }

  private static Put createPut(int rowAsInt) {
    byte[] row = Bytes.toBytes(rowAsInt);
    Put put = new Put(row);
    put.addColumn(FAM_NAME, row, row);
    put.addColumn(FAM_NAME_2, row, row);
    return put;
  }

}
