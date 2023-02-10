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
package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link MetricsTableLatencies} to track latencies for one table in a
 * RegionServer.
 */
@InterfaceAudience.Private
public class MetricsTableLatenciesImpl extends BaseSourceImpl implements MetricsTableLatencies {

  private final HashMap<TableName, TableMetrics> histogramsByTable = new HashMap<>();

  public static class TableMetrics {
    final MetricHistogram getTimeHisto;
    final MetricHistogram incrementTimeHisto;
    final MetricHistogram appendTimeHisto;
    final MetricHistogram putTimeHisto;
    final MetricHistogram putBatchTimeHisto;
    final MetricHistogram deleteTimeHisto;
    final MetricHistogram deleteBatchTimeHisto;
    final MetricHistogram scanTimeHisto;
    final MetricHistogram scanSizeHisto;
    final MetricHistogram checkAndDeleteTimeHisto;
    final MetricHistogram checkAndPutTimeHisto;
    final MetricHistogram checkAndMutateTimeHisto;

    private final MutableFastCounter blockBytesScannedCount;
    private final MetricHistogram checkAndMutateBlockBytesScanned;
    private final MetricHistogram getBlockBytesScanned;
    private final MetricHistogram incrementBlockBytesScanned;
    private final MetricHistogram appendBlockBytesScanned;
    private final MetricHistogram scanBlockBytesScanned;

    TableMetrics(DynamicMetricsRegistry registry, TableName tn) {
      getTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, GET_TIME));
      incrementTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, INCREMENT_TIME));
      appendTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, APPEND_TIME));
      putTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, PUT_TIME));
      putBatchTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, PUT_BATCH_TIME));
      deleteTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, DELETE_TIME));
      deleteBatchTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, DELETE_BATCH_TIME));
      scanTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, SCAN_TIME));
      scanSizeHisto = registry.newSizeHistogram(qualifyMetricsName(tn, SCAN_SIZE));
      checkAndDeleteTimeHisto =
        registry.newTimeHistogram(qualifyMetricsName(tn, CHECK_AND_DELETE_TIME));
      checkAndPutTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, CHECK_AND_PUT_TIME));
      checkAndMutateTimeHisto =
        registry.newTimeHistogram(qualifyMetricsName(tn, CHECK_AND_MUTATE_TIME));

      blockBytesScannedCount = registry.newCounter(qualifyMetricsName(tn, BLOCK_BYTES_SCANNED_KEY),
        BLOCK_BYTES_SCANNED_DESC, 0L);
      checkAndMutateBlockBytesScanned =
        registry.newSizeHistogram(qualifyMetricsName(tn, CHECK_AND_MUTATE_BLOCK_BYTES_SCANNED_KEY));
      getBlockBytesScanned =
        registry.newSizeHistogram(qualifyMetricsName(tn, GET_BLOCK_BYTES_SCANNED_KEY));
      incrementBlockBytesScanned =
        registry.newSizeHistogram(qualifyMetricsName(tn, INCREMENT_BLOCK_BYTES_SCANNED_KEY));
      appendBlockBytesScanned =
        registry.newSizeHistogram(qualifyMetricsName(tn, APPEND_BLOCK_BYTES_SCANNED_KEY));
      scanBlockBytesScanned =
        registry.newSizeHistogram(qualifyMetricsName(tn, SCAN_BLOCK_BYTES_SCANNED_KEY));
    }

    public void updatePut(long time) {
      putTimeHisto.add(time);
    }

    public void updatePutBatch(long time) {
      putBatchTimeHisto.add(time);
    }

    public void updateDelete(long t) {
      deleteTimeHisto.add(t);
    }

    public void updateDeleteBatch(long t) {
      deleteBatchTimeHisto.add(t);
    }

    public void updateGet(long t, long blockBytesScanned) {
      getTimeHisto.add(t);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.incr(blockBytesScanned);
        getBlockBytesScanned.add(blockBytesScanned);
      }
    }

    public void updateIncrement(long t, long blockBytesScanned) {
      incrementTimeHisto.add(t);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.incr(blockBytesScanned);
        incrementBlockBytesScanned.add(blockBytesScanned);
      }
    }

    public void updateAppend(long t, long blockBytesScanned) {
      appendTimeHisto.add(t);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.incr(blockBytesScanned);
        appendBlockBytesScanned.add(blockBytesScanned);
      }
    }

    public void updateScanSize(long responseSize, long blockBytesScanned) {
      scanSizeHisto.add(responseSize);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.incr(blockBytesScanned);
        scanBlockBytesScanned.add(blockBytesScanned);
      }
    }

    public void updateScanTime(long t) {
      scanTimeHisto.add(t);
    }

    public void updateCheckAndDeleteTime(long t) {
      checkAndDeleteTimeHisto.add(t);
    }

    public void updateCheckAndPutTime(long t) {
      checkAndPutTimeHisto.add(t);
    }

    public void updateCheckAndMutateTime(long t, long blockBytesScanned) {
      checkAndMutateTimeHisto.add(t);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.incr(blockBytesScanned);
        checkAndMutateBlockBytesScanned.add(blockBytesScanned);
      }
    }
  }

  public static String qualifyMetricsName(TableName tableName, String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    sb.append("_metric_").append(metric);
    return sb.toString();
  }

  public TableMetrics getOrCreateTableMetrics(String tableName) {
    // TODO Java8's ConcurrentHashMap#computeIfAbsent would be stellar instead
    final TableName tn = TableName.valueOf(tableName);
    TableMetrics latency = histogramsByTable.get(tn);
    if (latency == null) {
      latency = new TableMetrics(getMetricsRegistry(), tn);
      histogramsByTable.put(tn, latency);
    }
    return latency;
  }

  public MetricsTableLatenciesImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsTableLatenciesImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void updatePut(String tableName, long t) {
    getOrCreateTableMetrics(tableName).updatePut(t);
  }

  @Override
  public void updatePutBatch(String tableName, long t) {
    getOrCreateTableMetrics(tableName).updatePutBatch(t);
  }

  @Override
  public void updateDelete(String tableName, long t) {
    getOrCreateTableMetrics(tableName).updateDelete(t);
  }

  @Override
  public void updateDeleteBatch(String tableName, long t) {
    getOrCreateTableMetrics(tableName).updateDeleteBatch(t);
  }

  @Override
  public void updateGet(String tableName, long t, long blockBytesScanned) {
    getOrCreateTableMetrics(tableName).updateGet(t, blockBytesScanned);
  }

  @Override
  public void updateIncrement(String tableName, long t, long blockBytesScanned) {
    getOrCreateTableMetrics(tableName).updateIncrement(t, blockBytesScanned);
  }

  @Override
  public void updateAppend(String tableName, long t, long blockBytesScanned) {
    getOrCreateTableMetrics(tableName).updateAppend(t, blockBytesScanned);
  }

  @Override
  public void updateScanSize(String tableName, long responseSize, long blockBytesScanned) {
    getOrCreateTableMetrics(tableName).updateScanSize(responseSize, blockBytesScanned);
  }

  @Override
  public void updateScanTime(String tableName, long t) {
    getOrCreateTableMetrics(tableName).updateScanTime(t);
  }

  @Override
  public void updateCheckAndDelete(String tableName, long time) {
    getOrCreateTableMetrics(tableName).updateCheckAndDeleteTime(time);
  }

  @Override
  public void updateCheckAndPut(String tableName, long time) {
    getOrCreateTableMetrics(tableName).updateCheckAndPutTime(time);
  }

  @Override
  public void updateCheckAndMutate(String tableName, long time, long blockBytesScanned) {
    getOrCreateTableMetrics(tableName).updateCheckAndMutateTime(time, blockBytesScanned);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);
    // source is registered in supers constructor, sometimes called before the whole initialization.
    metricsRegistry.snapshot(mrb, all);
    if (metricsAdapter != null) {
      // snapshot MetricRegistry as well
      metricsAdapter.snapshotAllMetrics(registry, mrb);
    }
  }
}
