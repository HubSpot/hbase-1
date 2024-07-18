package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class MultiSnapshotInputFormat extends TableSnapshotInputFormat implements InputFormat<ImmutableBytesWritable, Result> {
  private final MultiSnapshotInputFormatImpl delegate;

  public MultiSnapshotInputFormat(MultiSnapshotInputFormatImpl delegate) {
    this.delegate = delegate;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = delegate.getSplits(job);
    InputSplit[] results = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      results[i] = new TableSnapshotRegionSplit(splits.get(i));
    }
    return results;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(InputSplit split, JobConf job,
    Reporter reporter) throws IOException {
    return new TableSnapshotRecordReader((TableSnapshotRegionSplit) split, job);
  }

  public static void setInput(JobConf conf, Scan scan, Collection<String> snapshotNames, Path restoreDir) throws IOException {
    MultiSnapshotInputFormatImpl.setInput(conf, scan, snapshotNames, restoreDir);
  }
}
