package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class MultiSnapshotInputFormat extends TableSnapshotInputFormat {

  private final MultiSnapshotInputFormatImpl delegate;

  public MultiSnapshotInputFormat() {
    this.delegate = new MultiSnapshotInputFormatImpl();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job)
    throws IOException, InterruptedException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = delegate.getSplits(job.getConfiguration());
    List<InputSplit> rtn = Lists.newArrayListWithCapacity(splits.size());

    for (TableSnapshotInputFormatImpl.InputSplit split: splits) {
      rtn.add(new TableSnapshotInputFormat.TableSnapshotRegionSplit(split));
    }

    return rtn;
  }

  public static void setInput(JobConf conf, Scan scan, Collection<String> snapshotNames, Path restoreDir) throws IOException {
    MultiSnapshotInputFormatImpl.setInput(conf, scan, snapshotNames, restoreDir);
  }
}
