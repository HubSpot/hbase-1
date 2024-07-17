package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate({ "HBase" })
@InterfaceStability.Evolving
public class MultiSnapshotInputFormatImpl {
  public static final String SNAPSHOT_TO_SCANS_KEY =
    "hbase.MultiSnapshotInputFormat.snapshotsToScans";
  public static final String SCAN_KEY =
    "hbase.MultiSnapshotInputFormat.scan";
  public static final String RESTORE_DIR_KEY =
    "hbase.MultiSnapshotInputFormat.restore.restoreDir";

  public static void setInput(Configuration conf, Scan scan, Collection<String> snapshots, Path restoreDir) throws
    IOException {
    conf.setStrings(SNAPSHOT_TO_SCANS_KEY, snapshots.toArray(snapshots.toArray(new String[0])));
    conf.set(SCAN_KEY, TableMapReduceUtil.convertScanToString(scan));
    conf.set(RESTORE_DIR_KEY, restoreDir.toString());

    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    for (String snapshot: snapshots) {
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshot);
    }
  }

  public List<TableSnapshotInputFormatImpl.InputSplit> getSplits(Configuration conf) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = Lists.newArrayList();

    Collection<String> snapshots = conf.getStringCollection(SNAPSHOT_TO_SCANS_KEY);
    Scan scan = TableMapReduceUtil.convertStringToScan(conf.get(SCAN_KEY));
    Path restoreDir = new Path(conf.get(RESTORE_DIR_KEY));
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    for (String snapshot: snapshots) {
      SnapshotManifest manifest = TableSnapshotInputFormatImpl.getSnapshotManifest(conf, snapshot, rootDir, fs);
      List<HRegionInfo> regions = TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest);
      splits.addAll(TableSnapshotInputFormatImpl.getSplits(scan, manifest, regions, restoreDir, conf));
    }

    return splits;
  }
}
