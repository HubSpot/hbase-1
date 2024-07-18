package org.apache.hadoop.hbase.backup;

import static org.mockito.Mockito.*;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.MultiSnapshotInputFormat;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.MultiSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatTestBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ VerySlowMapReduceTests.class, LargeTests.class })
public class TestMultiSnapshotInputFormat extends TableSnapshotInputFormatTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiSnapshotInputFormat.class);
  private static final byte[] AAA = Bytes.toBytes("aaa");
  private static final byte[] AFTER_ZZZ = Bytes.toBytes("zz{"); // 'z' + 1 => '{'
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String COLUMNS =
    Bytes.toString(FAMILIES[0]) + " " + Bytes.toString(FAMILIES[1]);
  private static  String BACKUP_ROOT_DIR;

  @Rule
  public TestName name = new TestName();

  static class TestMultiSnapshotMapper extends MapReduceBase
    implements TableMap<ImmutableBytesWritable, NullWritable> {
    @Override
    public void map(ImmutableBytesWritable key, Result value,
      OutputCollector<ImmutableBytesWritable, NullWritable> collector, Reporter reporter)
      throws IOException {
      verifyRowFromMap(key, value);
      collector.collect(key, NullWritable.get());
    }
  }

  public static class TestMultiSnapshotReducer extends MapReduceBase
    implements Reducer<ImmutableBytesWritable, NullWritable, NullWritable, NullWritable> {
    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(AAA, AFTER_ZZZ);

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<NullWritable> values,
      OutputCollector<NullWritable, NullWritable> collector, Reporter reporter) throws IOException {
      rowTracker.addRow(key.get());
    }

    @Override
    public void close() {
      rowTracker.validate();
    }
  }


  @BeforeClass
  public static void setUp() {
    BACKUP_ROOT_DIR = new Path(new Path(TEST_UTIL.getConfiguration().get("fs.defaultFS")), BACKUP_ROOT_DIR)
      .toString();
  }

  @Override
  protected void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
    int numRegions, int numSplitsPerRegion, int expectedNumSplits, boolean setLocalityEnabledTo)
    throws Exception {
    JobConf conf = new JobConf(util.getConfiguration());
    TableName tn = TableName.valueOf(name.getMethodName());
    SnapshotsAndRowsWritten srw = createTableAndSnapshots(util, tn, numRegions);

    // we need to create a new path where the resulting data will live, combine snapshots?
    // todo - make this better
    int hash = Objects.hashCode(srw.snapshots.toArray());
    String dirBase = new UUID(hash, hash).toString();
    Path tmpTableDir = util.getDataTestDirOnTestFS(dirBase);
    TableMapReduceUtil.initTableMultiSnapshotMapJob( new Scan(), srw.snapshots, COLUMNS,
      TestMultiSnapshotMapper.class, ImmutableBytesWritable.class, NullWritable.class, conf,
      false, tmpTableDir);


    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(AAA, AFTER_ZZZ);
    MultiSnapshotInputFormat tmsif = new MultiSnapshotInputFormat(new MultiSnapshotInputFormatImpl());
    InputSplit[] splits = tmsif.getSplits(conf, numSplitsPerRegion);

    for (InputSplit split : splits) {
      Reporter reporter = mock(Reporter.class);
      RecordReader<ImmutableBytesWritable, Result> rr = tmsif.getRecordReader(split, conf, reporter);

      ImmutableBytesWritable key = rr.createKey();
      Result value = rr.createValue();
      while (rr.next(key, value)) {
        verifyRowFromMap(key, value);
        rowTracker.addRow(key.copyBytes());
      }

      rr.close();
    }
    rowTracker.validate();
  }

  @Override
  protected void testWithMapReduceImpl(HBaseTestingUtility util, TableName tableName,
    String snapshotName, Path tableDir, int numRegions, int numSplitsPerRegion,
    int expectedNumSplits, boolean shutdownCluster) throws Exception {

  }

  @Override
  protected byte[] getStartRow() {
    return AAA;
  }

  @Override
  protected byte[] getEndRow() {
    return AFTER_ZZZ;
  }

  @Override
  public void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
    String snapshotName, Path tmpTableDir) throws Exception {

  }

  private static SnapshotsAndRowsWritten createTableAndSnapshots(HBaseTestingUtility util, TableName tn, int numRegions) throws Exception {
    try {
      util.deleteTable(tn);
    } catch (Exception ignore) {
      // ignore
    }

    if (numRegions > 1) {
      util.createTable(tn, FAMILIES, 1, AAA, AFTER_ZZZ, numRegions);
    } else {
      util.createTable(tn, FAMILIES);
    }

    Admin admin = util.getAdmin();

    Table table = util.getConnection().getTable(tn);
    util.loadTable(table, FAMILIES);

    BackupAdminImpl backupAdmin = new BackupAdminImpl(admin.getConnection());
    String fullBackup = backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tn));

    // todo update the table - does this work the way I want it to?
    util.loadTable(table, FAMILIES);
    String incrementalBackup = backupAdmin.backupTables(createBackupRequest(BackupType.INCREMENTAL, tn));

    int rows = 0;
    for (Result ignore: table.getScanner(new Scan())) {
      rows++;
    }

    return new SnapshotsAndRowsWritten(rows, Lists.newArrayList(fullBackup, incrementalBackup));
  }

  private static BackupRequest createBackupRequest(BackupType type, TableName tn) {
    BackupRequest.Builder builder = new BackupRequest.Builder();
    builder.withBackupType(type).withTableList(Lists.newArrayList(tn));
    return builder.build();
  }

  private static class SnapshotsAndRowsWritten {
    private final int rowsWritten;
    private final List<String> snapshots;

    private SnapshotsAndRowsWritten(int rowsWritten, List<String> snapshots) {
      this.rowsWritten = rowsWritten;
      this.snapshots = snapshots;
    }
  }
}
