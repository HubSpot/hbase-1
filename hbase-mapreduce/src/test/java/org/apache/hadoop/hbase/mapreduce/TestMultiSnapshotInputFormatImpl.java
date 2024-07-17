package org.apache.hadoop.hbase.mapreduce;

import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ SmallTests.class })
public class TestMultiSnapshotInputFormatImpl {

  private Scan scan;
  private Set<String> snapshots;

  private MultiTableSnapshotInputFormatImpl subject = Mockito.mock(MultiTableSnapshotInputFormatImpl.class);

  private final Configuration conf = new Configuration();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiTableSnapshotInputFormatImpl.class);

  @Before
  public void setup() throws Exception {
    CommonFSUtils.setRootDir(conf, new Path("file:///test-root-dir"));
  }
}
