package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertArrayEquals;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestPrefixColocationConditional {

  @Test
  public void itGetsStartStopPrefix() {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("test"))
      .setStartKey(Bytes.add(Bytes.toBytes((short) 1), Bytes.toBytes(1)))
      .setEndKey((Bytes.add(Bytes.toBytes((short) 2), Bytes.toBytes(0))))
      .setReplicaId(0)
      .build();
    assertArrayEquals(Bytes.toBytes((short) 1), PrefixColocationConditional.getPrefix(regionInfo, 2));
  }

}
