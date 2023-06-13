package org.apache.hadoop.hbase.client;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import java.io.File;
import java.io.IOException;
import java.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

public class LocalTest {
  private static final String SCAN_WITH_FILTER_STRING = "EiMKHnNjYW4uYXR0cmlidXRlcy5tZXRyaWNzLmVuYWJsZRIB/xIVChBfaXNvbGF0aW9ubGV2ZWxfEgEBIioQ0QzUACQ5MzYwNjBjNi02MzNjLTQ3ZTEtYmQ1Yi0zMWYzNzFiZThkMDcqzgoKKW9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5GaWx0ZXJMaXN0EqAKCAISgAEKNm9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5TaW5nbGVDb2x1bW5WYWx1ZUZpbHRlchJGCgEwEgFhGAIiOAovb3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLkJpbmFyeUNvbXBhcmF0b3ISBQoDCgFlKAAwARKAAQo2b3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLlNpbmdsZUNvbHVtblZhbHVlRmlsdGVyEkYKATASAWEYAiI4Ci9vcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuQmluYXJ5Q29tcGFyYXRvchIFCgMKAXgoADABEoABCjZvcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuU2luZ2xlQ29sdW1uVmFsdWVGaWx0ZXISRgoBMBIBYRgCIjgKL29yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5CaW5hcnlDb21wYXJhdG9yEgUKAwoBcigAMAESgAEKNm9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5TaW5nbGVDb2x1bW5WYWx1ZUZpbHRlchJGCgEwEgFhGAIiOAovb3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLkJpbmFyeUNvbXBhcmF0b3ISBQoDCgFsKAAwARKAAQo2b3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLlNpbmdsZUNvbHVtblZhbHVlRmlsdGVyEkYKATASAWEYAiI4Ci9vcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuQmluYXJ5Q29tcGFyYXRvchIFCgMKAW0oADABEoABCjZvcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuU2luZ2xlQ29sdW1uVmFsdWVGaWx0ZXISRgoBMBIBYRgCIjgKL29yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5CaW5hcnlDb21wYXJhdG9yEgUKAwoBRSgAMAESgAEKNm9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5TaW5nbGVDb2x1bW5WYWx1ZUZpbHRlchJGCgEwEgFhGAIiOAovb3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLkJpbmFyeUNvbXBhcmF0b3ISBQoDCgF1KAAwARKAAQo2b3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLlNpbmdsZUNvbHVtblZhbHVlRmlsdGVyEkYKATASAWEYAiI4Ci9vcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuQmluYXJ5Q29tcGFyYXRvchIFCgMKAW8oADABEoABCjZvcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuU2luZ2xlQ29sdW1uVmFsdWVGaWx0ZXISRgoBMBIBYRgCIjgKL29yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5CaW5hcnlDb21wYXJhdG9yEgUKAwoBbigAMAESgAEKNm9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5TaW5nbGVDb2x1bW5WYWx1ZUZpbHRlchJGCgEwEgFhGAIiOAovb3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLkJpbmFyeUNvbXBhcmF0b3ISBQoDCgFIKAAwATIMCAAQ//////////9/OAFAAFCAgIABiAHQD7ABAA==";
  private static final File ROOT_DIRECTORY = new File(System.getenv("HOME"), "snapshot-data");
  private static final String REGION_INFO = "UEJVRgiz77/p9jASKgoHZGVmYXVsdBIfY29udmVyc2F0aW9ucy1tZXNzYWdlLXN0b3JhZ2UtMxoAIioQ0QzUACQ5MzYwNjBjNi02MzNjLTQ3ZTEtYmQ1Yi0zMWYzNzFiZThkMDcoADAAOAA=";
  private static final String TABLE_DESCRIPTOR = "UEJVRgoqCgdkZWZhdWx0Eh9jb252ZXJzYXRpb25zLW1lc3NhZ2Utc3RvcmFnZS0zEhAKB0lTX01FVEESBWZhbHNlEhsKDE1BWF9GSUxFU0laRRILMjE0NzQ4MzY0ODASHgoSTUVNU1RPUkVfRkxVU0hTSVpFEgg2NzEwODg2NBIoCiBOT1JNQUxJWkVSX1RBUkdFVF9SRUdJT05fU0laRV9NQhIEMTAyNBIoCh1oYmFzZS5zdG9yZS5maWxlLXRyYWNrZXIuaW1wbBIHREVGQVVMVBqOAgoBMBIcChRJTkRFWF9CTE9DS19FTkNPRElORxIETk9ORRINCghWRVJTSU9OUxIBMRIbChJLRUVQX0RFTEVURURfQ0VMTFMSBUZBTFNFEh0KE0RBVEFfQkxPQ0tfRU5DT0RJTkcSBlBSRUZJWBIRCgNUVEwSCjIxNDc0ODM2NDcSEQoMTUlOX1ZFUlNJT05TEgEwEhYKEVJFUExJQ0FUSU9OX1NDT1BFEgEwEhIKC0JMT09NRklMVEVSEgNST1cSEgoJSU5fTUVNT1JZEgVmYWxzZRISCgtDT01QUkVTU0lPThIDTFpPEhIKCkJMT0NLQ0FDSEUSBHRydWUSEgoJQkxPQ0tTSVpFEgU2NTUzNg==";

  public static Scan convertStringToScan(String base64) throws IOException {
    byte[] decoded = Base64.getDecoder().decode(base64);
    return ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(decoded));
  }

  private static final byte[] CF = {'0'};
  private static final byte[] QF = {'a'};

  public static void main(String[] args) throws IOException, DeserializationException {
    Scan scan = convertStringToScan(SCAN_WITH_FILTER_STRING);
    Configuration conf = new Configuration();

    scan.setFilter(null);
    scan.setFilter(new RandomRowFilter(0.5f));

    try (ClientSideRegionScanner regionScanner = new ClientSideRegionScanner(
      conf,
      FileSystem.getLocal(conf),
      new Path(ROOT_DIRECTORY.listFiles()[0].getAbsolutePath()),
      HTableDescriptor.parseFrom(Base64.getDecoder().decode(TABLE_DESCRIPTOR)),
      HRegionInfo.parseFrom(Base64.getDecoder().decode(REGION_INFO)),
      scan,
      null
    )) {
      Multiset<Character> typeCharacter = HashMultiset.create();

      for (Result result : regionScanner) {
        Cell cell = result.getColumnLatestCell(CF, QF);
        if (cell == null) {
          typeCharacter.add(' ');
        } else {
          typeCharacter.add((char) cell.getValueArray()[0]);
        }
      }
      System.out.println(typeCharacter);
    }
  }
}
