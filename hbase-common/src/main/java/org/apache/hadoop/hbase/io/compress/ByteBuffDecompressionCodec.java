package org.apache.hadoop.hbase.io.compress;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ByteBuffDecompressionCodec extends CompressionCodec {

  Class<? extends ByteBuffDecompressor> getByteBuffDecompressorType();

  ByteBuffDecompressor createByteBuffDecompressor();

}
