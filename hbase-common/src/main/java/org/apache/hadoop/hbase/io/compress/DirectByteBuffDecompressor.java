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

package org.apache.hadoop.hbase.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Wrapper around a DirectDecompressor to make it work as a ByteBuffDecompressor.
 * This is useful with decompressors that are defined in the Hadoop project. Decompressors
 * defined in HBase can directly implement ByteBuffDecompressor.
 *
 * Parameterized with the type of the DirectDecompressor in use so that the CodecPool differentiate
 * between DirectByteBuffDecompressors for different codecs at runtime.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DirectByteBuffDecompressor implements ByteBuffDecompressor {

  private final DirectDecompressor directDecompressor;

  public DirectByteBuffDecompressor(DirectDecompressor directDecompressor) {
    this.directDecompressor = directDecompressor;
  }

  /**
   * Fills the ouput buffer with uncompressed data.
   *
   * @return The actual number of bytes of uncompressed data.
   */
  @Override
  public int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    if (output instanceof SingleByteBuff && input instanceof SingleByteBuff) {
      ByteBuffer nioOutput = output.nioByteBuffers()[0];
      ByteBuffer nioInput = input.nioByteBuffers()[0];
      int oldOutputPos = nioOutput.position();
      directDecompressor.decompress(nioInput, nioOutput);
      return nioOutput.position() - oldOutputPos;
    }

    throw new IllegalStateException("At least one buffer is not direct or not a SinlgeByteBuff. "
      + "This is not supported");
  }

  @Override
  public boolean canDecompress(ByteBuff output, ByteBuff input) {
    if (output instanceof SingleByteBuff && input instanceof SingleByteBuff) {
      ByteBuffer nioOutput = output.nioByteBuffers()[0];
      ByteBuffer nioInput = input.nioByteBuffers()[0];
      return nioOutput.isDirect() && nioInput.isDirect();
    }

    return false;
  }

  public Class<? extends DirectDecompressor> getDirectDecompressorClass() {
    return directDecompressor.getClass();
  }

  @Override
  public void reset() {}

  @Override
  public void close() throws Exception { }
}
