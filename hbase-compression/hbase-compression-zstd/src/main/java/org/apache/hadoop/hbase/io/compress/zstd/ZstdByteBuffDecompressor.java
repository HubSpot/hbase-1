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
package org.apache.hadoop.hbase.io.compress.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictDecompress;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.BlockDecompressorHelper;
import org.apache.hadoop.hbase.io.compress.ByteBuffDecompressor;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Glue for ByteBuffDecompressor on top of zstd-jni. Note that this doesn't implement CanReinit
 * because it's faster to avoid reinitializing on each usage.
 */
@InterfaceAudience.Private
public class ZstdByteBuffDecompressor implements ByteBuffDecompressor {

  protected ZstdDictDecompress dict;
  protected ZstdDecompressCtx ctx;

  ZstdByteBuffDecompressor(@Nullable byte[] dictionary) {
    ctx = new ZstdDecompressCtx();
    if (dictionary != null) {
      this.dict = new ZstdDictDecompress(dictionary);
      this.ctx.loadDict(this.dict);
    }
  }

  @Override
  public boolean canDecompress(ByteBuff output, ByteBuff input) {
    if (output instanceof SingleByteBuff && input instanceof SingleByteBuff) {
      ByteBuffer nioOutput = output.nioByteBuffers()[0];
      ByteBuffer nioInput = input.nioByteBuffers()[0];
      if (nioOutput.isDirect() && nioInput.isDirect()) {
        return true;
      } else if (!nioOutput.isDirect() && !nioInput.isDirect()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    return BlockDecompressorHelper.decompress(output, input, inputLen, this::decompressRaw);
  }

  private int decompressRaw(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    if (output instanceof SingleByteBuff && input instanceof SingleByteBuff) {
      ByteBuffer nioOutput = output.nioByteBuffers()[0];
      ByteBuffer nioInput = input.nioByteBuffers()[0];
      if (nioOutput.isDirect() && nioInput.isDirect()) {
        return decompressDirectByteBuffers(nioOutput, nioInput, inputLen);
      } else if (!nioOutput.isDirect() && !nioInput.isDirect()) {
        return decompressHeapByteBuffers(nioOutput, nioInput, inputLen);
      }
    }

    throw new IllegalStateException("One buffer is direct and the other is not, "
      + "or one or more not SingleByteBuffs. This is not supported");
  }

  private int decompressDirectByteBuffers(ByteBuffer output, ByteBuffer input, int inputLen) {
    int origOutputPos = output.position();

    int n = ctx.decompressDirectByteBuffer(output, output.position(),
      output.limit() - output.position(), input, input.position(), inputLen);

    output.position(origOutputPos + n);
    output.limit(output.position());
    return n;
  }

  private int decompressHeapByteBuffers(ByteBuffer output, ByteBuffer input, int inputLen) {
    int origOutputPos = output.position();

    int n = ctx.decompressByteArray(output.array(), output.arrayOffset() + output.position(),
      output.limit() - output.position(), input.array(), input.arrayOffset() + input.position(),
      inputLen);

    output.position(origOutputPos + n);
    output.limit(output.position());
    return n;
  }

  @Override
  public void close() {
    ctx.close();
    dict.close();
  }
}
