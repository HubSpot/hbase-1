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
package org.apache.hadoop.hbase.io.encoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Encapsulates a data block compressed using a particular encoding algorithm. Useful for testing
 * and benchmarking. This is used only in testing.
 */
@InterfaceAudience.Private
public class EncodedDataBlock {
  private byte[] rawKVs;
  private ByteBuffer rawBuffer;
  private DataBlockEncoder dataBlockEncoder;

  private byte[] cachedEncodedData;

  private final HFileBlockEncodingContext encodingCtx;
  private HFileContext meta;

  private final DataBlockEncoding encoding;
  private final Configuration conf;

  // The is for one situation that there are some cells includes tags and others are not.
  // isTagsLenZero stores if cell tags length is zero before doing encoding since we need
  // to check cell tags length is zero or not after decoding.
  // Encoders ROW_INDEX_V1 would abandon tags segment if tags is 0 after decode cells to
  // byte array, other encoders won't do that. So we have to find a way to add tagsLen zero
  // in the decoded byte array.
  private List<Boolean> isTagsLenZero = new ArrayList<>();

  /**
   * Create a buffer which will be encoded using dataBlockEncoder.
   * @param conf             store configuration
   * @param dataBlockEncoder Algorithm used for compression.
   * @param encoding         encoding type used
   * @param rawKVs           raw KVs
   * @param meta             hfile context
   */
  public EncodedDataBlock(Configuration conf, DataBlockEncoder dataBlockEncoder,
    DataBlockEncoding encoding, byte[] rawKVs, HFileContext meta) {
    Preconditions.checkNotNull(encoding, "Cannot create encoded data block with null encoder");
    this.dataBlockEncoder = dataBlockEncoder;
    this.encoding = encoding;
    encodingCtx = dataBlockEncoder.newDataBlockEncodingContext(conf, encoding,
      HConstants.HFILEBLOCK_DUMMY_HEADER, meta);
    this.rawKVs = rawKVs;
    this.meta = meta;
    this.conf = conf;
  }

  /**
   * Provides access to compressed value.
   * @param headerSize header size of the block.
   * @return Forwards sequential iterator.
   */
  public Iterator<Cell> getIterator(int headerSize) {
    final int rawSize = rawKVs.length;
    byte[] encodedDataWithHeader = getEncodedData();
    int bytesToSkip = headerSize + Bytes.SIZEOF_SHORT;
    ByteArrayInputStream bais = new ByteArrayInputStream(encodedDataWithHeader, bytesToSkip,
      encodedDataWithHeader.length - bytesToSkip);
    final DataInputStream dis = new DataInputStream(bais);

    return new Iterator<Cell>() {
      private ByteBuffer decompressedData = null;
      private Iterator<Boolean> it = isTagsLenZero.iterator();

      @Override
      public boolean hasNext() {
        if (decompressedData == null) {
          return rawSize > 0;
        }
        return decompressedData.hasRemaining();
      }

      @Override
      public Cell next() {
        if (decompressedData == null) {
          try {
            decompressedData = dataBlockEncoder.decodeKeyValues(dis,
              dataBlockEncoder.newDataBlockDecodingContext(conf, meta));
          } catch (IOException e) {
            throw new RuntimeException("Problem with data block encoder, "
              + "most likely it requested more bytes than are available.", e);
          }
          decompressedData.rewind();
        }
        int offset = decompressedData.position();
        int klen = decompressedData.getInt();
        int vlen = decompressedData.getInt();
        int tagsLen = 0;
        ByteBufferUtils.skip(decompressedData, klen + vlen);
        // Read the tag length in case when stream contain tags
        if (meta.isIncludesTags()) {
          boolean noTags = true;
          if (it.hasNext()) {
            noTags = it.next();
          }
          // ROW_INDEX_V1 will not put tagsLen back in cell if it is zero, there is no need
          // to read short here.
          if (!(encoding.equals(DataBlockEncoding.ROW_INDEX_V1) && noTags)) {
            tagsLen = ((decompressedData.get() & 0xff) << 8) ^ (decompressedData.get() & 0xff);
            ByteBufferUtils.skip(decompressedData, tagsLen);
          }
        }
        KeyValue kv =
          new KeyValue(decompressedData.array(), decompressedData.arrayOffset() + offset,
            (int) KeyValue.getKeyValueDataStructureSize(klen, vlen, tagsLen));
        if (meta.isIncludesMvcc()) {
          long mvccVersion = ByteBufferUtils.readVLong(decompressedData);
          kv.setSequenceId(mvccVersion);
        }
        return kv;
      }

      @Override
      public void remove() {
        throw new NotImplementedException("remove() is not supported!");
      }

      @Override
      public String toString() {
        return "Iterator of: " + dataBlockEncoder.getClass().getName();
      }

    };
  }

  /**
   * Find the size of minimal buffer that could store compressed data.
   * @return Size in bytes of compressed data.
   */
  public int getSize() {
    return getEncodedData().length;
  }

  /**
   * Find the size of compressed data assuming that buffer will be compressed using given algorithm.
   * @param algo        compression algorithm
   * @param compressor  compressor already requested from codec
   * @param inputBuffer Array to be compressed.
   * @param offset      Offset to beginning of the data.
   * @param length      Length to be compressed.
   * @return Size of compressed data in bytes.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH_EXCEPTION",
      justification = "No sure what findbugs wants but looks to me like no NPE")
  public static int getCompressedSize(Algorithm algo, Compressor compressor, byte[] inputBuffer,
    int offset, int length) throws IOException {

    // Create streams
    // Storing them so we can close them
    final IOUtils.NullOutputStream nullOutputStream = new IOUtils.NullOutputStream();
    final DataOutputStream compressedStream = new DataOutputStream(nullOutputStream);
    OutputStream compressingStream = null;

    try {
      if (compressor != null) {
        compressor.reset();
      }

      compressingStream = algo.createCompressionStream(compressedStream, compressor, 0);

      compressingStream.write(inputBuffer, offset, length);
      compressingStream.flush();

      return compressedStream.size();
    } finally {
      nullOutputStream.close();
      compressedStream.close();
      compressingStream.close();
    }
  }

  /**
   * Estimate size after second stage of compression (e.g. LZO).
   * @param comprAlgo  compression algorithm to be used for compression
   * @param compressor compressor corresponding to the given compression algorithm
   * @return Size after second stage of compression.
   */
  public int getEncodedCompressedSize(Algorithm comprAlgo, Compressor compressor)
    throws IOException {
    byte[] compressedBytes = getEncodedData();
    return getCompressedSize(comprAlgo, compressor, compressedBytes, 0, compressedBytes.length);
  }

  /** Returns encoded data with header */
  private byte[] getEncodedData() {
    if (cachedEncodedData != null) {
      return cachedEncodedData;
    }
    cachedEncodedData = encodeData();
    return cachedEncodedData;
  }

  private ByteBuffer getUncompressedBuffer() {
    if (rawBuffer == null || rawBuffer.limit() < rawKVs.length) {
      rawBuffer = ByteBuffer.wrap(rawKVs);
    }
    return rawBuffer;
  }

  /**
   * Do the encoding, but do not cache the encoded data.
   * @return encoded data block with header and checksum
   */
  public byte[] encodeData() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] baosBytes = null;
    try {
      baos.write(HConstants.HFILEBLOCK_DUMMY_HEADER);
      DataOutputStream out = new DataOutputStream(baos);
      this.dataBlockEncoder.startBlockEncoding(encodingCtx, out);
      ByteBuffer in = getUncompressedBuffer();
      in.rewind();
      int klength, vlength;
      int tagsLength = 0;
      long memstoreTS = 0L;
      KeyValue kv = null;
      while (in.hasRemaining()) {
        int kvOffset = in.position();
        klength = in.getInt();
        vlength = in.getInt();
        ByteBufferUtils.skip(in, klength + vlength);
        if (this.meta.isIncludesTags()) {
          tagsLength = ((in.get() & 0xff) << 8) ^ (in.get() & 0xff);
          ByteBufferUtils.skip(in, tagsLength);
          this.isTagsLenZero.add(tagsLength == 0);
        }
        if (this.meta.isIncludesMvcc()) {
          memstoreTS = ByteBufferUtils.readVLong(in);
        }
        kv = new KeyValue(in.array(), in.arrayOffset() + kvOffset,
          (int) KeyValue.getKeyValueDataStructureSize(klength, vlength, tagsLength));
        kv.setSequenceId(memstoreTS);
        this.dataBlockEncoder.encode(kv, encodingCtx, out);
      }
      // Below depends on BAOS internal behavior. toByteArray makes a copy of bytes so far.
      baos.flush();
      baosBytes = baos.toByteArray();
      this.dataBlockEncoder.endBlockEncoding(encodingCtx, out, baosBytes);
      // In endBlockEncoding(encodingCtx, out, baosBytes), Encoder ROW_INDEX_V1 write integer in
      // out while the others write integer in baosBytes(byte array). We need to add
      // baos.toByteArray() after endBlockEncoding again to make sure the integer writes in
      // outputstream with Encoder ROW_INDEX_V1 dump to byte array (baosBytes).
      // The if branch is necessary because Encoders excepts ROW_INDEX_V1 write integer in
      // baosBytes directly, without if branch and do toByteArray() again, baosBytes won't
      // contains the integer wrotten in endBlockEncoding.
      if (this.encoding.equals(DataBlockEncoding.ROW_INDEX_V1)) {
        baosBytes = baos.toByteArray();
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Bug in encoding part of algorithm %s. "
        + "Probably it requested more bytes than are available.", toString()), e);
    }
    return baosBytes;
  }

  @Override
  public String toString() {
    return encoding.name();
  }
}
