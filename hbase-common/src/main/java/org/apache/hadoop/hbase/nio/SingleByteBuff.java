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
package org.apache.hadoop.hbase.nio;

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.NONE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.hbase.util.UnsafeAccess;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An implementation of ByteBuff where a single BB backs the BBI. This just acts as a wrapper over a
 * normal BB - offheap or onheap
 */
@InterfaceAudience.Private
public class SingleByteBuff extends ByteBuff {

  private static final boolean UNSAFE_AVAIL = HBasePlatformDependent.isUnsafeAvailable();
  private static final boolean UNSAFE_UNALIGNED = HBasePlatformDependent.unaligned();

  // Underlying BB
  private final ByteBuffer buf;

  // To access primitive values from underlying ByteBuffer using Unsafe
  private long unsafeOffset;
  private Object unsafeRef = null;

  public SingleByteBuff(ByteBuffer buf) {
    this(NONE, buf);
  }

  public SingleByteBuff(Recycler recycler, ByteBuffer buf) {
    this(new RefCnt(recycler), buf);
  }

  SingleByteBuff(RefCnt refCnt, ByteBuffer buf) {
    this.refCnt = refCnt;
    this.buf = buf;
    if (buf.hasArray()) {
      this.unsafeOffset = UnsafeAccess.BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset();
      this.unsafeRef = buf.array();
    } else {
      this.unsafeOffset = UnsafeAccess.directBufferAddress(buf);
    }
  }

  @Override
  public int position() {
    isAccessible();
    return this.buf.position();
  }

  @Override
  public SingleByteBuff position(int position) {
    isAccessible();
    this.buf.position(position);
    return this;
  }

  @Override
  public SingleByteBuff skip(int len) {
    isAccessible();
    this.buf.position(this.buf.position() + len);
    return this;
  }

  @Override
  public SingleByteBuff moveBack(int len) {
    isAccessible();
    this.buf.position(this.buf.position() - len);
    return this;
  }

  @Override
  public int capacity() {
    isAccessible();
    return this.buf.capacity();
  }

  @Override
  public int limit() {
    isAccessible();
    return this.buf.limit();
  }

  @Override
  public SingleByteBuff limit(int limit) {
    isAccessible();
    this.buf.limit(limit);
    return this;
  }

  @Override
  public SingleByteBuff rewind() {
    isAccessible();
    this.buf.rewind();
    return this;
  }

  @Override
  public SingleByteBuff mark() {
    isAccessible();
    this.buf.mark();
    return this;
  }

  @Override
  public ByteBuffer asSubByteBuffer(int length) {
    isAccessible();
    // Just return the single BB that is available
    return this.buf;
  }

  @Override
  public void asSubByteBuffer(int offset, int length, ObjectIntPair<ByteBuffer> pair) {
    isAccessible();
    // Just return the single BB that is available
    pair.setFirst(this.buf);
    pair.setSecond(offset);
  }

  @Override
  public int remaining() {
    isAccessible();
    return this.buf.remaining();
  }

  @Override
  public boolean hasRemaining() {
    isAccessible();
    return buf.hasRemaining();
  }

  @Override
  public SingleByteBuff reset() {
    isAccessible();
    this.buf.reset();
    return this;
  }

  @Override
  public SingleByteBuff slice() {
    isAccessible();
    return new SingleByteBuff(this.refCnt, this.buf.slice());
  }

  @Override
  public SingleByteBuff duplicate() {
    isAccessible();
    return new SingleByteBuff(this.refCnt, this.buf.duplicate());
  }

  @Override
  public byte get() {
    isAccessible();
    return buf.get();
  }

  @Override
  public byte get(int index) {
    isAccessible();
    if (UNSAFE_AVAIL) {
      return UnsafeAccess.toByte(this.unsafeRef, this.unsafeOffset + index);
    }
    return this.buf.get(index);
  }

  @Override
  public byte getByteAfterPosition(int offset) {
    isAccessible();
    return get(this.buf.position() + offset);
  }

  @Override
  public SingleByteBuff put(byte b) {
    isAccessible();
    this.buf.put(b);
    return this;
  }

  @Override
  public SingleByteBuff put(int index, byte b) {
    isAccessible();
    buf.put(index, b);
    return this;
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    isAccessible();
    ByteBufferUtils.copyFromBufferToArray(dst, buf, buf.position(), offset, length);
    buf.position(buf.position() + length);
  }

  @Override
  public void get(int sourceOffset, byte[] dst, int offset, int length) {
    isAccessible();
    ByteBufferUtils.copyFromBufferToArray(dst, buf, sourceOffset, offset, length);
  }

  @Override
  public void get(byte[] dst) {
    get(dst, 0, dst.length);
  }

  @Override
  public SingleByteBuff put(int offset, ByteBuff src, int srcOffset, int length) {
    isAccessible();
    if (src instanceof SingleByteBuff) {
      ByteBufferUtils.copyFromBufferToBuffer(((SingleByteBuff) src).buf, this.buf, srcOffset,
        offset, length);
    } else {
      // TODO we can do some optimization here? Call to asSubByteBuffer might
      // create a copy.
      ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<>();
      src.asSubByteBuffer(srcOffset, length, pair);
      if (pair.getFirst() != null) {
        ByteBufferUtils.copyFromBufferToBuffer(pair.getFirst(), this.buf, pair.getSecond(), offset,
          length);
      }
    }
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src, int offset, int length) {
    isAccessible();
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, src, offset, length);
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src) {
    isAccessible();
    return put(src, 0, src.length);
  }

  @Override
  public boolean hasArray() {
    isAccessible();
    return this.buf.hasArray();
  }

  @Override
  public byte[] array() {
    isAccessible();
    return this.buf.array();
  }

  @Override
  public int arrayOffset() {
    isAccessible();
    return this.buf.arrayOffset();
  }

  @Override
  public short getShort() {
    isAccessible();
    return this.buf.getShort();
  }

  @Override
  public short getShort(int index) {
    isAccessible();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toShort(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getShort(index);
  }

  @Override
  public short getShortAfterPosition(int offset) {
    isAccessible();
    return getShort(this.buf.position() + offset);
  }

  @Override
  public int getInt() {
    isAccessible();
    return this.buf.getInt();
  }

  @Override
  public SingleByteBuff putInt(int value) {
    isAccessible();
    ByteBufferUtils.putInt(this.buf, value);
    return this;
  }

  @Override
  public int getInt(int index) {
    isAccessible();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toInt(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getInt(index);
  }

  @Override
  public int getIntAfterPosition(int offset) {
    isAccessible();
    return getInt(this.buf.position() + offset);
  }

  @Override
  public long getLong() {
    isAccessible();
    return this.buf.getLong();
  }

  @Override
  public SingleByteBuff putLong(long value) {
    isAccessible();
    ByteBufferUtils.putLong(this.buf, value);
    return this;
  }

  @Override
  public long getLong(int index) {
    isAccessible();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toLong(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getLong(index);
  }

  @Override
  public long getLongAfterPosition(int offset) {
    isAccessible();
    return getLong(this.buf.position() + offset);
  }

  @Override
  public byte[] toBytes(int offset, int length) {
    isAccessible();
    byte[] output = new byte[length];
    ByteBufferUtils.copyFromBufferToArray(output, buf, offset, 0, length);
    return output;
  }

  @Override
  public void get(ByteBuffer out, int sourceOffset, int length) {
    isAccessible();
    ByteBufferUtils.copyFromBufferToBuffer(buf, out, sourceOffset, length);
  }

  @Override
  public int read(ReadableByteChannel channel) throws IOException {
    isAccessible();
    return read(channel, buf, 0, CHANNEL_READER);
  }

  @Override
  public int read(FileChannel channel, long offset) throws IOException {
    isAccessible();
    return read(channel, buf, offset, FILE_READER);
  }

  @Override
  public int write(FileChannel channel, long offset) throws IOException {
    isAccessible();
    int total = 0;
    while (buf.hasRemaining()) {
      int len = channel.write(buf, offset);
      total += len;
      offset += len;
    }
    return total;
  }

  @Override
  public ByteBuffer[] nioByteBuffers() {
    isAccessible();
    return new ByteBuffer[] { this.buf };
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SingleByteBuff)) {
      return false;
    }
    return this.buf.equals(((SingleByteBuff) obj).buf);
  }

  @Override
  public int hashCode() {
    return this.buf.hashCode();
  }

  @Override
  public SingleByteBuff retain() {
    refCnt.retain();
    return this;
  }
}
