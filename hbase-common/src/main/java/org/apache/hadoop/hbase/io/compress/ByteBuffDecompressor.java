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
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Specification of a block-based 'de-compressor', which can be more efficient than the
 * stream-based Decompressor.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ByteBuffDecompressor extends AutoCloseable {

  /**
   * Fills the ouput buffer with uncompressed data.
   *
   * @return The actual number of bytes of uncompressed data.
   */
  int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException;

  boolean canDecompress(ByteBuff output, ByteBuff input);

  void reset();

}
