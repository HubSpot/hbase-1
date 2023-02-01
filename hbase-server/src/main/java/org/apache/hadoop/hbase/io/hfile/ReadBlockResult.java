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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The result of a call to
 * {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader#readBlock(long, long, boolean, boolean, boolean, boolean, BlockType, DataBlockEncoding)}.
 * Holds the block fetched as well as whether it was fetched from cache.
 */
@InterfaceAudience.Private
public class ReadBlockResult {
  private final HFileBlock block;
  private final boolean fromCache;

  ReadBlockResult(HFileBlock block, boolean fromCache) {
    this.block = block;
    this.fromCache = fromCache;
  }

  /** Returns the block read */
  public HFileBlock getBlock() {
    return block;
  }

  /** Returns true if the block was read from cache (as opposed to disk) */
  public boolean isFromCache() {
    return fromCache;
  }
}
