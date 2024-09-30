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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RingBufferPayload {

  private final RowStatistics rowStatistics;
  private final boolean isMajor;
  private final byte[] fullRegionName;

  public RingBufferPayload(RowStatistics rowStatistics, boolean isMajor, byte[] fullRegionName) {
    this.rowStatistics = rowStatistics;
    this.isMajor = isMajor;
    this.fullRegionName = fullRegionName;
  }

  public RowStatistics getRowStatistics() {
    return rowStatistics;
  }

  public boolean getIsMajor() {
    return isMajor;
  }

  public byte[] getFullRegionName() {
    return fullRegionName;
  }
}
