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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder;

import java.util.Optional;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatisticsImpl;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CombinedRecorder implements RowStatisticsRecorder {

  private final RowStatisticsRecorder one;
  private final RowStatisticsRecorder two;

  public CombinedRecorder(RowStatisticsRecorder one, RowStatisticsRecorder two) {
    this.one = one;
    this.two = two;
  }

  @Override
  public void record(RowStatisticsImpl stats, boolean isMajor, Optional<byte[]> fullRegionName) {
    one.record(stats, isMajor, fullRegionName);
    two.record(stats, isMajor, fullRegionName);
  }

  public RowStatisticsRecorder getOne() {
    return one;
  }

  public RowStatisticsRecorder getTwo() {
    return two;
  }
}
