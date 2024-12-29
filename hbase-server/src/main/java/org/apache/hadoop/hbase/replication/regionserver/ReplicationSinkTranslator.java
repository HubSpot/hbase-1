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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public interface ReplicationSinkTranslator {

  public TableName getSinkTableName(TableName tableName);

  public byte[] getSinkRowKey(TableName tableName, byte[] rowKey);

  public byte[] getSinkFamily(TableName tableName, byte[] family);

  public byte[] getSinkQualifier(TableName tableName, byte[] family, byte[] qualifier);

  public ExtendedCell getSinkExtendedCell(TableName tableName, ExtendedCell cell);
}
