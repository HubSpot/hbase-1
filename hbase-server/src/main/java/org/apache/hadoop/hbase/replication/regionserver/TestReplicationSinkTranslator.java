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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
public class TestReplicationSinkTranslator implements ReplicationSinkTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationSinkTranslator.class);

  @Override public TableName getSinkTableName(TableName tableName) {
    String namespace = tableName.getNameWithNamespaceInclAsString();
    String qualifier = tableName.getQualifierAsString();
    if (namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
      namespace = "eboland";
    }
    TableName ret = TableName.valueOf(namespace, qualifier);
    LOG.debug("Returning translated tableName {}", ret);
    return ret;
  }

  @Override public byte[] getSinkRowKey(TableName tableName, byte[] rowKey) {
    LOG.debug("Returning identity rowKey {}", rowKey);
    return rowKey;
  }

  @Override public byte[] getSinkFamily(TableName tableName, byte[] family) {
    LOG.debug("Returning identity family {}", family);
    return family;
  }

  @Override public byte[] getSinkQualifier(TableName tableName, byte[] family, byte[] qualifier) {
    LOG.debug("Returning identity qualifier {}", qualifier);
    return qualifier;
  }

  @Override public Cell getSinkCell(TableName tableName, Cell cell) {
    LOG.debug("Returning identity cell {}", cell);
    return cell;
  }
}
