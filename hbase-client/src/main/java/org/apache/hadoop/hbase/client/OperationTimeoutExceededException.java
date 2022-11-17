package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when a batch operation exceeds the operation timeout
 */
@InterfaceAudience.Public
public class OperationTimeoutExceededException  extends DoNotRetryIOException {
  public OperationTimeoutExceededException() { super(); }

  public OperationTimeoutExceededException(String msg) {
    super(msg);
  }

  public OperationTimeoutExceededException(String msg, Throwable t) { super(msg, t); }
}
