/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift2.generated;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2022-07-05")
public enum TLogType implements org.apache.thrift.TEnum {
  SLOW_LOG(1),
  LARGE_LOG(2);

  private final int value;

  private TLogType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static TLogType findByValue(int value) { 
    switch (value) {
      case 1:
        return SLOW_LOG;
      case 2:
        return LARGE_LOG;
      default:
        return null;
    }
  }
}
