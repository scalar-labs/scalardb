// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

/**
 * Protobuf enum {@code scalardb.rpc.DataType}
 */
public enum DataType
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>DATA_TYPE_BOOLEAN = 0;</code>
   */
  DATA_TYPE_BOOLEAN(0),
  /**
   * <code>DATA_TYPE_INT = 1;</code>
   */
  DATA_TYPE_INT(1),
  /**
   * <code>DATA_TYPE_BIGINT = 2;</code>
   */
  DATA_TYPE_BIGINT(2),
  /**
   * <code>DATA_TYPE_FLOAT = 3;</code>
   */
  DATA_TYPE_FLOAT(3),
  /**
   * <code>DATA_TYPE_DOUBLE = 4;</code>
   */
  DATA_TYPE_DOUBLE(4),
  /**
   * <code>DATA_TYPE_TEXT = 5;</code>
   */
  DATA_TYPE_TEXT(5),
  /**
   * <code>DATA_TYPE_BLOB = 6;</code>
   */
  DATA_TYPE_BLOB(6),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>DATA_TYPE_BOOLEAN = 0;</code>
   */
  public static final int DATA_TYPE_BOOLEAN_VALUE = 0;
  /**
   * <code>DATA_TYPE_INT = 1;</code>
   */
  public static final int DATA_TYPE_INT_VALUE = 1;
  /**
   * <code>DATA_TYPE_BIGINT = 2;</code>
   */
  public static final int DATA_TYPE_BIGINT_VALUE = 2;
  /**
   * <code>DATA_TYPE_FLOAT = 3;</code>
   */
  public static final int DATA_TYPE_FLOAT_VALUE = 3;
  /**
   * <code>DATA_TYPE_DOUBLE = 4;</code>
   */
  public static final int DATA_TYPE_DOUBLE_VALUE = 4;
  /**
   * <code>DATA_TYPE_TEXT = 5;</code>
   */
  public static final int DATA_TYPE_TEXT_VALUE = 5;
  /**
   * <code>DATA_TYPE_BLOB = 6;</code>
   */
  public static final int DATA_TYPE_BLOB_VALUE = 6;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static DataType valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static DataType forNumber(int value) {
    switch (value) {
      case 0: return DATA_TYPE_BOOLEAN;
      case 1: return DATA_TYPE_INT;
      case 2: return DATA_TYPE_BIGINT;
      case 3: return DATA_TYPE_FLOAT;
      case 4: return DATA_TYPE_DOUBLE;
      case 5: return DATA_TYPE_TEXT;
      case 6: return DATA_TYPE_BLOB;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<DataType>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      DataType> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<DataType>() {
          public DataType findValueByNumber(int number) {
            return DataType.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalStateException(
          "Can't get the descriptor of an unrecognized enum value.");
    }
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return com.scalar.db.rpc.ScalarDbProto.getDescriptor().getEnumTypes().get(2);
  }

  private static final DataType[] VALUES = values();

  public static DataType valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private DataType(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:scalardb.rpc.DataType)
}

