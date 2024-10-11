// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

/**
 * Protobuf enum {@code rpc.Order}
 */
public enum Order
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>ORDER_ASC = 0;</code>
   */
  ORDER_ASC(0),
  /**
   * <code>ORDER_DESC = 1;</code>
   */
  ORDER_DESC(1),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>ORDER_ASC = 0;</code>
   */
  public static final int ORDER_ASC_VALUE = 0;
  /**
   * <code>ORDER_DESC = 1;</code>
   */
  public static final int ORDER_DESC_VALUE = 1;


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
  public static Order valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static Order forNumber(int value) {
    switch (value) {
      case 0: return ORDER_ASC;
      case 1: return ORDER_DESC;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<Order>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      Order> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<Order>() {
          public Order findValueByNumber(int number) {
            return Order.forNumber(number);
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
    return com.scalar.db.rpc.ScalarDbProto.getDescriptor().getEnumTypes().get(1);
  }

  private static final Order[] VALUES = values();

  public static Order valueOf(
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

  private Order(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:rpc.Order)
}

