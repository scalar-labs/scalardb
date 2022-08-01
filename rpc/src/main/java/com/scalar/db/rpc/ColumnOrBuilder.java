// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface ColumnOrBuilder extends
    // @@protoc_insertion_point(interface_extends:scalardb.rpc.Column)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string name = 1;</code>
   * @return The name.
   */
  java.lang.String getName();
  /**
   * <code>string name = 1;</code>
   * @return The bytes for name.
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>bool boolean_value = 2;</code>
   * @return Whether the booleanValue field is set.
   */
  boolean hasBooleanValue();
  /**
   * <code>bool boolean_value = 2;</code>
   * @return The booleanValue.
   */
  boolean getBooleanValue();

  /**
   * <code>int32 int_value = 3;</code>
   * @return Whether the intValue field is set.
   */
  boolean hasIntValue();
  /**
   * <code>int32 int_value = 3;</code>
   * @return The intValue.
   */
  int getIntValue();

  /**
   * <code>int64 bigint_value = 4;</code>
   * @return Whether the bigintValue field is set.
   */
  boolean hasBigintValue();
  /**
   * <code>int64 bigint_value = 4;</code>
   * @return The bigintValue.
   */
  long getBigintValue();

  /**
   * <code>float float_value = 5;</code>
   * @return Whether the floatValue field is set.
   */
  boolean hasFloatValue();
  /**
   * <code>float float_value = 5;</code>
   * @return The floatValue.
   */
  float getFloatValue();

  /**
   * <code>double double_value = 6;</code>
   * @return Whether the doubleValue field is set.
   */
  boolean hasDoubleValue();
  /**
   * <code>double double_value = 6;</code>
   * @return The doubleValue.
   */
  double getDoubleValue();

  /**
   * <code>string text_value = 7;</code>
   * @return Whether the textValue field is set.
   */
  boolean hasTextValue();
  /**
   * <code>string text_value = 7;</code>
   * @return The textValue.
   */
  java.lang.String getTextValue();
  /**
   * <code>string text_value = 7;</code>
   * @return The bytes for textValue.
   */
  com.google.protobuf.ByteString
      getTextValueBytes();

  /**
   * <code>bytes blob_value = 8;</code>
   * @return Whether the blobValue field is set.
   */
  boolean hasBlobValue();
  /**
   * <code>bytes blob_value = 8;</code>
   * @return The blobValue.
   */
  com.google.protobuf.ByteString getBlobValue();

  public com.scalar.db.rpc.Column.ValueCase getValueCase();
}
