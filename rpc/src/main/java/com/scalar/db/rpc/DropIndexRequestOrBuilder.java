// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

public interface DropIndexRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:rpc.DropIndexRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string namespace = 1;</code>
   * @return The namespace.
   */
  java.lang.String getNamespace();
  /**
   * <code>string namespace = 1;</code>
   * @return The bytes for namespace.
   */
  com.google.protobuf.ByteString
      getNamespaceBytes();

  /**
   * <code>string table = 2;</code>
   * @return The table.
   */
  java.lang.String getTable();
  /**
   * <code>string table = 2;</code>
   * @return The bytes for table.
   */
  com.google.protobuf.ByteString
      getTableBytes();

  /**
   * <code>string column_name = 3;</code>
   * @return The columnName.
   */
  java.lang.String getColumnName();
  /**
   * <code>string column_name = 3;</code>
   * @return The bytes for columnName.
   */
  com.google.protobuf.ByteString
      getColumnNameBytes();

  /**
   * <code>bool if_exists = 4;</code>
   * @return The ifExists.
   */
  boolean getIfExists();
}
