// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface DropNamespaceRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:rpc.DropNamespaceRequest)
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
   * <code>bool if_exists = 2;</code>
   * @return The ifExists.
   */
  boolean getIfExists();
}
