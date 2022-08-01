// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface CreateNamespaceRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:scalardb.rpc.CreateNamespaceRequest)
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
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  int getOptionsCount();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  boolean containsOptions(
      java.lang.String key);
  /**
   * Use {@link #getOptionsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getOptions();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getOptionsMap();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */

  /* nullable */
java.lang.String getOptionsOrDefault(
      java.lang.String key,
      /* nullable */
java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */

  java.lang.String getOptionsOrThrow(
      java.lang.String key);

  /**
   * <code>bool if_not_exists = 3;</code>
   * @return The ifNotExists.
   */
  boolean getIfNotExists();
}
