// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface RepairCoordinatorTablesRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:scalardb.rpc.RepairCoordinatorTablesRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>map&lt;string, string&gt; options = 1;</code>
   */
  int getOptionsCount();
  /**
   * <code>map&lt;string, string&gt; options = 1;</code>
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
   * <code>map&lt;string, string&gt; options = 1;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getOptionsMap();
  /**
   * <code>map&lt;string, string&gt; options = 1;</code>
   */

  /* nullable */
java.lang.String getOptionsOrDefault(
      java.lang.String key,
      /* nullable */
java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; options = 1;</code>
   */

  java.lang.String getOptionsOrThrow(
      java.lang.String key);
}
