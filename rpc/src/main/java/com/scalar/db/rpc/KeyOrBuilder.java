// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface KeyOrBuilder extends
    // @@protoc_insertion_point(interface_extends:scalardb.rpc.Key)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .scalardb.rpc.Value value = 1 [deprecated = true];</code>
   */
  @java.lang.Deprecated java.util.List<com.scalar.db.rpc.Value> 
      getValueList();
  /**
   * <code>repeated .scalardb.rpc.Value value = 1 [deprecated = true];</code>
   */
  @java.lang.Deprecated com.scalar.db.rpc.Value getValue(int index);
  /**
   * <code>repeated .scalardb.rpc.Value value = 1 [deprecated = true];</code>
   */
  @java.lang.Deprecated int getValueCount();
  /**
   * <code>repeated .scalardb.rpc.Value value = 1 [deprecated = true];</code>
   */
  @java.lang.Deprecated java.util.List<? extends com.scalar.db.rpc.ValueOrBuilder> 
      getValueOrBuilderList();
  /**
   * <code>repeated .scalardb.rpc.Value value = 1 [deprecated = true];</code>
   */
  @java.lang.Deprecated com.scalar.db.rpc.ValueOrBuilder getValueOrBuilder(
      int index);

  /**
   * <code>repeated .scalardb.rpc.Column columns = 2;</code>
   */
  java.util.List<com.scalar.db.rpc.Column> 
      getColumnsList();
  /**
   * <code>repeated .scalardb.rpc.Column columns = 2;</code>
   */
  com.scalar.db.rpc.Column getColumns(int index);
  /**
   * <code>repeated .scalardb.rpc.Column columns = 2;</code>
   */
  int getColumnsCount();
  /**
   * <code>repeated .scalardb.rpc.Column columns = 2;</code>
   */
  java.util.List<? extends com.scalar.db.rpc.ColumnOrBuilder> 
      getColumnsOrBuilderList();
  /**
   * <code>repeated .scalardb.rpc.Column columns = 2;</code>
   */
  com.scalar.db.rpc.ColumnOrBuilder getColumnsOrBuilder(
      int index);
}
