// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

package com.scalar.db.rpc;

public interface ScanNextResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:rpc.ScanNextResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .rpc.Result result = 1;</code>
   */
  java.util.List<com.scalar.db.rpc.Result> 
      getResultList();
  /**
   * <code>repeated .rpc.Result result = 1;</code>
   */
  com.scalar.db.rpc.Result getResult(int index);
  /**
   * <code>repeated .rpc.Result result = 1;</code>
   */
  int getResultCount();
  /**
   * <code>repeated .rpc.Result result = 1;</code>
   */
  java.util.List<? extends com.scalar.db.rpc.ResultOrBuilder> 
      getResultOrBuilderList();
  /**
   * <code>repeated .rpc.Result result = 1;</code>
   */
  com.scalar.db.rpc.ResultOrBuilder getResultOrBuilder(
      int index);

  /**
   * <code>bool has_more_results = 2;</code>
   * @return The hasMoreResults.
   */
  boolean getHasMoreResults();
}
