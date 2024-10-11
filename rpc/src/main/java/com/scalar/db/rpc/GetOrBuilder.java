// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

public interface GetOrBuilder extends
    // @@protoc_insertion_point(interface_extends:rpc.Get)
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
   * <code>.rpc.Key partition_key = 3;</code>
   * @return Whether the partitionKey field is set.
   */
  boolean hasPartitionKey();
  /**
   * <code>.rpc.Key partition_key = 3;</code>
   * @return The partitionKey.
   */
  com.scalar.db.rpc.Key getPartitionKey();
  /**
   * <code>.rpc.Key partition_key = 3;</code>
   */
  com.scalar.db.rpc.KeyOrBuilder getPartitionKeyOrBuilder();

  /**
   * <code>.rpc.Key clustering_key = 4;</code>
   * @return Whether the clusteringKey field is set.
   */
  boolean hasClusteringKey();
  /**
   * <code>.rpc.Key clustering_key = 4;</code>
   * @return The clusteringKey.
   */
  com.scalar.db.rpc.Key getClusteringKey();
  /**
   * <code>.rpc.Key clustering_key = 4;</code>
   */
  com.scalar.db.rpc.KeyOrBuilder getClusteringKeyOrBuilder();

  /**
   * <code>.rpc.Consistency consistency = 5;</code>
   * @return The enum numeric value on the wire for consistency.
   */
  int getConsistencyValue();
  /**
   * <code>.rpc.Consistency consistency = 5;</code>
   * @return The consistency.
   */
  com.scalar.db.rpc.Consistency getConsistency();

  /**
   * <code>repeated string projections = 6;</code>
   * @return A list containing the projections.
   */
  java.util.List<java.lang.String>
      getProjectionsList();
  /**
   * <code>repeated string projections = 6;</code>
   * @return The count of projections.
   */
  int getProjectionsCount();
  /**
   * <code>repeated string projections = 6;</code>
   * @param index The index of the element to return.
   * @return The projections at the given index.
   */
  java.lang.String getProjections(int index);
  /**
   * <code>repeated string projections = 6;</code>
   * @param index The index of the value to return.
   * @return The bytes of the projections at the given index.
   */
  com.google.protobuf.ByteString
      getProjectionsBytes(int index);
}
