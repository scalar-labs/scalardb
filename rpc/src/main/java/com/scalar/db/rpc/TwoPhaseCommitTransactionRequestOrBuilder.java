// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: scalardb.proto

// Protobuf Java Version: 3.25.5
package com.scalar.db.rpc;

public interface TwoPhaseCommitTransactionRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:rpc.TwoPhaseCommitTransactionRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.StartRequest start_request = 1;</code>
   * @return Whether the startRequest field is set.
   */
  boolean hasStartRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.StartRequest start_request = 1;</code>
   * @return The startRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.StartRequest getStartRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.StartRequest start_request = 1;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.StartRequestOrBuilder getStartRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.JoinRequest join_request = 2;</code>
   * @return Whether the joinRequest field is set.
   */
  boolean hasJoinRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.JoinRequest join_request = 2;</code>
   * @return The joinRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.JoinRequest getJoinRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.JoinRequest join_request = 2;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.JoinRequestOrBuilder getJoinRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.GetRequest get_request = 3;</code>
   * @return Whether the getRequest field is set.
   */
  boolean hasGetRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.GetRequest get_request = 3;</code>
   * @return The getRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.GetRequest getGetRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.GetRequest get_request = 3;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.GetRequestOrBuilder getGetRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ScanRequest scan_request = 4;</code>
   * @return Whether the scanRequest field is set.
   */
  boolean hasScanRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ScanRequest scan_request = 4;</code>
   * @return The scanRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ScanRequest getScanRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ScanRequest scan_request = 4;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ScanRequestOrBuilder getScanRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.MutateRequest mutate_request = 5;</code>
   * @return Whether the mutateRequest field is set.
   */
  boolean hasMutateRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.MutateRequest mutate_request = 5;</code>
   * @return The mutateRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.MutateRequest getMutateRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.MutateRequest mutate_request = 5;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.MutateRequestOrBuilder getMutateRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest prepare_request = 6;</code>
   * @return Whether the prepareRequest field is set.
   */
  boolean hasPrepareRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest prepare_request = 6;</code>
   * @return The prepareRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest getPrepareRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest prepare_request = 6;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.PrepareRequestOrBuilder getPrepareRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest validate_request = 7;</code>
   * @return Whether the validateRequest field is set.
   */
  boolean hasValidateRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest validate_request = 7;</code>
   * @return The validateRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest getValidateRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest validate_request = 7;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ValidateRequestOrBuilder getValidateRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.CommitRequest commit_request = 8;</code>
   * @return Whether the commitRequest field is set.
   */
  boolean hasCommitRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.CommitRequest commit_request = 8;</code>
   * @return The commitRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.CommitRequest getCommitRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.CommitRequest commit_request = 8;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.CommitRequestOrBuilder getCommitRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest rollback_request = 9;</code>
   * @return Whether the rollbackRequest field is set.
   */
  boolean hasRollbackRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest rollback_request = 9;</code>
   * @return The rollbackRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest getRollbackRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest rollback_request = 9;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RollbackRequestOrBuilder getRollbackRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.BeginRequest begin_request = 10;</code>
   * @return Whether the beginRequest field is set.
   */
  boolean hasBeginRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.BeginRequest begin_request = 10;</code>
   * @return The beginRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.BeginRequest getBeginRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.BeginRequest begin_request = 10;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.BeginRequestOrBuilder getBeginRequestOrBuilder();

  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.AbortRequest abort_request = 11;</code>
   * @return Whether the abortRequest field is set.
   */
  boolean hasAbortRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.AbortRequest abort_request = 11;</code>
   * @return The abortRequest.
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.AbortRequest getAbortRequest();
  /**
   * <code>.rpc.TwoPhaseCommitTransactionRequest.AbortRequest abort_request = 11;</code>
   */
  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.AbortRequestOrBuilder getAbortRequestOrBuilder();

  com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RequestCase getRequestCase();
}
