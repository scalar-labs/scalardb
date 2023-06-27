package com.scalar.db.transaction.rpc;

import static org.mockito.Mockito.verify;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.Error.ErrorCode;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.util.ProtoUtils;
import io.grpc.stub.ClientCallStreamObserver;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class GrpcTwoPhaseCommitTransactionOnBidirectionalStreamTest {
  @Mock private GrpcConfig grpcConfig;
  @Mock private TwoPhaseCommitTransactionStub stub;
  @Mock private TableMetadataManager tableMetadataManager;
  private GrpcTwoPhaseCommitTransactionOnBidirectionalStream twoPhaseCommitTransaction;
  @Mock private ClientCallStreamObserver<TwoPhaseCommitTransactionRequest> requestStream;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    twoPhaseCommitTransaction =
        new GrpcTwoPhaseCommitTransactionOnBidirectionalStream(
            grpcConfig, stub, tableMetadataManager);
    Mockito.when(grpcConfig.getDeadlineDurationMillis()).thenReturn(1000L);
    twoPhaseCommitTransaction.beforeStart(requestStream);
  }

  @Test
  public void
      mutate_singleMutationWithUnsatisfiedCondition_ShouldThrowUnsatisfiedConditionException() {
    mutate_withUnsatisfiedCondition_ShouldThrowUnsatisfiedConditionException(
        put -> twoPhaseCommitTransaction.mutate(put));
  }

  @Test
  public void
      mutate_listOfMutationsWithUnsatisfiedCondition_ShouldThrowUnsatisfiedConditionException() {
    mutate_withUnsatisfiedCondition_ShouldThrowUnsatisfiedConditionException(
        put -> twoPhaseCommitTransaction.mutate(Collections.singletonList(put)));
  }

  private void mutate_withUnsatisfiedCondition_ShouldThrowUnsatisfiedConditionException(
      MutationConsumer mutationConsumer) {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act Assert
    twoPhaseCommitTransaction.onNext(
        TwoPhaseCommitTransactionResponse.newBuilder()
            .setError(
                TwoPhaseCommitTransactionResponse.Error.newBuilder()
                    .setErrorCode(ErrorCode.UNSATISFIED_CONDITION)
                    .setMessage("error_msg")
                    .build())
            .build());
    Assertions.assertThatThrownBy(() -> mutationConsumer.mutate(put))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessage("error_msg");
    verify(requestStream)
        .onNext(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setMutateRequest(
                    TwoPhaseCommitTransactionRequest.MutateRequest.newBuilder()
                        .addMutations(ProtoUtils.toMutation(put)))
                .build());
  }

  @FunctionalInterface
  private interface MutationConsumer {
    void mutate(Put put) throws CrudException;
  }
}
