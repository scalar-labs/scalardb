package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Empty;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.CommitRequest;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.StartTransactionRequest;
import com.scalar.db.rpc.StartTransactionResponse;
import com.scalar.db.rpc.TransactionalGetRequest;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.rpc.TransactionalScanResponse;
import com.scalar.db.rpc.util.ProtoUtil;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributedTransactionServiceTest {

  private static final String ANY_ID = "id";

  @Mock private DistributedTransactionManager manager;
  @Mock private DistributedTransaction transaction;
  @Captor private ArgumentCaptor<StatusRuntimeException> exceptionCaptor;

  private DistributedTransactionService transactionService;

  @Before
  public void setUp() throws TransactionException {
    MockitoAnnotations.initMocks(this);

    // Arrange
    transactionService = new DistributedTransactionService(manager);
    when(manager.start()).thenReturn(transaction);
    when(manager.start(anyString())).thenReturn(transaction);
    when(transaction.getId()).thenReturn(ANY_ID);
  }

  @Test
  public void start_IsCalledWithoutAnyArguments_ManagerShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionRequest request = StartTransactionRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.start(request, responseObserver);

    // Assert
    verify(manager).start();
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void start_IsCalledWithTxId_ManagerShouldBeCalledProperly() throws TransactionException {
    // Arrange
    StartTransactionRequest request =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.start(request, responseObserver);

    // Assert
    verify(manager).start(anyString());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void get_IsCalledWithProperArguments_TransactionShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalGetRequest request =
        TransactionalGetRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalGetResponse> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.get(request, responseObserver);

    // Assert
    verify(transaction).get(any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void get_TransactionThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalGetRequest request =
        TransactionalGetRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalGetResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.get(any())).thenThrow(IllegalArgumentException.class);

    // Act
    transactionService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void get_TransactionThrowsCrudConflictException_ShouldThrowFailedPreconditionError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalGetRequest request =
        TransactionalGetRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalGetResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.get(any())).thenThrow(CrudConflictException.class);

    // Act
    transactionService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION);
  }

  @Test
  public void get_TransactionThrowsCrudException_ShouldThrowInternalError() throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalGetRequest request =
        TransactionalGetRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalGetResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.get(any())).thenThrow(CrudException.class);

    // Act
    transactionService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void scan_IsCalledWithProperArguments_TransactionShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalScanRequest request =
        TransactionalScanRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalScanResponse> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.scan(request, responseObserver);

    // Assert
    verify(transaction).scan(any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void scan_TransactionThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalScanRequest request =
        TransactionalScanRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalScanResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.scan(any())).thenThrow(IllegalArgumentException.class);

    // Act
    transactionService.scan(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void scan_TransactionThrowsCrudConflictException_ShouldThrowFailedPreconditionError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalScanRequest request =
        TransactionalScanRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalScanResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.scan(any())).thenThrow(CrudConflictException.class);

    // Act
    transactionService.scan(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION);
  }

  @Test
  public void scan_TransactionThrowsCrudException_ShouldThrowInternalError() throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    TransactionalScanRequest request =
        TransactionalScanRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionalScanResponse> responseObserver = mock(StreamObserver.class);
    when(transaction.scan(any())).thenThrow(CrudException.class);

    // Act
    transactionService.scan(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void mutate_IsCalledWithSinglePut_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addMutations(ProtoUtil.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(transaction).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMultiplePuts_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addAllMutations(
                Arrays.asList(
                    ProtoUtil.toMutation(new Put(partitionKey)),
                    ProtoUtil.toMutation(new Put(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(transaction).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithSingleDelete_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addMutations(ProtoUtil.toMutation(new Delete(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(transaction).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMultipleDeletes_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addAllMutations(
                Arrays.asList(
                    ProtoUtil.toMutation(new Delete(partitionKey)),
                    ProtoUtil.toMutation(new Delete(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(transaction).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMixedPutAndDelete_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addAllMutations(
                Arrays.asList(
                    ProtoUtil.toMutation(new Put(partitionKey)),
                    ProtoUtil.toMutation(new Delete(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(transaction).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_TransactionThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addMutations(ProtoUtil.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(IllegalArgumentException.class).when(transaction).mutate(anyList());

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void mutate_TransactionThrowsCrudConflictException_ShouldThrowFailedPreconditionError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addMutations(ProtoUtil.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(CrudConflictException.class).when(transaction).mutate(anyList());

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION);
  }

  @Test
  public void mutate_TransactionThrowsCrudException_ShouldThrowInternalError()
      throws CrudException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    TransactionalMutateRequest request =
        TransactionalMutateRequest.newBuilder()
            .setTransactionId(ANY_ID)
            .addMutations(ProtoUtil.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(CrudException.class).when(transaction).mutate(anyList());

    // Act
    transactionService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void commit_IsCalledWithProperArguments_TransactionShouldBeCalledProperly()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    CommitRequest request = CommitRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.commit(request, responseObserver);

    // Assert
    verify(transaction).commit();
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void commit_TransactionThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    CommitRequest request = CommitRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(IllegalArgumentException.class).when(transaction).commit();

    // Act
    transactionService.commit(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
  }

  @Test
  public void commit_TransactionThrowsCommitConflictException_ShouldThrowFailedPreconditionError()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    CommitRequest request = CommitRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act
    transactionService.commit(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION);
  }

  @Test
  public void commit_TransactionThrowsUnknownTransactionStatusException_ShouldThrowUnknownError()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    CommitRequest request = CommitRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    // Act
    transactionService.commit(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.UNKNOWN);
  }

  @Test
  public void commit_TransactionThrowsCommitException_ShouldThrowInternalError()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    CommitRequest request = CommitRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(CommitException.class).when(transaction).commit();

    // Act
    transactionService.commit(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void abort_IsCalledWithProperArguments_TransactionShouldBeCalledProperly()
      throws AbortException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(transaction).abort();
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void abort_TransactionThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws AbortException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(IllegalArgumentException.class).when(transaction).abort();

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
  }

  @Test
  public void abort_TransactionThrowsAbortException_ShouldThrowInternalError()
      throws AbortException {
    // Arrange
    StartTransactionRequest startTransactionRequest =
        StartTransactionRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<StartTransactionResponse> startTransactionResponseObserver =
        mock(StreamObserver.class);
    transactionService.start(startTransactionRequest, startTransactionResponseObserver);

    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(AbortException.class).when(transaction).abort();

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void getState_IsCalledWithProperArguments_ManagerShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    GetTransactionStateRequest request =
        GetTransactionStateRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTransactionStateResponse> responseObserver = mock(StreamObserver.class);
    when(manager.getState(anyString())).thenReturn(TransactionState.COMMITTED);

    // Act
    transactionService.getState(request, responseObserver);

    // Assert
    verify(manager).getState(anyString());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void getState_ManagerThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws TransactionException {
    // Arrange
    GetTransactionStateRequest request =
        GetTransactionStateRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTransactionStateResponse> responseObserver = mock(StreamObserver.class);
    when(manager.getState(anyString())).thenThrow(IllegalArgumentException.class);

    // Act
    transactionService.getState(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
  }
}
