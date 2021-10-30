package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributedTransactionServiceTest {

  private static final String ANY_ID = "id";

  @Mock private DistributedTransactionManager manager;
  @Mock private GateKeeper gateKeeper;
  @Mock private DistributedTransaction transaction;
  @Captor private ArgumentCaptor<StatusRuntimeException> exceptionCaptor;

  private DistributedTransactionService transactionService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionService = new DistributedTransactionService(manager, gateKeeper, new Metrics());
    when(manager.start()).thenReturn(transaction);
    when(manager.start(anyString())).thenReturn(transaction);
    when(transaction.getId()).thenReturn(ANY_ID);
    when(gateKeeper.letIn()).thenReturn(true);
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

  @Test
  public void getState_ManagerThrowsTransactionException_ShouldThrowInternalError()
      throws TransactionException {
    // Arrange
    GetTransactionStateRequest request =
        GetTransactionStateRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTransactionStateResponse> responseObserver = mock(StreamObserver.class);
    when(manager.getState(anyString())).thenThrow(TransactionException.class);

    // Act
    transactionService.getState(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void getState_GateKeeperReturnsFalse_ShouldThrowUnavailableError() {
    // Arrange
    GetTransactionStateRequest request =
        GetTransactionStateRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTransactionStateResponse> responseObserver = mock(StreamObserver.class);
    when(gateKeeper.letIn()).thenReturn(false);

    // Act
    transactionService.getState(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void abort_IsCalledWithProperArguments_ManagerShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<AbortResponse> responseObserver = mock(StreamObserver.class);
    when(manager.abort(anyString())).thenReturn(TransactionState.ABORTED);

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(manager).abort(anyString());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void abort_ManagerThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws TransactionException {
    // Arrange
    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<AbortResponse> responseObserver = mock(StreamObserver.class);
    when(manager.abort(anyString())).thenThrow(IllegalArgumentException.class);

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
  }

  @Test
  public void abort_ManagerThrowsTransactionException_ShouldThrowInternalError()
      throws TransactionException {
    // Arrange
    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<AbortResponse> responseObserver = mock(StreamObserver.class);
    when(manager.abort(anyString())).thenThrow(TransactionException.class);

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL);
  }

  @Test
  public void abort_GateKeeperReturnsFalse_ShouldThrowUnavailableError() {
    // Arrange
    AbortRequest request = AbortRequest.newBuilder().setTransactionId(ANY_ID).build();
    @SuppressWarnings("unchecked")
    StreamObserver<AbortResponse> responseObserver = mock(StreamObserver.class);
    when(gateKeeper.letIn()).thenReturn(false);

    // Act
    transactionService.abort(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.UNAVAILABLE);
  }
}
