package com.scalar.db.transaction.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.RollbackRequest;
import com.scalar.db.rpc.RollbackResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.storage.rpc.GrpcConfig;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcTwoPhaseCommitTransactionManagerTest {
  private static final String ANY_ID = "id";

  @Mock private GrpcConfig config;
  @Mock private DatabaseConfig databaseConfig;
  @Mock private TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub;
  @Mock private TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub;
  @Mock private TableMetadataManager metadataManager;

  private GrpcTwoPhaseCommitTransactionManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    manager =
        new GrpcTwoPhaseCommitTransactionManager(
            config, databaseConfig, stub, blockingStub, metadataManager);
    manager.with("namespace", "table");
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(blockingStub.withDeadlineAfter(anyLong(), any())).thenReturn(blockingStub);
  }

  @Test
  public void begin_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    // Act Assert
    spiedManager.begin(ANY_ID);
    assertThatThrownBy(() -> spiedManager.begin(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.startTransaction(ANY_ID)).thenReturn(ANY_ID);

    // Act Assert
    spiedManager.start(ANY_ID);
    assertThatThrownBy(() -> spiedManager.start(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    TwoPhaseCommitTransaction transaction1 = spiedManager.begin(ANY_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = spiedManager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithStart_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.startTransaction(ANY_ID)).thenReturn(ANY_ID);

    TwoPhaseCommitTransaction transaction1 = spiedManager.start(ANY_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = spiedManager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithJoin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();

    TwoPhaseCommitTransaction transaction1 = spiedManager.join(ANY_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = spiedManager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBeginOrStartOrJoin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    TwoPhaseCommitTransaction transaction = spiedManager.begin(ANY_ID);
    transaction.prepare();
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> spiedManager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    doThrow(CommitException.class).when(bidirectionalStream).commit();

    TwoPhaseCommitTransaction transaction1 = spiedManager.begin(ANY_ID);
    transaction1.prepare();
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    TwoPhaseCommitTransaction transaction2 = spiedManager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    TwoPhaseCommitTransaction transaction = spiedManager.begin(ANY_ID);
    transaction.prepare();
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> spiedManager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      resume_CalledWithBeginAndRollback_RollbackExceptionThrown_ThrowTransactionNotFoundException()
          throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransactionManager spiedManager = spy(manager);
    GrpcTwoPhaseCommitTransactionOnBidirectionalStream bidirectionalStream =
        mock(GrpcTwoPhaseCommitTransactionOnBidirectionalStream.class);
    doReturn(bidirectionalStream).when(spiedManager).getStream();
    when(bidirectionalStream.beginTransaction(ANY_ID)).thenReturn(ANY_ID);

    doThrow(RollbackException.class).when(bidirectionalStream).rollback();

    TwoPhaseCommitTransaction transaction1 = spiedManager.begin(ANY_ID);
    try {
      transaction1.rollback();
    } catch (RollbackException ignored) {
      // expected
    }

    // Act Assert
    assertThatThrownBy(() -> spiedManager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void getState_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    GetTransactionStateResponse response = mock(GetTransactionStateResponse.class);
    when(response.getState())
        .thenReturn(com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_COMMITTED);
    when(blockingStub.getState(any())).thenReturn(response);

    // Act
    TransactionState state = manager.getState(ANY_ID);

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
    verify(blockingStub)
        .getState(GetTransactionStateRequest.newBuilder().setTransactionId(ANY_ID).build());
  }

  @Test
  public void getState_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(blockingStub.getState(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.getState(ANY_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getState_StubThrowsInternalError_ShouldThrowTransactionException() {
    // Arrange
    when(blockingStub.getState(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.getState(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void rollback_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    RollbackResponse response = mock(RollbackResponse.class);
    when(response.getState())
        .thenReturn(com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_ABORTED);
    when(blockingStub.rollback(any())).thenReturn(response);

    // Act
    TransactionState state = manager.rollback(ANY_ID);

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
    verify(blockingStub).rollback(RollbackRequest.newBuilder().setTransactionId(ANY_ID).build());
  }

  @Test
  public void rollback_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(blockingStub.rollback(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.rollback(ANY_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void rollback_StubThrowsInternalError_ShouldThrowTransactionException() {
    // Arrange
    when(blockingStub.rollback(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.rollback(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void abort_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    AbortResponse response = mock(AbortResponse.class);
    when(response.getState())
        .thenReturn(com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_ABORTED);
    when(blockingStub.abort(any())).thenReturn(response);

    // Act
    TransactionState state = manager.abort(ANY_ID);

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
    verify(blockingStub).abort(AbortRequest.newBuilder().setTransactionId(ANY_ID).build());
  }

  @Test
  public void abort_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(blockingStub.abort(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.abort(ANY_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void abort_StubThrowsInternalError_ShouldThrowTransactionException() {
    // Arrange
    when(blockingStub.abort(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.abort(ANY_ID)).isInstanceOf(TransactionException.class);
  }
}
