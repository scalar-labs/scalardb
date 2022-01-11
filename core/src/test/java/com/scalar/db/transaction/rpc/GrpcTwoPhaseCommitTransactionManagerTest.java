package com.scalar.db.transaction.rpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TransactionState;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.util.TableMetadataManager;
import io.grpc.Status;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcTwoPhaseCommitTransactionManagerTest {
  private static final String ANY_ID = "id";

  @Mock private GrpcConfig config;
  @Mock private TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub;
  @Mock private TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub;
  @Mock private TableMetadataManager metadataManager;

  private GrpcTwoPhaseCommitTransactionManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    manager = new GrpcTwoPhaseCommitTransactionManager(config, stub, blockingStub, metadataManager);
    manager.with("namespace", "table");
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(blockingStub.withDeadlineAfter(anyLong(), any())).thenReturn(blockingStub);
  }

  @Test
  public void
      resume_WhenActiveTransactionsManagementEnabledIsFalse_ShouldThrowUnsupportedOperationException() {
    // Arrange
    when(config.isActiveTransactionsManagementEnabled()).thenReturn(false);
    manager = new GrpcTwoPhaseCommitTransactionManager(config, stub, blockingStub, metadataManager);

    // Act Assert
    AssertionsForClassTypes.assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getState_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    GetTransactionStateResponse response = mock(GetTransactionStateResponse.class);
    when(response.getState()).thenReturn(TransactionState.TRANSACTION_STATE_COMMITTED);
    when(blockingStub.getState(any())).thenReturn(response);

    // Act
    manager.getState(ANY_ID);

    // Assert
    verify(blockingStub).getState(any());
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
  public void abort_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    AbortResponse response = mock(AbortResponse.class);
    when(response.getState()).thenReturn(TransactionState.TRANSACTION_STATE_ABORTED);
    when(blockingStub.abort(any())).thenReturn(response);

    // Act
    manager.abort(ANY_ID);

    // Assert
    verify(blockingStub).abort(any());
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
