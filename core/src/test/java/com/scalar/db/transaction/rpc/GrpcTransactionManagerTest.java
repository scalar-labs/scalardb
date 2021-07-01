package com.scalar.db.transaction.rpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TransactionState;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcTransactionManagerTest {

  private static final String ANY_ID = "id";

  @Mock private DistributedTransactionGrpc.DistributedTransactionStub stub;
  @Mock private DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub;
  @Mock private GrpcTableMetadataManager metadataManager;

  private GrpcTransactionManager manager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    manager = new GrpcTransactionManager(stub, blockingStub, metadataManager);
    manager.with("namespace", "table");
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
}
