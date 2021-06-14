package com.scalar.db.transaction.rpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.StartTransactionResponse;
import com.scalar.db.rpc.TransactionState;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalScanResponse;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import io.grpc.Status;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcTransactionManagerTest {

  private static final String ANY_ID = "id";

  @Mock private DistributedTransactionGrpc.DistributedTransactionBlockingStub stub;
  @Mock private GrpcTableMetadataManager metadataManager;

  private GrpcTransactionManager manager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    manager = new GrpcTransactionManager(stub, metadataManager);
    manager.with("namespace", "table");
  }

  @Test
  public void start_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse response = mock(StartTransactionResponse.class);
    when(response.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(response);

    // Act
    manager.start();

    // Assert
    verify(stub).start(any());
  }

  @Test
  public void start_IsCalledWithTxId_StubShouldBeCalledProperly() throws TransactionException {
    // Arrange
    StartTransactionResponse response = mock(StartTransactionResponse.class);
    when(response.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(response);

    // Act
    manager.start(ANY_ID);

    // Assert
    verify(stub).start(any());
  }

  @Test
  public void start_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(stub.start(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.start()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void start_StubThrowsInternalError_ShouldThrowTransactionException() {
    // Arrange
    when(stub.start(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.start()).isInstanceOf(TransactionException.class);
  }

  @Test
  public void getState_IsCalledWithoutAnyArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    GetTransactionStateResponse response = mock(GetTransactionStateResponse.class);
    when(response.getState()).thenReturn(TransactionState.TRANSACTION_STATE_COMMITTED);
    when(stub.getState(any())).thenReturn(response);

    // Act
    manager.getState(ANY_ID);

    // Assert
    verify(stub).getState(any());
  }

  @Test
  public void getState_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(stub.getState(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.getState(ANY_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getState_StubThrowsInternalError_ShouldThrowTransactionException() {
    // Arrange
    when(stub.getState(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> manager.getState(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void get_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Get get = new Get(partitionKey);
    TransactionalGetResponse response = mock(TransactionalGetResponse.class);
    when(response.hasResult()).thenReturn(false);
    when(stub.get(any())).thenReturn(response);

    // Act
    transaction.get(get);

    // Assert
    verify(transaction).get(any());
  }

  @Test
  public void get_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Get get = new Get(partitionKey);
    when(stub.get(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Get get = new Get(partitionKey);
    when(stub.get(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void get_StubThrowsInternalError_ShouldThrowCrudException() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Get get = new Get(partitionKey);
    when(stub.get(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(CrudException.class);
  }

  @Test
  public void scan_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Scan scan = new Scan(partitionKey);
    TransactionalScanResponse response = mock(TransactionalScanResponse.class);
    when(response.getResultList()).thenReturn(Collections.emptyList());
    when(stub.scan(any())).thenReturn(response);

    // Act
    transaction.scan(scan);

    // Assert
    verify(transaction).scan(any());
  }

  @Test
  public void scan_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Scan scan = new Scan(partitionKey);
    when(stub.scan(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Scan scan = new Scan(partitionKey);
    when(stub.scan(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void scan_StubThrowsInternalError_ShouldThrowCrudException() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Scan scan = new Scan(partitionKey);
    when(stub.scan(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(CrudException.class);
  }

  @Test
  public void put_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Put put = new Put(partitionKey);

    // Act
    transaction.put(put);

    // Assert
    verify(transaction).put(any(Put.class));
  }

  @Test
  public void put_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Put put = new Put(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Put put = new Put(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void put_StubThrowsInternalError_ShouldThrowCrudException() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Put put = new Put(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(CrudException.class);
  }

  @Test
  public void puts_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));

    // Act
    transaction.put(puts);

    // Assert
    verify(transaction).put(anyList());
  }

  @Test
  public void puts_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(puts)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void puts_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(puts)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void puts_StubThrowsInternalError_ShouldThrowCrudException() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.put(puts)).isInstanceOf(CrudException.class);
  }

  @Test
  public void delete_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Delete delete = new Delete(partitionKey);

    // Act
    transaction.delete(delete);

    // Assert
    verify(transaction).delete(any(Delete.class));
  }

  @Test
  public void delete_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Delete delete = new Delete(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void delete_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Delete delete = new Delete(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(delete)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void delete_StubThrowsInternalError_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    Delete delete = new Delete(partitionKey);
    when(stub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(delete)).isInstanceOf(CrudException.class);
  }

  @Test
  public void deletes_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));

    // Act
    transaction.delete(deletes);

    // Assert
    verify(transaction).delete(anyList());
  }

  @Test
  public void deletes_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(deletes))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void deletes_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(deletes)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void deletes_StubThrowsInternalError_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey1 = Key.newBuilder().addInt("col1", 1).build();
    Key partitionKey2 = Key.newBuilder().addInt("col1", 2).build();
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(stub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.delete(deletes)).isInstanceOf(CrudException.class);
  }

  @Test
  public void mutate_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));

    // Act
    transaction.mutate(mutations);

    // Assert
    verify(transaction).mutate(anyList());
  }

  @Test
  public void mutate_StubThrowsInvalidArgumentError_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(stub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.mutate(mutations))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void mutate_StubThrowsFailedPreconditionError_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(stub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.mutate(mutations))
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void mutate_StubThrowsInternalError_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    Key partitionKey = Key.newBuilder().addInt("col1", 1).build();
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(stub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act　Assert
    assertThatThrownBy(() -> transaction.mutate(mutations)).isInstanceOf(CrudException.class);
  }

  @Test
  public void commit_IsCalled_StubShouldBeCalledProperly() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    // Act
    transaction.commit();

    // Assert
    verify(transaction).commit();
  }

  @Test
  public void commit_StubThrowsFailedPreconditionError_ShouldThrowCommitConflictException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    when(stub.commit(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void commit_StubThrowsUnknownError_ShouldThrowUnknownTransactionStatusException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    when(stub.commit(any())).thenThrow(Status.UNKNOWN.asRuntimeException());

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(UnknownTransactionStatusException.class);
  }

  @Test
  public void commit_StubThrowsInternalError_ShouldThrowCommitException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    when(stub.commit(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
  }

  @Test
  public void abort_IsCalled_StubShouldBeCalledProperly() throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    // Act
    transaction.abort();

    // Assert
    verify(transaction).abort();
  }

  @Test
  public void abort_StubThrowsInternalError_ShouldThrowAbortException()
      throws TransactionException {
    // Arrange
    StartTransactionResponse startTransactionResponse = mock(StartTransactionResponse.class);
    when(startTransactionResponse.getTransactionId()).thenReturn(ANY_ID);
    when(stub.start(any())).thenReturn(startTransactionResponse);
    GrpcTransaction transaction = spy(manager.start(ANY_ID));

    when(stub.abort(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act Assert
    assertThatThrownBy(transaction::abort).isInstanceOf(AbortException.class);
  }
}
