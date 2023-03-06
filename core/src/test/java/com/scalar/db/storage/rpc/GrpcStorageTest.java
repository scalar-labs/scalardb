package com.scalar.db.storage.rpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetResponse;
import io.grpc.Status;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcStorageTest {
  @Mock private DatabaseConfig databaseConfig;
  @Mock private GrpcConfig config;
  @Mock private DistributedStorageGrpc.DistributedStorageStub stub;
  @Mock private DistributedStorageGrpc.DistributedStorageBlockingStub blockingStub;
  @Mock private TableMetadataManager metadataManager;

  private GrpcStorage storage;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    storage = new GrpcStorage(databaseConfig, config, stub, blockingStub, metadataManager);
    storage.with("namespace", "table");
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(blockingStub.withDeadlineAfter(anyLong(), any())).thenReturn(blockingStub);
  }

  @Test
  public void get_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Get get = new Get(partitionKey);
    when(blockingStub.get(any())).thenReturn(GetResponse.newBuilder().build());

    // Act
    storage.get(get);

    // Assert
    verify(blockingStub).get(any());
  }

  @Test
  public void get_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Get get = new Get(partitionKey);
    when(blockingStub.get(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Get get = new Get(partitionKey);
    when(blockingStub.get(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void put_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Put put = new Put(partitionKey);

    // Act
    storage.put(put);

    // Assert
    verify(blockingStub).mutate(any());
  }

  @Test
  public void put_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Put put = new Put(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_StubThrowFailedPreconditionError_ShouldThrowNoMutationException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Put put = new Put(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void put_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Put put = new Put(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void puts_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));

    // Act
    storage.put(puts);

    // Assert
    verify(blockingStub).mutate(any());
  }

  @Test
  public void puts_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.put(puts)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void puts_StubThrowFailedPreconditionError_ShouldThrowNoMutationException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.put(puts)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void puts_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Put> puts = Arrays.asList(new Put(partitionKey2), new Put(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.put(puts)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void delete_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Delete delete = new Delete(partitionKey);

    // Act
    storage.delete(delete);

    // Assert
    verify(blockingStub).mutate(any());
  }

  @Test
  public void delete_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Delete delete = new Delete(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void delete_StubThrowFailedPreconditionError_ShouldThrowNoMutationException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Delete delete = new Delete(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void delete_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    Delete delete = new Delete(partitionKey);
    when(blockingStub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void deletes_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));

    // Act
    storage.delete(deletes);

    // Assert
    verify(blockingStub).mutate(any());
  }

  @Test
  public void deletes_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.delete(deletes)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void deletes_StubThrowFailedPreconditionError_ShouldThrowNoMutationException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.delete(deletes)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void deletes_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey1 = new Key("col1", 1);
    Key partitionKey2 = new Key("col1", 2);
    List<Delete> deletes = Arrays.asList(new Delete(partitionKey2), new Delete(partitionKey1));
    when(blockingStub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.delete(deletes)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void mutate_isCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));

    // Act
    storage.mutate(mutations);

    // Assert
    verify(blockingStub).mutate(any());
  }

  @Test
  public void mutate_StubThrowInvalidArgumentError_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(blockingStub.mutate(any())).thenThrow(Status.INVALID_ARGUMENT.asRuntimeException());

    // Act Assert
    assertThatThrownBy(() -> storage.mutate(mutations))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void mutate_StubThrowFailedPreconditionError_ShouldThrowNoMutationException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(blockingStub.mutate(any())).thenThrow(Status.FAILED_PRECONDITION.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.mutate(mutations)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void mutate_StubThrowInternalError_ShouldThrowExecutionException() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    List<Mutation> mutations = Arrays.asList(new Put(partitionKey), new Delete(partitionKey));
    when(blockingStub.mutate(any())).thenThrow(Status.INTERNAL.asRuntimeException());

    // Act
    assertThatThrownBy(() -> storage.mutate(mutations)).isInstanceOf(ExecutionException.class);
  }
}
