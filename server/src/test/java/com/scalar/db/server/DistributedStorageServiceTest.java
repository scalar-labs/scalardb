package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Empty;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.util.ProtoUtils;
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

public class DistributedStorageServiceTest {

  @Mock private DistributedStorage storage;
  @Mock private GateKeeper gateKeeper;
  @Captor private ArgumentCaptor<StatusRuntimeException> exceptionCaptor;

  private DistributedStorageService storageService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    storageService = new DistributedStorageService(storage, gateKeeper, new Metrics());
    when(gateKeeper.letIn()).thenReturn(true);
  }

  @Test
  public void get_IsCalledWithProperArguments_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    GetRequest request = GetRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(storage).get(any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void get_StorageThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws ExecutionException {
    // Arrange
    GetRequest request = GetRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);
    when(storage.get(any())).thenThrow(IllegalArgumentException.class);

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void get_StorageThrowsExecutionException_ShouldThrowInternalError()
      throws ExecutionException {
    // Arrange
    GetRequest request = GetRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);
    when(storage.get(any())).thenThrow(ExecutionException.class);

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  public void get_GateKeeperReturnsFalse_ShouldThrowUnavailableError() {
    // Arrange
    GetRequest request = GetRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);
    when(gateKeeper.letIn()).thenReturn(false);

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.UNAVAILABLE);
  }

  @Test
  public void mutate_IsCalledWithSinglePut_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(storage).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMultiplePuts_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addAllMutation(
                Arrays.asList(
                    ProtoUtils.toMutation(new Put(partitionKey)),
                    ProtoUtils.toMutation(new Put(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(storage).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithSingleDelete_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Delete(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(storage).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMultipleDeletes_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addAllMutation(
                Arrays.asList(
                    ProtoUtils.toMutation(new Delete(partitionKey)),
                    ProtoUtils.toMutation(new Delete(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(storage).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithMixedPutAndDelete_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addAllMutation(
                Arrays.asList(
                    ProtoUtils.toMutation(new Put(partitionKey)),
                    ProtoUtils.toMutation(new Delete(partitionKey))))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(storage).mutate(anyList());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_StorageThrowsIllegalArgumentException_ShouldThrowInvalidArgumentError()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(IllegalArgumentException.class).when(storage).mutate(anyList());

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void mutate_StorageThrowsNoMutationException_ShouldThrowFailedPreconditionError()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(NoMutationException.class).when(storage).mutate(anyList());

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION);
  }

  @Test
  public void mutate_StorageThrowsExecutionException_ShouldThrowInternalError()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    doThrow(ExecutionException.class).when(storage).mutate(anyList());

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  public void mutate_GateKeeperReturnsFalse_ShouldThrowUnavailableError() {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutation(ProtoUtils.toMutation(new Put(partitionKey)))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);
    when(gateKeeper.letIn()).thenReturn(false);

    // Act
    storageService.mutate(request, responseObserver);

    // Assert
    verify(responseObserver).onError(exceptionCaptor.capture());
    assertThat(exceptionCaptor.getValue().getStatus().getCode()).isEqualTo(Code.UNAVAILABLE);
  }
}
