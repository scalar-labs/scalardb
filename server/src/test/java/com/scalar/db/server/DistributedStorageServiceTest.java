package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.rpc.Get;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.rpc.ScanRequest;
import com.scalar.db.rpc.ScanResponse;
import com.scalar.db.rpc.Value;
import com.scalar.db.rpc.Value.BlobValue;
import com.scalar.db.rpc.Value.TextValue;
import com.scalar.db.server.DistributedStorageService.ScanStreamObserver;
import com.scalar.db.util.ProtoUtils;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributedStorageServiceTest {

  private final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.INT)
          .addColumn("p2", DataType.TEXT)
          .addColumn("c1", DataType.TEXT)
          .addColumn("c2", DataType.INT)
          .addColumn("col1", DataType.BOOLEAN)
          .addColumn("col2", DataType.INT)
          .addColumn("col3", DataType.BIGINT)
          .addColumn("col4", DataType.FLOAT)
          .addColumn("col5", DataType.DOUBLE)
          .addColumn("col6", DataType.TEXT)
          .addColumn("col7", DataType.BLOB)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .addClusteringKey("c1", Scan.Ordering.Order.DESC)
          .addClusteringKey("c2", Scan.Ordering.Order.ASC)
          .addSecondaryIndex("col2")
          .addSecondaryIndex("col4")
          .build();

  @Mock private DistributedStorage storage;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private GateKeeper gateKeeper;
  @Captor private ArgumentCaptor<StatusRuntimeException> exceptionCaptor;

  private DistributedStorageService storageService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    storageService =
        new DistributedStorageService(storage, tableMetadataManager, gateKeeper, new Metrics());
    when(tableMetadataManager.getTableMetadata(any(), any())).thenReturn(TABLE_METADATA);
    when(gateKeeper.letIn()).thenReturn(true);
  }

  @Test
  public void get_ProperArgumentsGiven_StorageShouldBeCalledProperly() throws ExecutionException {
    // Arrange
    GetRequest request = GetRequest.newBuilder().build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);

    when(storage.get(any()))
        .thenReturn(
            Optional.of(
                new ResultImpl(
                    ImmutableMap.<String, Column<?>>builder()
                        .put("p1", IntColumn.of("p1", 10))
                        .put("p2", TextColumn.of("p2", "text1"))
                        .put("c1", TextColumn.of("c1", "text2"))
                        .put("c2", IntColumn.of("c2", 20))
                        .put("col1", BooleanColumn.of("col1", true))
                        .put("col2", IntColumn.of("col2", 10))
                        .put("col3", BigIntColumn.of("col3", 100L))
                        .put("col4", FloatColumn.of("col4", 1.23F))
                        .put("col5", DoubleColumn.of("col5", 4.56))
                        .put("col6", TextColumn.of("col6", "text"))
                        .put("col7", BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                        .build(),
                    TABLE_METADATA)));

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(storage).get(any());
    verify(responseObserver)
        .onNext(
            GetResponse.newBuilder()
                .setResult(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col1")
                                .setBooleanValue(true)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col2")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col3")
                                .setBigintValue(100L)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col4")
                                .setFloatValue(1.23F)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col5")
                                .setDoubleValue(4.56)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col6")
                                .setTextValue("text")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col7")
                                .setBlobValue(
                                    ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                                .build())
                        .build())
                .build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void get_ProperArgumentsFromOldClientGiven_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    GetRequest request =
        GetRequest.newBuilder()
            .setGet(
                Get.newBuilder()
                    .setPartitionKey(
                        com.scalar.db.rpc.Key.newBuilder()
                            .addValue(Value.newBuilder().setName("p1").setIntValue(1).build())
                            .build())
                    .build())
            .build();

    @SuppressWarnings("unchecked")
    StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);

    when(storage.get(any()))
        .thenReturn(
            Optional.of(
                new ResultImpl(
                    ImmutableMap.<String, Column<?>>builder()
                        .put("p1", IntColumn.of("p1", 10))
                        .put("p2", TextColumn.of("p2", "text1"))
                        .put("c1", TextColumn.of("c1", "text2"))
                        .put("c2", IntColumn.of("c2", 20))
                        .put("col1", BooleanColumn.of("col1", true))
                        .put("col2", IntColumn.of("col2", 10))
                        .put("col3", BigIntColumn.of("col3", 100L))
                        .put("col4", FloatColumn.of("col4", 1.23F))
                        .put("col5", DoubleColumn.of("col5", 4.56))
                        .put("col6", TextColumn.of("col6", "text"))
                        .put("col7", BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                        .build(),
                    TABLE_METADATA)));

    // Act
    storageService.get(request, responseObserver);

    // Assert
    verify(storage).get(any());
    verify(responseObserver)
        .onNext(
            GetResponse.newBuilder()
                .setResult(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("p2")
                                .setTextValue(TextValue.newBuilder().setValue("text1").build())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("c1")
                                .setTextValue(TextValue.newBuilder().setValue("text2").build())
                                .build())
                        .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                        .addValue(Value.newBuilder().setName("col1").setBooleanValue(true).build())
                        .addValue(Value.newBuilder().setName("col2").setIntValue(10).build())
                        .addValue(Value.newBuilder().setName("col3").setBigintValue(100L).build())
                        .addValue(Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
                        .addValue(Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col6")
                                .setTextValue(TextValue.newBuilder().setValue("text").build())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col7")
                                .setBlobValue(
                                    BlobValue.newBuilder()
                                        .setValue(
                                            ByteString.copyFrom(
                                                "blob".getBytes(StandardCharsets.UTF_8)))
                                        .build())
                                .build())
                        .build())
                .build());
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
  public void scan_ProperArgumentsGiven_StorageShouldBeCalledProperly() throws ExecutionException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<ScanResponse> responseObserver = mock(StreamObserver.class);

    ScanStreamObserver scanStreamObserver =
        new ScanStreamObserver(
            storage, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    ScanRequest request =
        ScanRequest.newBuilder().setScan(com.scalar.db.rpc.Scan.newBuilder().build()).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator())
        .thenReturn(
            Arrays.asList(
                    (Result)
                        new ResultImpl(
                            ImmutableMap.<String, Column<?>>builder()
                                .put("p1", IntColumn.of("p1", 10))
                                .put("p2", TextColumn.of("p2", "text1"))
                                .put("c1", TextColumn.of("c1", "text2"))
                                .put("c2", IntColumn.of("c2", 20))
                                .put("col1", BooleanColumn.of("col1", true))
                                .put("col2", IntColumn.of("col2", 10))
                                .put("col3", BigIntColumn.of("col3", 100L))
                                .put("col4", FloatColumn.of("col4", 1.23F))
                                .put("col5", DoubleColumn.of("col5", 4.56))
                                .put("col6", TextColumn.of("col6", "text"))
                                .put(
                                    "col7",
                                    BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                            TABLE_METADATA),
                    new ResultImpl(
                        ImmutableMap.<String, Column<?>>builder()
                            .put("p1", IntColumn.of("p1", 10))
                            .put("p2", TextColumn.of("p2", "text1"))
                            .put("c1", TextColumn.of("c1", "text2"))
                            .put("c2", IntColumn.of("c2", 20))
                            .put("col1", BooleanColumn.ofNull("col1"))
                            .put("col2", IntColumn.ofNull("col2"))
                            .put("col3", BigIntColumn.ofNull("col3"))
                            .put("col4", FloatColumn.ofNull("col4"))
                            .put("col5", DoubleColumn.ofNull("col5"))
                            .put("col6", TextColumn.ofNull("col6"))
                            .put("col7", BlobColumn.ofNull("col7"))
                            .build(),
                        TABLE_METADATA))
                .iterator());

    when(storage.scan(any())).thenReturn(scanner);

    // Act
    scanStreamObserver.onNext(request);

    // Assert
    verify(storage).scan(any());
    verify(responseObserver)
        .onNext(
            ScanResponse.newBuilder()
                .addResults(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col1")
                                .setBooleanValue(true)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col2")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col3")
                                .setBigintValue(100L)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col4")
                                .setFloatValue(1.23F)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col5")
                                .setDoubleValue(4.56)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col6")
                                .setTextValue("text")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col7")
                                .setBlobValue(
                                    ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                                .build())
                        .build())
                .addResults(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                        .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                        .build())
                .build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void scan_ProperArgumentsFromOldClientGiven_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<ScanResponse> responseObserver = mock(StreamObserver.class);

    ScanStreamObserver scanStreamObserver =
        new ScanStreamObserver(
            storage, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    ScanRequest request =
        ScanRequest.newBuilder()
            .setScan(
                com.scalar.db.rpc.Scan.newBuilder()
                    .setPartitionKey(
                        com.scalar.db.rpc.Key.newBuilder()
                            .addValue(Value.newBuilder().setName("p1").setIntValue(1).build())
                            .build())
                    .build())
            .build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator())
        .thenReturn(
            Arrays.asList(
                    (Result)
                        new ResultImpl(
                            ImmutableMap.<String, Column<?>>builder()
                                .put("p1", IntColumn.of("p1", 10))
                                .put("p2", TextColumn.of("p2", "text1"))
                                .put("c1", TextColumn.of("c1", "text2"))
                                .put("c2", IntColumn.of("c2", 20))
                                .put("col1", BooleanColumn.of("col1", true))
                                .put("col2", IntColumn.of("col2", 10))
                                .put("col3", BigIntColumn.of("col3", 100L))
                                .put("col4", FloatColumn.of("col4", 1.23F))
                                .put("col5", DoubleColumn.of("col5", 4.56))
                                .put("col6", TextColumn.of("col6", "text"))
                                .put(
                                    "col7",
                                    BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                            TABLE_METADATA),
                    new ResultImpl(
                        ImmutableMap.<String, Column<?>>builder()
                            .put("p1", IntColumn.of("p1", 10))
                            .put("p2", TextColumn.of("p2", "text1"))
                            .put("c1", TextColumn.of("c1", "text2"))
                            .put("c2", IntColumn.of("c2", 20))
                            .put("col1", BooleanColumn.ofNull("col1"))
                            .put("col2", IntColumn.ofNull("col2"))
                            .put("col3", BigIntColumn.ofNull("col3"))
                            .put("col4", FloatColumn.ofNull("col4"))
                            .put("col5", DoubleColumn.ofNull("col5"))
                            .put("col6", TextColumn.ofNull("col6"))
                            .put("col7", BlobColumn.ofNull("col7"))
                            .build(),
                        TABLE_METADATA))
                .iterator());

    when(storage.scan(any())).thenReturn(scanner);

    // Act
    scanStreamObserver.onNext(request);

    // Assert
    verify(storage).scan(any());
    verify(responseObserver)
        .onNext(
            ScanResponse.newBuilder()
                .addResults(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("p2")
                                .setTextValue(TextValue.newBuilder().setValue("text1").build())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("c1")
                                .setTextValue(TextValue.newBuilder().setValue("text2").build())
                                .build())
                        .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                        .addValue(Value.newBuilder().setName("col1").setBooleanValue(true).build())
                        .addValue(Value.newBuilder().setName("col2").setIntValue(10).build())
                        .addValue(Value.newBuilder().setName("col3").setBigintValue(100L).build())
                        .addValue(Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
                        .addValue(Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col6")
                                .setTextValue(TextValue.newBuilder().setValue("text").build())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col7")
                                .setBlobValue(
                                    BlobValue.newBuilder()
                                        .setValue(
                                            ByteString.copyFrom(
                                                "blob".getBytes(StandardCharsets.UTF_8)))
                                        .build())
                                .build())
                        .build())
                .addResults(
                    com.scalar.db.rpc.Result.newBuilder()
                        .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("p2")
                                .setTextValue(TextValue.newBuilder().setValue("text1").build())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("c1")
                                .setTextValue(TextValue.newBuilder().setValue("text2").build())
                                .build())
                        .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                        .addValue(Value.newBuilder().setName("col1").setBooleanValue(false).build())
                        .addValue(Value.newBuilder().setName("col2").setIntValue(0).build())
                        .addValue(Value.newBuilder().setName("col3").setBigintValue(0L).build())
                        .addValue(Value.newBuilder().setName("col4").setFloatValue(0.0F).build())
                        .addValue(Value.newBuilder().setName("col5").setDoubleValue(0.0).build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col6")
                                .setTextValue(TextValue.getDefaultInstance())
                                .build())
                        .addValue(
                            Value.newBuilder()
                                .setName("col7")
                                .setBlobValue(BlobValue.getDefaultInstance())
                                .build())
                        .build())
                .build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void mutate_IsCalledWithSinglePut_StorageShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    Key partitionKey = new Key("col1", 1);
    MutateRequest request =
        MutateRequest.newBuilder()
            .addMutations(ProtoUtils.toMutation(new Put(partitionKey)))
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
            .addAllMutations(
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
            .addMutations(ProtoUtils.toMutation(new Delete(partitionKey)))
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
            .addAllMutations(
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
            .addAllMutations(
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
            .addMutations(ProtoUtils.toMutation(new Put(partitionKey)))
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
            .addMutations(ProtoUtils.toMutation(new Put(partitionKey)))
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
            .addMutations(ProtoUtils.toMutation(new Put(partitionKey)))
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
            .addMutations(ProtoUtils.toMutation(new Put(partitionKey)))
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
