package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.Get;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.GetRequest;
import com.scalar.db.rpc.TransactionRequest.ScanRequest;
import com.scalar.db.rpc.TransactionRequest.StartRequest;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.GetResponse;
import com.scalar.db.rpc.TransactionResponse.ScanResponse;
import com.scalar.db.rpc.Value;
import com.scalar.db.rpc.Value.BlobValue;
import com.scalar.db.rpc.Value.TextValue;
import com.scalar.db.server.DistributedTransactionService.TransactionStreamObserver;
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

public class DistributedTransactionServiceTest {

  private static final String ANY_ID = "id";

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

  @Mock private DistributedTransactionManager manager;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private GateKeeper gateKeeper;
  @Mock private DistributedTransaction transaction;
  @Captor private ArgumentCaptor<StatusRuntimeException> exceptionCaptor;

  private DistributedTransactionService transactionService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionService =
        new DistributedTransactionService(manager, tableMetadataManager, gateKeeper, new Metrics());
    when(manager.start()).thenReturn(transaction);
    when(manager.start(anyString())).thenReturn(transaction);
    when(transaction.getId()).thenReturn(ANY_ID);
    when(tableMetadataManager.getTableMetadata(any(), any())).thenReturn(TABLE_METADATA);
    when(gateKeeper.letIn()).thenReturn(true);
  }

  @Test
  public void get_ProperArgumentsGiven_TransactionShouldBeCalledProperly() throws CrudException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionResponse> responseObserver = mock(StreamObserver.class);
    TransactionStreamObserver transactionStreamObserver =
        new TransactionStreamObserver(
            manager, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    TransactionRequest request =
        TransactionRequest.newBuilder()
            .setGetRequest(GetRequest.newBuilder().setGet(Get.newBuilder().build()).build())
            .build();

    when(transaction.get(any()))
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

    transactionStreamObserver.onNext(
        TransactionRequest.newBuilder().setStartRequest(StartRequest.getDefaultInstance()).build());

    // Act
    transactionStreamObserver.onNext(request);

    // Assert
    verify(transaction).get(any());
    verify(responseObserver)
        .onNext(
            TransactionResponse.newBuilder()
                .setGetResponse(
                    GetResponse.newBuilder()
                        .setResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p1")
                                        .setIntValue(10)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p2")
                                        .setTextValue("text1")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c1")
                                        .setTextValue("text2")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c2")
                                        .setIntValue(20)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col2")
                                        .setIntValue(10)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col3")
                                        .setBigintValue(100L)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col4")
                                        .setFloatValue(1.23F)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col5")
                                        .setDoubleValue(4.56)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col6")
                                        .setTextValue("text")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col7")
                                        .setBlobValue(
                                            ByteString.copyFrom(
                                                "blob".getBytes(StandardCharsets.UTF_8)))
                                        .build())
                                .build()))
                .build());
  }

  @Test
  public void get_ProperArgumentsFromOldClientGiven_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionResponse> responseObserver = mock(StreamObserver.class);
    TransactionStreamObserver transactionStreamObserver =
        new TransactionStreamObserver(
            manager, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    TransactionRequest request =
        TransactionRequest.newBuilder()
            .setGetRequest(
                GetRequest.newBuilder()
                    .setGet(
                        Get.newBuilder()
                            .setPartitionKey(
                                com.scalar.db.rpc.Key.newBuilder()
                                    .addValue(
                                        Value.newBuilder().setName("p1").setIntValue(1).build())
                                    .build())
                            .build())
                    .build())
            .build();

    when(transaction.get(any()))
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

    transactionStreamObserver.onNext(
        TransactionRequest.newBuilder().setStartRequest(StartRequest.getDefaultInstance()).build());

    // Act
    transactionStreamObserver.onNext(request);

    // Assert
    verify(transaction).get(any());
    verify(responseObserver)
        .onNext(
            TransactionResponse.newBuilder()
                .setGetResponse(
                    GetResponse.newBuilder()
                        .setResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("p2")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text1").build())
                                        .build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("c1")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text2").build())
                                        .build())
                                .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .addValue(
                                    Value.newBuilder().setName("col2").setIntValue(10).build())
                                .addValue(
                                    Value.newBuilder().setName("col3").setBigintValue(100L).build())
                                .addValue(
                                    Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
                                .addValue(
                                    Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("col6")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text").build())
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
                                .build()))
                .build());
  }

  @Test
  public void scan_ProperArgumentsGiven_TransactionShouldBeCalledProperly() throws CrudException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionResponse> responseObserver = mock(StreamObserver.class);
    TransactionStreamObserver transactionStreamObserver =
        new TransactionStreamObserver(
            manager, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    TransactionRequest request =
        TransactionRequest.newBuilder()
            .setScanRequest(
                ScanRequest.newBuilder()
                    .setScan(com.scalar.db.rpc.Scan.newBuilder().build())
                    .build())
            .build();

    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
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
                    TABLE_METADATA)));

    transactionStreamObserver.onNext(
        TransactionRequest.newBuilder().setStartRequest(StartRequest.getDefaultInstance()).build());

    // Act
    transactionStreamObserver.onNext(request);

    // Assert
    verify(transaction).scan(any());
    verify(responseObserver)
        .onNext(
            TransactionResponse.newBuilder()
                .setScanResponse(
                    ScanResponse.newBuilder()
                        .addResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p1")
                                        .setIntValue(10)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p2")
                                        .setTextValue("text1")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c1")
                                        .setTextValue("text2")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c2")
                                        .setIntValue(20)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col2")
                                        .setIntValue(10)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col3")
                                        .setBigintValue(100L)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col4")
                                        .setFloatValue(1.23F)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col5")
                                        .setDoubleValue(4.56)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col6")
                                        .setTextValue("text")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col7")
                                        .setBlobValue(
                                            ByteString.copyFrom(
                                                "blob".getBytes(StandardCharsets.UTF_8)))
                                        .build())
                                .build())
                        .addResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p1")
                                        .setIntValue(10)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("p2")
                                        .setTextValue("text1")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c1")
                                        .setTextValue("text2")
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("c2")
                                        .setIntValue(20)
                                        .build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                                .addColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                                .build()))
                .build());
  }

  @Test
  public void scan_ProperArgumentsFromOldClientGiven_TransactionShouldBeCalledProperly()
      throws CrudException {
    // Arrange
    @SuppressWarnings("unchecked")
    StreamObserver<TransactionResponse> responseObserver = mock(StreamObserver.class);
    TransactionStreamObserver transactionStreamObserver =
        new TransactionStreamObserver(
            manager, tableMetadataManager, responseObserver, new Metrics(), s -> true, () -> {});

    TransactionRequest request =
        TransactionRequest.newBuilder()
            .setScanRequest(
                ScanRequest.newBuilder()
                    .setScan(
                        com.scalar.db.rpc.Scan.newBuilder()
                            .setPartitionKey(
                                com.scalar.db.rpc.Key.newBuilder()
                                    .addValue(
                                        Value.newBuilder().setName("p1").setIntValue(1).build())
                                    .build())
                            .build())
                    .build())
            .build();

    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
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
                    TABLE_METADATA)));

    transactionStreamObserver.onNext(
        TransactionRequest.newBuilder().setStartRequest(StartRequest.getDefaultInstance()).build());

    // Act
    transactionStreamObserver.onNext(request);

    // Assert
    verify(transaction).scan(any());
    verify(responseObserver)
        .onNext(
            TransactionResponse.newBuilder()
                .setScanResponse(
                    ScanResponse.newBuilder()
                        .addResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("p2")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text1").build())
                                        .build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("c1")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text2").build())
                                        .build())
                                .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .addValue(
                                    Value.newBuilder().setName("col2").setIntValue(10).build())
                                .addValue(
                                    Value.newBuilder().setName("col3").setBigintValue(100L).build())
                                .addValue(
                                    Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
                                .addValue(
                                    Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("col6")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text").build())
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
                        .addResult(
                            com.scalar.db.rpc.Result.newBuilder()
                                .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("p2")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text1").build())
                                        .build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("c1")
                                        .setTextValue(
                                            TextValue.newBuilder().setValue("text2").build())
                                        .build())
                                .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                                .addValue(
                                    Value.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(false)
                                        .build())
                                .addValue(Value.newBuilder().setName("col2").setIntValue(0).build())
                                .addValue(
                                    Value.newBuilder().setName("col3").setBigintValue(0L).build())
                                .addValue(
                                    Value.newBuilder().setName("col4").setFloatValue(0.0F).build())
                                .addValue(
                                    Value.newBuilder().setName("col5").setDoubleValue(0.0).build())
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
                                .build()))
                .build());
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
