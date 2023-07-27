package com.scalar.db.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.rpc.AddNewColumnToTableRequest;
import com.scalar.db.rpc.CoordinatorTablesExistRequest;
import com.scalar.db.rpc.CoordinatorTablesExistResponse;
import com.scalar.db.rpc.CreateCoordinatorTablesRequest;
import com.scalar.db.rpc.CreateIndexRequest;
import com.scalar.db.rpc.CreateNamespaceRequest;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DropCoordinatorTablesRequest;
import com.scalar.db.rpc.DropIndexRequest;
import com.scalar.db.rpc.DropNamespaceRequest;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesResponse;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.NamespaceExistsRequest;
import com.scalar.db.rpc.NamespaceExistsResponse;
import com.scalar.db.rpc.TruncateCoordinatorTablesRequest;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.util.ProtoUtils;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributedTransactionAdminServiceTest {

  @Mock private DistributedTransactionAdmin admin;

  private DistributedTransactionAdminService adminService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    adminService = new DistributedTransactionAdminService(admin, new Metrics());
  }

  @Test
  public void createNamespace_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CreateNamespaceRequest request =
        CreateNamespaceRequest.newBuilder().setNamespace("namespace").setIfNotExists(true).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.createNamespace(request, responseObserver);

    // Assert
    verify(admin).createNamespace(any(), anyBoolean(), anyMap());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void dropNamespace_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    DropNamespaceRequest request =
        DropNamespaceRequest.newBuilder().setNamespace("namespace").build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.dropNamespace(request, responseObserver);

    // Assert
    verify(admin).dropNamespace(any(), anyBoolean());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void createTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CreateTableRequest request =
        CreateTableRequest.newBuilder()
            .setNamespace("namespace")
            .setTable("table")
            .setTableMetadata(
                ProtoUtils.toTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addPartitionKey("col1")
                        .build()))
            .setIfNotExists(true)
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.createTable(request, responseObserver);

    // Assert
    verify(admin).createTable(any(), any(), any(), anyBoolean(), anyMap());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void dropTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    DropTableRequest request =
        DropTableRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.dropTable(request, responseObserver);

    // Assert
    verify(admin).dropTable(any(), any(), anyBoolean());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void truncateTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    TruncateTableRequest request =
        TruncateTableRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.truncateTable(request, responseObserver);

    // Assert
    verify(admin).truncateTable(any(), any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void createIndex_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CreateIndexRequest request =
        CreateIndexRequest.newBuilder()
            .setNamespace("namespace")
            .setTable("table")
            .setTable("col")
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.createIndex(request, responseObserver);

    // Assert
    verify(admin).createIndex(any(), any(), any(), anyBoolean(), anyMap());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void dropIndex_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    DropIndexRequest request =
        DropIndexRequest.newBuilder()
            .setNamespace("namespace")
            .setTable("table")
            .setTable("col")
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.dropIndex(request, responseObserver);

    // Assert
    verify(admin).dropIndex(any(), any(), any(), anyBoolean());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void getTableMetadata_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    GetTableMetadataRequest request =
        GetTableMetadataRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTableMetadataResponse> responseObserver = mock(StreamObserver.class);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .addColumn("col3", DataType.TEXT)
            .addPartitionKey("col1")
            .addClusteringKey("col2")
            .build();
    when(admin.getTableMetadata("namespace", "table")).thenReturn(tableMetadata);

    // Act
    adminService.getTableMetadata(request, responseObserver);

    // Assert
    verify(admin).getTableMetadata(any(), any());
    verify(responseObserver)
        .onNext(
            GetTableMetadataResponse.newBuilder()
                .setTableMetadata(ProtoUtils.toTableMetadata(tableMetadata))
                .build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void getNamespaceTableNames_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    GetNamespaceTableNamesRequest request =
        GetNamespaceTableNamesRequest.newBuilder().setNamespace("namespace").build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetNamespaceTableNamesResponse> responseObserver = mock(StreamObserver.class);

    when(admin.getNamespaceTableNames("namespace"))
        .thenReturn(ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // Act
    adminService.getNamespaceTableNames(request, responseObserver);

    // Assert
    verify(admin).getNamespaceTableNames(any());
    verify(responseObserver)
        .onNext(
            GetNamespaceTableNamesResponse.newBuilder()
                .addTableNames("tbl1")
                .addTableNames("tbl2")
                .addTableNames("tbl3")
                .build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void namespaceExists_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    NamespaceExistsRequest request =
        NamespaceExistsRequest.newBuilder().setNamespace("namespace").build();
    @SuppressWarnings("unchecked")
    StreamObserver<NamespaceExistsResponse> responseObserver = mock(StreamObserver.class);

    when(admin.namespaceExists("namespace")).thenReturn(true);

    // Act
    adminService.namespaceExists(request, responseObserver);

    // Assert
    verify(admin).namespaceExists(any());
    verify(responseObserver).onNext(NamespaceExistsResponse.newBuilder().setExists(true).build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void createCoordinatorTables_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CreateCoordinatorTablesRequest request =
        CreateCoordinatorTablesRequest.newBuilder().setIfNotExist(true).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.createCoordinatorTables(request, responseObserver);

    // Assert
    verify(admin).createCoordinatorTables(true, Collections.emptyMap());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void dropCoordinatorTables_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    DropCoordinatorTablesRequest request =
        DropCoordinatorTablesRequest.newBuilder().setIfExist(true).build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.dropCoordinatorTables(request, responseObserver);

    // Assert
    verify(admin).dropCoordinatorTables(true);
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void truncateCoordinatorTables_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    TruncateCoordinatorTablesRequest request =
        TruncateCoordinatorTablesRequest.getDefaultInstance();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.truncateCoordinatorTables(request, responseObserver);

    // Assert
    verify(admin).truncateCoordinatorTables();
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void coordinatorTablesExist_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CoordinatorTablesExistRequest request = CoordinatorTablesExistRequest.getDefaultInstance();
    @SuppressWarnings("unchecked")
    StreamObserver<CoordinatorTablesExistResponse> responseObserver = mock(StreamObserver.class);

    when(admin.coordinatorTablesExist()).thenReturn(true);

    // Act
    adminService.coordinatorTablesExist(request, responseObserver);

    // Assert
    verify(admin).coordinatorTablesExist();
    verify(responseObserver)
        .onNext(CoordinatorTablesExistResponse.newBuilder().setExist(true).build());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void addNewColumnToTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    String column = "c1";

    AddNewColumnToTableRequest request =
        AddNewColumnToTableRequest.newBuilder()
            .setNamespace(namespace)
            .setTable(table)
            .setColumnName(column)
            .setColumnType(com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.addNewColumnToTable(request, responseObserver);

    // Assert
    verify(admin).addNewColumnToTable(namespace, table, column, DataType.TEXT);
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }
}
