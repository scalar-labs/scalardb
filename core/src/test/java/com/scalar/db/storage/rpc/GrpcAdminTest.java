package com.scalar.db.storage.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.LazyStringArrayList;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.rpc.AddNewColumnToTableRequest;
import com.scalar.db.rpc.CreateIndexRequest;
import com.scalar.db.rpc.CreateNamespaceRequest;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.DropIndexRequest;
import com.scalar.db.rpc.DropNamespaceRequest;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesResponse;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.NamespaceExistsRequest;
import com.scalar.db.rpc.NamespaceExistsResponse;
import com.scalar.db.rpc.RepairTableRequest;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.util.ProtoUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcAdminTest {

  @Mock private GrpcConfig config;
  @Mock private DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub;

  private GrpcAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    admin = new GrpcAdmin(stub, config);
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
  }

  @Test
  public void createNamespace_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createNamespace(namespace, true, options);

    // Assert
    verify(stub)
        .createNamespace(
            CreateNamespaceRequest.newBuilder()
                .setNamespace(namespace)
                .putAllOptions(options)
                .setIfNotExists(true)
                .build());
  }

  @Test
  public void dropNamespace_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";

    // Act
    admin.dropNamespace(namespace, true);

    // Assert
    verify(stub)
        .dropNamespace(
            DropNamespaceRequest.newBuilder().setNamespace(namespace).setIfExists(true).build());
  }

  @Test
  public void createTable_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .addPartitionKey("col1")
            .build();
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createTable(namespace, table, metadata, true, options);

    // Assert
    verify(stub)
        .createTable(
            CreateTableRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setTableMetadata(ProtoUtils.toTableMetadata(metadata))
                .setIfNotExists(true)
                .putAllOptions(options)
                .build());
  }

  @Test
  public void dropTable_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";

    // Act
    admin.dropTable(namespace, table, true);

    // Assert
    verify(stub)
        .dropTable(
            DropTableRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setIfExists(true)
                .build());
  }

  @Test
  public void createIndex_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    String column = "col";
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createIndex(namespace, table, column, true, options);

    // Assert
    verify(stub)
        .createIndex(
            CreateIndexRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setColumnName(column)
                .setIfNotExists(true)
                .putAllOptions(options)
                .build());
  }

  @Test
  public void dropIndex_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    String column = "col";

    // Act
    admin.dropIndex(namespace, table, column, true);

    // Assert
    verify(stub)
        .dropIndex(
            DropIndexRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setColumnName(column)
                .setIfExists(true)
                .build());
  }

  @Test
  public void truncateTable_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(stub)
        .truncateTable(
            TruncateTableRequest.newBuilder().setNamespace(namespace).setTable(table).build());
  }

  @Test
  public void getTableMetadata_CalledWithProperArguments_MetadataManagerShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    GetTableMetadataResponse response = mock(GetTableMetadataResponse.class);
    when(stub.getTableMetadata(any())).thenReturn(response);
    when(response.hasTableMetadata()).thenReturn(true);
    when(response.getTableMetadata())
        .thenReturn(
            com.scalar.db.rpc.TableMetadata.newBuilder()
                .putColumns("col1", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col2", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col3", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .addPartitionKeyNames("col1")
                .addClusteringKeyNames("col2")
                .putClusteringOrders("col2", com.scalar.db.rpc.Order.ORDER_DESC)
                .build());

    // Act
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, table);

    // Assert
    verify(stub)
        .getTableMetadata(
            GetTableMetadataRequest.newBuilder().setNamespace(namespace).setTable(table).build());
    assertThat(tableMetadata)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addColumn("col1", DataType.INT)
                .addColumn("col2", DataType.INT)
                .addColumn("col3", DataType.INT)
                .addPartitionKey("col1")
                .addClusteringKey("col2", Scan.Ordering.Order.DESC)
                .build());
  }

  @Test
  public void getNamespaceTableNames_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    GetNamespaceTableNamesResponse response = mock(GetNamespaceTableNamesResponse.class);
    when(stub.getNamespaceTableNames(any())).thenReturn(response);

    LazyStringArrayList tableNames = new LazyStringArrayList();
    tableNames.add("tbl1");
    tableNames.add("tbl2");
    tableNames.add("tbl3");
    when(response.getTableNamesList()).thenReturn(tableNames);

    // Act
    Set<String> results = admin.getNamespaceTableNames(namespace);

    // Assert
    verify(stub)
        .getNamespaceTableNames(
            GetNamespaceTableNamesRequest.newBuilder().setNamespace(namespace).build());
    assertThat(results).isEqualTo(ImmutableSet.of("tbl1", "tbl2", "tbl3"));
  }

  @Test
  public void namespaceExists_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    NamespaceExistsResponse response = mock(NamespaceExistsResponse.class);
    when(stub.namespaceExists(any())).thenReturn(response);
    when(response.getExists()).thenReturn(false);

    // Act
    boolean result = admin.namespaceExists(namespace);

    // Assert
    verify(stub)
        .namespaceExists(NamespaceExistsRequest.newBuilder().setNamespace(namespace).build());
    assertThat(result).isFalse();
  }

  @Test
  public void repairTable_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .addPartitionKey("col1")
            .build();
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.repairTable(namespace, table, metadata, options);

    // Assert
    verify(stub)
        .repairTable(
            RepairTableRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setTableMetadata(ProtoUtils.toTableMetadata(metadata))
                .putAllOptions(options)
                .build());
  }

  @Test
  public void addNewColumnToTable_CalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    String columnName = "c1";

    // Act
    admin.addNewColumnToTable(namespace, table, columnName, DataType.TEXT);

    // Assert
    verify(stub)
        .addNewColumnToTable(
            AddNewColumnToTableRequest.newBuilder()
                .setNamespace(namespace)
                .setTable(table)
                .setColumnName(columnName)
                .setColumnType(com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .build());
  }

  @Test
  public void unsupportedOperations_ShouldThrowUnsupportedException() {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String column = "col";

    // Act Assert
    assertThatThrownBy(() -> admin.getImportTableMetadata(namespace, table))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> admin.addRawColumnToTable(namespace, table, column, DataType.INT))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> admin.importTable(namespace, table))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> admin.getNamespaceNames())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
