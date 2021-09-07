package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Get;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.util.Utility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DynamoTableMetadataManagerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_COLUMN_NAME_1 = "val1";
  private static final String ANY_COLUMN_NAME_2 = "val2";
  private static final String FULLNAME = ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME;

  private DynamoTableMetadataManager manager;
  private Map<String, AttributeValue> metadataMap;
  @Mock private DynamoDbClient client;
  @Mock private GetItemResponse response;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    manager = new DynamoTableMetadataManager(client, Optional.empty());
    setMetadataMap();
  }

  private void setMetadataMap() {
    metadataMap = new HashMap<>();
    metadataMap.put(
        "partitionKey",
        AttributeValue.builder().l(AttributeValue.builder().s(ANY_NAME_1).build()).build());
    metadataMap.put("clusteringKey", AttributeValue.builder().l(new ArrayList<>()).build());
    metadataMap.put("sortKey", AttributeValue.builder().s(null).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(ANY_NAME_1, AttributeValue.builder().s("text").build());
    columns.put(ANY_NAME_2, AttributeValue.builder().s("text").build());
    columns.put(ANY_COLUMN_NAME_1, AttributeValue.builder().s("boolean").build());
    columns.put(ANY_COLUMN_NAME_2, AttributeValue.builder().s("int").build());
    metadataMap.put("columns", AttributeValue.builder().m(columns).build());
  }

  @Test
  public void getTableMetadata_ProperOperationGivenFirst_ShouldCallGetItem() {
    // Arrange
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(metadataMap);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    Map<String, AttributeValue> expectedKey = new HashMap<>();
    expectedKey.put("table", AttributeValue.builder().s(FULLNAME).build());

    // Act
    manager.getTableMetadata(get);

    // Assert
    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(client).getItem(captor.capture());
    GetItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.tableName()).isEqualTo("scalardb.metadata");
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
  }

  @Test
  public void getTableMetadata_ProperOperationGivenFirstWithPrefix_ShouldCallGetItem() {
    // Arrange
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(metadataMap);
    manager = new DynamoTableMetadataManager(client, Optional.of("prefix_"));

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    Map<String, AttributeValue> expectedKey = new HashMap<>();
    expectedKey.put("table", AttributeValue.builder().s(FULLNAME).build());

    // Act
    manager.getTableMetadata(get);

    // Assert
    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(client).getItem(captor.capture());
    GetItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.tableName()).isEqualTo("prefix_scalardb.metadata");
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
  }

  @Test
  public void getTableMetadata_SameTableGiven_ShouldCallReadItemOnce() {
    // Arrange
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(metadataMap);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get1 = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    Key partitionKey2 = new Key(ANY_NAME_1, ANY_TEXT_2);
    Get get2 = new Get(partitionKey2).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act
    manager.getTableMetadata(get1);
    manager.getTableMetadata(get2);

    verify(client, times(1)).getItem(any(GetItemRequest.class));
  }

  @Test
  public void getTableMetadata_OperationWithoutTableGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(metadataMap);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME);

    // Act Assert
    assertThatThrownBy(() -> manager.getTableMetadata(get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getTableMetadata_CosmosExceptionThrown_ShouldThrowStorageRuntimeException() {
    // Arrange
    DynamoDbException toThrow = mock(DynamoDbException.class);
    doThrow(toThrow).when(client).getItem(any(GetItemRequest.class));

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act Assert
    assertThatThrownBy(() -> manager.getTableMetadata(get))
        .isInstanceOf(StorageRuntimeException.class)
        .hasCause(toThrow);
  }

  @Test
  public void addTableMetadata_WithMetaTableNotExist_ShouldExecuteCreateTable()
      throws StorageRuntimeException {
    // Arrange
    String namespace = "ns";
    String table = "tb";

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(listTablesResponse.tableNames()).thenReturn(Collections.emptyList());
    when(client.listTables()).thenReturn(listTablesResponse);

    // Act
    manager.addTableMetadata(namespace, table, tableMetadata);

    // Assert
    verify(client).createTable(any(CreateTableRequest.class));
  }

  @Test
  public void addTableMetadata_WithCorrectParams_ShouldExecutePutItem()
      throws StorageRuntimeException {
    // Arrange
    String namespace = "ns";
    String table = "tb";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.BLOB)
            .addPartitionKey("c1")
            .addClusteringKey("c2")
            .build();
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.tableNames())
        .thenReturn(
            ImmutableList.<String>builder()
                .add(Utility.getFullTableName(Optional.empty(), namespace, table))
                .build());
    when(client.listTables()).thenReturn(listTablesResponse);

    // Act
    manager.addTableMetadata(namespace, table, tableMetadata);

    // Assert
    verify(client).putItem(any(PutItemRequest.class));
  }

  @Test
  public void deleteTableMetadata_WithCorrectParams_ShouldExecuteDeleteItem()
      throws StorageRuntimeException {
    // Arrange
    String namespace = "ns";
    String table = "tb";
    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableDescription.itemCount()).thenReturn((long) 1);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    // Act
    manager.deleteTableMetadata(namespace, table);

    // Assert
    verify(client).deleteItem(any(DeleteItemRequest.class));
  }

  @Test
  public void deleteTableMetadata_WithEmptyTableAfterDeletion_ShouldExecuteDeleteTable()
      throws StorageRuntimeException {
    // Arrange
    String namespace = "ns";
    String table = "tb";
    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableDescription.itemCount()).thenReturn((long) 0);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    // Act
    manager.deleteTableMetadata(namespace, table);

    // Assert
    verify(client).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void getTableNames_WithExistingTables_ShouldReturnTableNames() {
    // Arrange
    String namespace = "ns";
    List<String> tableFullNames =
        ImmutableList.<String>builder().add("ns.tb1").add("ns.tb2").add("ns.tb3").build();

    Set<String> tableSetExpected = new HashSet<>(tableFullNames);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(listTablesResponse.tableNames()).thenReturn(tableFullNames);
    when(client.listTables()).thenReturn(listTablesResponse);

    // Act
    Set<String> tableSet = manager.getTableNames(namespace);

    // Assert
    assertThat(tableSet).isEqualTo(tableSetExpected);
  }
}
