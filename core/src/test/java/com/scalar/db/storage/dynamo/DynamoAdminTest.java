package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyResponse;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

public class DynamoAdminTest {

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String FULL_TABLE_NAME = NAMESPACE + "." + TABLE;

  @Mock private DynamoDbClient client;
  @Mock private ApplicationAutoScalingClient applicationAutoScalingClient;
  @Mock private DynamoConfig config;
  private DynamoAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
  }

  @Test
  public void
      getTableMetadata_WithoutTableMetadataNamespaceChanged_ShouldReturnCorrectTableMetadata()
          throws ExecutionException {
    getTableMetadata_ShouldReturnCorrectTableMetadata(Optional.empty());
  }

  @Test
  public void getTableMetadata_WithTableMetadataNamespaceChanged_ShouldReturnCorrectTableMetadata()
      throws ExecutionException {
    getTableMetadata_ShouldReturnCorrectTableMetadata(Optional.of("changed"));
  }

  private void getTableMetadata_ShouldReturnCorrectTableMetadata(
      Optional<String> tableMetadataNamespace) throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    Map<String, AttributeValue> expectedKey = new HashMap<>();
    expectedKey.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(FULL_TABLE_NAME).build());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item())
        .thenReturn(
            ImmutableMap.of(
                DynamoAdmin.METADATA_ATTR_TABLE,
                AttributeValue.builder().s(FULL_TABLE_NAME).build(),
                DynamoAdmin.METADATA_ATTR_COLUMNS,
                AttributeValue.builder()
                    .m(
                        ImmutableMap.of(
                            "c1",
                            AttributeValue.builder().s("int").build(),
                            "c2",
                            AttributeValue.builder().s("text").build(),
                            "c3",
                            AttributeValue.builder().s("bigint").build()))
                    .build(),
                DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build()));

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    TableMetadata actual = admin.getTableMetadata(NAMESPACE, TABLE);

    // Assert
    assertThat(actual)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addColumn("c1", DataType.INT)
                .addColumn("c2", DataType.TEXT)
                .addColumn("c3", DataType.BIGINT)
                .addPartitionKey("c1")
                .build());

    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(client).getItem(captor.capture());
    GetItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
  }

  @Test
  public void dropNamespace_WithoutTableMetadataNamespaceChanged_ShouldDropAllTablesInNamespace()
      throws ExecutionException {
    dropNamespace_ShouldDropAllTablesInNamespace(Optional.empty());
  }

  @Test
  public void dropNamespace_WithTableMetadataNamespaceChanged_ShouldDropAllTablesInNamespace()
      throws ExecutionException {
    dropNamespace_ShouldDropAllTablesInNamespace(Optional.of("changed"));
  }

  private void dropNamespace_ShouldDropAllTablesInNamespace(Optional<String> tableMetadataNamespace)
      throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables()).thenReturn(listTablesResponse);
    when(listTablesResponse.tableNames())
        .thenReturn(
            ImmutableList.<String>builder()
                .add(NAMESPACE + ".tb1")
                .add(NAMESPACE + ".tb2")
                .add(NAMESPACE + ".tb3")
                .build());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(1);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    admin.dropNamespace(NAMESPACE);

    // Assert
    verify(client, times(3)).deleteTable(any(DeleteTableRequest.class));

    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client, times(3)).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
  }

  @Test
  public void
      createTable_WithoutTableMetadataNamespaceChangedWhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable()
          throws ExecutionException {
    createTable_WhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable(Optional.empty());
  }

  @Test
  public void
      createTable_WithTableMetadataNamespaceChangedWhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable()
          throws ExecutionException {
    createTable_WhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable(Optional.of("changed"));
  }

  private void createTable_WhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable(
      Optional<String> tableMetadataNamespace) throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.INT).build();

    when(applicationAutoScalingClient.registerScalableTarget(
            any(RegisterScalableTargetRequest.class)))
        .thenReturn(RegisterScalableTargetResponse.builder().build());
    when(applicationAutoScalingClient.putScalingPolicy(any(PutScalingPolicyRequest.class)))
        .thenReturn(PutScalingPolicyResponse.builder().build());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // for the table metadata table
    describeTableResponse = mock(DescribeTableResponse.class);
    tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.class)
        .thenReturn(describeTableResponse);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata);

    // Assert
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client, times(2)).createTable(createTableRequestCaptor.capture());
    CreateTableRequest actualCreateTableRequest = createTableRequestCaptor.getValue();
    assertThat(actualCreateTableRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
  }

  @Test
  public void
      createTable_WithoutTableMetadataNamespaceChangedWhenMetadataTableExists_ShouldCreateOnlyTable()
          throws ExecutionException {
    createTable_WhenMetadataTableExists_ShouldCreateOnlyTable(Optional.empty());
  }

  @Test
  public void
      createTable_WithTableMetadataNamespaceChangedWhenMetadataTableExists_ShouldCreateOnlyTable()
          throws ExecutionException {
    createTable_WhenMetadataTableExists_ShouldCreateOnlyTable(Optional.of("changed"));
  }

  private void createTable_WhenMetadataTableExists_ShouldCreateOnlyTable(
      Optional<String> tableMetadataNamespace) throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.INT).build();

    when(applicationAutoScalingClient.registerScalableTarget(
            any(RegisterScalableTargetRequest.class)))
        .thenReturn(RegisterScalableTargetResponse.builder().build());
    when(applicationAutoScalingClient.putScalingPolicy(any(PutScalingPolicyRequest.class)))
        .thenReturn(PutScalingPolicyResponse.builder().build());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // for the table metadata table
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata);

    // Assert
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client).createTable(createTableRequestCaptor.capture());
    CreateTableRequest actualCreateTableRequest = createTableRequestCaptor.getValue();
    assertThat(actualCreateTableRequest.tableName())
        .isNotEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
  }

  @Test
  public void
      dropTable_WithoutTableMetadataNamespaceChangedWithNoMetadataLeft_ShouldDropTableAndDeleteMetadata()
          throws ExecutionException {
    dropTable_WithNoMetadataLeft_ShouldDropTableAndDeleteMetadata(Optional.empty());
  }

  @Test
  public void
      dropTable_WithTableMetadataNamespaceChangedWithNoMetadataLeft_ShouldDropTableAndDeleteMetadata()
          throws ExecutionException {
    dropTable_WithNoMetadataLeft_ShouldDropTableAndDeleteMetadata(Optional.of("changed"));
  }

  private void dropTable_WithNoMetadataLeft_ShouldDropTableAndDeleteMetadata(
      Optional<String> tableMetadataNamespace) throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    // for the table metadata table
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(1);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(client).deleteTable(deleteTableRequestCaptor.capture());
    DeleteTableRequest actualDeleteTableRequest = deleteTableRequestCaptor.getValue();
    assertThat(actualDeleteTableRequest.tableName())
        .isNotEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
  }

  @Test
  public void
      dropTable_WithoutTableMetadataNamespaceChangedWithMetadataLeft_ShouldDropTableAndDropMetadataTable()
          throws ExecutionException {
    dropTable_WithMetadataLeft_ShouldDropTableAndDropMetadataTable(Optional.empty());
  }

  @Test
  public void
      dropTable_WithTableMetadataNamespaceChangedWithMetadataLeft_ShouldDropTableAndDropMetadataTable()
          throws ExecutionException {
    dropTable_WithMetadataLeft_ShouldDropTableAndDropMetadataTable(Optional.of("changed"));
  }

  private void dropTable_WithMetadataLeft_ShouldDropTableAndDropMetadataTable(
      Optional<String> tableMetadataNamespace) throws ExecutionException {
    // Arrange
    String metadataNamespaceName = tableMetadataNamespace.orElse(DynamoAdmin.METADATA_NAMESPACE);

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    // for the table metadata table
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(0);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(client, times(2)).deleteTable(deleteTableRequestCaptor.capture());
    DeleteTableRequest actualDeleteTableRequest = deleteTableRequestCaptor.getValue();
    assertThat(actualDeleteTableRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
  }

  @Test
  public void truncateTable_WithExistingRecords_ShouldDeleteAllRecords() throws ExecutionException {
    // Arrange
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.items())
        .thenReturn(
            ImmutableList.<Map<String, AttributeValue>>builder()
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey1", AttributeValue.builder().s("pKey1Val").build())
                        .build())
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey2", AttributeValue.builder().s("pKey2Val").build())
                        .build())
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey3", AttributeValue.builder().s("pKey3Val").build())
                        .build())
                .build());
    when(scanResponse.lastEvaluatedKey()).thenReturn(Collections.emptyMap());
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    // Act
    admin.truncateTable(NAMESPACE, TABLE);

    // Assert
    verify(client, times(3)).deleteItem(any(DeleteItemRequest.class));
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnTableNamesCorrectly() throws ExecutionException {
    // Arrange
    Set<String> tableNames = ImmutableSet.of("t1", "t2");

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables()).thenReturn(listTablesResponse);
    when(listTablesResponse.tableNames())
        .thenReturn(ImmutableList.of(NAMESPACE + ".t1", NAMESPACE + ".t2"));

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(NAMESPACE);

    // Assert
    assertThat(actualNames).isEqualTo(tableNames);
  }
}
