package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsDescription;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsStatus;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;

public class DynamoAdminTest {

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String FULL_TABLE_NAME = NAMESPACE + "." + TABLE;

  @Mock private DynamoDbClient client;
  @Mock private ApplicationAutoScalingClient applicationAutoScalingClient;
  @Mock private DynamoConfig config;

  private DynamoAdmin admin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
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
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(FULL_TABLE_NAME).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_COLUMNS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c1", AttributeValue.builder().s("text").build())
                                .put("c2", AttributeValue.builder().s("bigint").build())
                                .put("c3", AttributeValue.builder().s("boolean").build())
                                .put("c4", AttributeValue.builder().s("blob").build())
                                .put("c5", AttributeValue.builder().s("int").build())
                                .put("c6", AttributeValue.builder().s("double").build())
                                .put("c7", AttributeValue.builder().s("float").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                    AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_KEY,
                    AttributeValue.builder()
                        .l(
                            AttributeValue.builder().s("c2").build(),
                            AttributeValue.builder().s("c3").build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_ORDERS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c2", AttributeValue.builder().s("DESC").build())
                                .put("c3", AttributeValue.builder().s("ASC").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_SECONDARY_INDEX,
                    AttributeValue.builder().ss("c4").build())
                .build());

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
                .addPartitionKey("c1")
                .addClusteringKey("c2", Order.DESC)
                .addClusteringKey("c3", Order.ASC)
                .addColumn("c1", DataType.TEXT)
                .addColumn("c2", DataType.BIGINT)
                .addColumn("c3", DataType.BOOLEAN)
                .addColumn("c4", DataType.BLOB)
                .addColumn("c5", DataType.INT)
                .addColumn("c6", DataType.DOUBLE)
                .addColumn("c7", DataType.FLOAT)
                .addSecondaryIndex("c4")
                .build());

    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(client).getItem(captor.capture());
    GetItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
    assertThat(actualRequest.consistentRead()).isTrue();
  }

  @Test
  public void dropNamespace_ShouldDropAllTablesInNamespace() throws ExecutionException {
    // Arrange
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames())
        .thenReturn(
            ImmutableList.<String>builder()
                .add(NAMESPACE + ".tb1")
                .add(NAMESPACE + ".tb2")
                .add(NAMESPACE + ".tb3")
                .build())
        .thenReturn(Collections.emptyList());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(1);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    admin = spy(admin);

    // Act
    admin.dropNamespace(NAMESPACE);

    // Assert
    verify(admin).dropTable(NAMESPACE, "tb1");
    verify(admin).dropTable(NAMESPACE, "tb2");
    verify(admin).dropTable(NAMESPACE, "tb3");
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
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c2", Order.ASC)
            .addClusteringKey("c3", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c4")
            .build();

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(DescribeTableRequest.builder().tableName(FULL_TABLE_NAME).build()))
        .thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    DescribeContinuousBackupsResponse describeContinuousBackupsResponse =
        mock(DescribeContinuousBackupsResponse.class);
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(describeContinuousBackupsResponse);
    ContinuousBackupsDescription continuousBackupsDescription =
        mock(ContinuousBackupsDescription.class);
    when(describeContinuousBackupsResponse.continuousBackupsDescription())
        .thenReturn(continuousBackupsDescription);
    when(continuousBackupsDescription.continuousBackupsStatus())
        .thenReturn(ContinuousBackupsStatus.ENABLED);

    // for the table metadata table
    describeTableResponse = mock(DescribeTableResponse.class);
    tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);
    when(client.describeTable(
            DescribeTableRequest.builder()
                .tableName(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE)
                .build()))
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
    List<CreateTableRequest> actualCreateTableRequests = createTableRequestCaptor.getAllValues();

    List<AttributeDefinition> attributeDefinitions =
        actualCreateTableRequests.get(0).attributeDefinitions();
    assertThat(attributeDefinitions.size()).isEqualTo(3);
    assertThat(attributeDefinitions.get(0).attributeName()).isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(attributeDefinitions.get(0).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(1).attributeName()).isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(attributeDefinitions.get(1).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(2).attributeName()).isEqualTo("c4");
    assertThat(attributeDefinitions.get(2).attributeType()).isEqualTo(ScalarAttributeType.B);

    assertThat(actualCreateTableRequests.get(0).keySchema().size()).isEqualTo(2);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(0).attributeName())
        .isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(0).keyType())
        .isEqualTo(KeyType.HASH);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(1).attributeName())
        .isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(1).keyType())
        .isEqualTo(KeyType.RANGE);

    assertThat(actualCreateTableRequests.get(0).globalSecondaryIndexes().size()).isEqualTo(1);
    assertThat(actualCreateTableRequests.get(0).globalSecondaryIndexes().get(0).indexName())
        .isEqualTo(FULL_TABLE_NAME + ".global_index.c4");
    assertThat(actualCreateTableRequests.get(0).globalSecondaryIndexes().get(0).keySchema().size())
        .isEqualTo(1);
    assertThat(
            actualCreateTableRequests
                .get(0)
                .globalSecondaryIndexes()
                .get(0)
                .keySchema()
                .get(0)
                .attributeName())
        .isEqualTo("c4");
    assertThat(
            actualCreateTableRequests
                .get(0)
                .globalSecondaryIndexes()
                .get(0)
                .keySchema()
                .get(0)
                .keyType())
        .isEqualTo(KeyType.HASH);
    assertThat(
            actualCreateTableRequests
                .get(0)
                .globalSecondaryIndexes()
                .get(0)
                .projection()
                .projectionType())
        .isEqualTo(ProjectionType.ALL);
    assertThat(
            actualCreateTableRequests
                .get(0)
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .readCapacityUnits())
        .isEqualTo(10);
    assertThat(
            actualCreateTableRequests
                .get(0)
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .writeCapacityUnits())
        .isEqualTo(10);

    assertThat(actualCreateTableRequests.get(0).provisionedThroughput().writeCapacityUnits())
        .isEqualTo(10);
    assertThat(actualCreateTableRequests.get(0).provisionedThroughput().readCapacityUnits())
        .isEqualTo(10);

    assertThat(actualCreateTableRequests.get(0).tableName()).isEqualTo(FULL_TABLE_NAME);

    // for the table metadata table
    attributeDefinitions = actualCreateTableRequests.get(1).attributeDefinitions();
    assertThat(attributeDefinitions.size()).isEqualTo(1);
    assertThat(attributeDefinitions.get(0).attributeName())
        .isEqualTo(DynamoAdmin.METADATA_ATTR_TABLE);
    assertThat(attributeDefinitions.get(0).attributeType()).isEqualTo(ScalarAttributeType.S);

    assertThat(actualCreateTableRequests.get(1).keySchema().size()).isEqualTo(1);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(0).attributeName())
        .isEqualTo(DynamoAdmin.METADATA_ATTR_TABLE);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(0).keyType())
        .isEqualTo(KeyType.HASH);

    assertThat(actualCreateTableRequests.get(1).provisionedThroughput().writeCapacityUnits())
        .isEqualTo(1);
    assertThat(actualCreateTableRequests.get(1).provisionedThroughput().readCapacityUnits())
        .isEqualTo(1);

    assertThat(actualCreateTableRequests.get(1).tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(FULL_TABLE_NAME).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put("c1", AttributeValue.builder().s("text").build());
    columns.put("c2", AttributeValue.builder().s("bigint").build());
    columns.put("c3", AttributeValue.builder().s("boolean").build());
    columns.put("c4", AttributeValue.builder().s("blob").build());
    columns.put("c5", AttributeValue.builder().s("int").build());
    columns.put("c6", AttributeValue.builder().s("double").build());
    columns.put("c7", AttributeValue.builder().s("float").build());
    itemValues.put(DynamoAdmin.METADATA_ATTR_COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
        AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_CLUSTERING_KEY,
        AttributeValue.builder()
            .l(AttributeValue.builder().s("c2").build(), AttributeValue.builder().s("c3").build())
            .build());
    Map<String, AttributeValue> clusteringOrders = new HashMap<>();
    clusteringOrders.put("c2", AttributeValue.builder().s("ASC").build());
    clusteringOrders.put("c3", AttributeValue.builder().s("ASC").build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_CLUSTERING_ORDERS,
        AttributeValue.builder().m(clusteringOrders).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_SECONDARY_INDEX, AttributeValue.builder().ss("c4").build());
    assertThat(actualPutItemRequest.item()).isEqualTo(itemValues);

    verify(applicationAutoScalingClient, times(4))
        .registerScalableTarget(any(RegisterScalableTargetRequest.class));
    verify(applicationAutoScalingClient, times(4))
        .putScalingPolicy(any(PutScalingPolicyRequest.class));
    verify(client).updateContinuousBackups(any(UpdateContinuousBackupsRequest.class));
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
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c2", Order.DESC)
            .addClusteringKey("c3", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.INT)
            .addColumn("c5", DataType.BLOB)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c4")
            .build();

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    DescribeContinuousBackupsResponse describeContinuousBackupsResponse =
        mock(DescribeContinuousBackupsResponse.class);
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(describeContinuousBackupsResponse);
    ContinuousBackupsDescription continuousBackupsDescription =
        mock(ContinuousBackupsDescription.class);
    when(describeContinuousBackupsResponse.continuousBackupsDescription())
        .thenReturn(continuousBackupsDescription);
    when(continuousBackupsDescription.continuousBackupsStatus())
        .thenReturn(ContinuousBackupsStatus.ENABLED);

    if (tableMetadataNamespace.isPresent()) {
      when(config.getTableMetadataNamespace()).thenReturn(tableMetadataNamespace);
      admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
    }

    Map<String, String> options = new HashMap<>();
    options.put(DynamoAdmin.REQUEST_UNIT, "100");
    options.put(DynamoAdmin.NO_SCALING, "true");
    options.put(DynamoAdmin.NO_BACKUP, "true");

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata, options);

    // Assert
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client).createTable(createTableRequestCaptor.capture());
    CreateTableRequest actualCreateTableRequest = createTableRequestCaptor.getValue();

    List<AttributeDefinition> attributeDefinitions =
        actualCreateTableRequest.attributeDefinitions();
    assertThat(attributeDefinitions.size()).isEqualTo(3);
    assertThat(attributeDefinitions.get(0).attributeName()).isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(attributeDefinitions.get(0).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(1).attributeName()).isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(attributeDefinitions.get(1).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(2).attributeName()).isEqualTo("c4");
    assertThat(attributeDefinitions.get(2).attributeType()).isEqualTo(ScalarAttributeType.N);

    assertThat(actualCreateTableRequest.keySchema().size()).isEqualTo(2);
    assertThat(actualCreateTableRequest.keySchema().get(0).attributeName())
        .isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(actualCreateTableRequest.keySchema().get(0).keyType()).isEqualTo(KeyType.HASH);
    assertThat(actualCreateTableRequest.keySchema().get(1).attributeName())
        .isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(actualCreateTableRequest.keySchema().get(1).keyType()).isEqualTo(KeyType.RANGE);

    assertThat(actualCreateTableRequest.globalSecondaryIndexes().size()).isEqualTo(1);
    assertThat(actualCreateTableRequest.globalSecondaryIndexes().get(0).indexName())
        .isEqualTo(FULL_TABLE_NAME + ".global_index.c4");
    assertThat(actualCreateTableRequest.globalSecondaryIndexes().get(0).keySchema().size())
        .isEqualTo(1);
    assertThat(
            actualCreateTableRequest
                .globalSecondaryIndexes()
                .get(0)
                .keySchema()
                .get(0)
                .attributeName())
        .isEqualTo("c4");
    assertThat(
            actualCreateTableRequest.globalSecondaryIndexes().get(0).keySchema().get(0).keyType())
        .isEqualTo(KeyType.HASH);
    assertThat(
            actualCreateTableRequest.globalSecondaryIndexes().get(0).projection().projectionType())
        .isEqualTo(ProjectionType.ALL);
    assertThat(
            actualCreateTableRequest
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .readCapacityUnits())
        .isEqualTo(100);
    assertThat(
            actualCreateTableRequest
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .writeCapacityUnits())
        .isEqualTo(100);

    assertThat(actualCreateTableRequest.provisionedThroughput().writeCapacityUnits())
        .isEqualTo(100);
    assertThat(actualCreateTableRequest.provisionedThroughput().readCapacityUnits()).isEqualTo(100);

    assertThat(actualCreateTableRequest.tableName()).isEqualTo(FULL_TABLE_NAME);

    // for the table metadata table
    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(FULL_TABLE_NAME).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put("c1", AttributeValue.builder().s("text").build());
    columns.put("c2", AttributeValue.builder().s("bigint").build());
    columns.put("c3", AttributeValue.builder().s("boolean").build());
    columns.put("c4", AttributeValue.builder().s("int").build());
    columns.put("c5", AttributeValue.builder().s("blob").build());
    columns.put("c6", AttributeValue.builder().s("double").build());
    columns.put("c7", AttributeValue.builder().s("float").build());
    itemValues.put(DynamoAdmin.METADATA_ATTR_COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
        AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_CLUSTERING_KEY,
        AttributeValue.builder()
            .l(AttributeValue.builder().s("c2").build(), AttributeValue.builder().s("c3").build())
            .build());
    Map<String, AttributeValue> clusteringOrders = new HashMap<>();
    clusteringOrders.put("c2", AttributeValue.builder().s("DESC").build());
    clusteringOrders.put("c3", AttributeValue.builder().s("ASC").build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_CLUSTERING_ORDERS,
        AttributeValue.builder().m(clusteringOrders).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_SECONDARY_INDEX, AttributeValue.builder().ss("c4").build());
    assertThat(actualPutItemRequest.item()).isEqualTo(itemValues);

    verify(applicationAutoScalingClient, never())
        .registerScalableTarget(any(RegisterScalableTargetRequest.class));
    verify(applicationAutoScalingClient, never())
        .putScalingPolicy(any(PutScalingPolicyRequest.class));
    verify(client, never()).updateContinuousBackups(any(UpdateContinuousBackupsRequest.class));
  }

  @Test
  public void
      createTable_tableMetadataWithPartitionKeyWithNonLastBlobValueGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addPartitionKey("c2")
            .addColumn("c1", DataType.BLOB)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.INT)
            .build();

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(NAMESPACE, TABLE, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      createTable_tableMetadataWithClusteringKeyWithBlobValueGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c2")
            .addClusteringKey("c3")
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.TEXT)
            .build();

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(NAMESPACE, TABLE, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      createTable_tableMetadataWithBooleanSecondaryIndexGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.BOOLEAN)
            .addSecondaryIndex("c2")
            .build();

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(NAMESPACE, TABLE, metadata))
        .isInstanceOf(IllegalArgumentException.class);
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
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(FULL_TABLE_NAME).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_COLUMNS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c1", AttributeValue.builder().s("text").build())
                                .put("c2", AttributeValue.builder().s("bigint").build())
                                .put("c3", AttributeValue.builder().s("boolean").build())
                                .put("c4", AttributeValue.builder().s("int").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                    AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_ORDERS,
                    AttributeValue.builder()
                        .l(
                            AttributeValue.builder().s("c2").build(),
                            AttributeValue.builder().s("c3").build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_KEY,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c2", AttributeValue.builder().s("DESC").build())
                                .put("c3", AttributeValue.builder().s("ASC").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_SECONDARY_INDEX,
                    AttributeValue.builder().ss("c4").build())
                .build());

    // for the table metadata table
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(1);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    List<String> tableList = Collections.emptyList();
    when(listTablesResponse.tableNames()).thenReturn(tableList);

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
    assertThat(actualDeleteTableRequest.tableName()).isEqualTo(FULL_TABLE_NAME);

    // for the table metadata table
    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
    assertThat(actualDeleteItemRequest.key().get(DynamoAdmin.METADATA_ATTR_TABLE).s())
        .isEqualTo(FULL_TABLE_NAME);

    verify(applicationAutoScalingClient, times(4))
        .deleteScalingPolicy(any(DeleteScalingPolicyRequest.class));
    verify(applicationAutoScalingClient, times(4))
        .deregisterScalableTarget(any(DeregisterScalableTargetRequest.class));
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
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(FULL_TABLE_NAME).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_COLUMNS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c1", AttributeValue.builder().s("text").build())
                                .put("c2", AttributeValue.builder().s("bigint").build())
                                .put("c3", AttributeValue.builder().s("boolean").build())
                                .put("c4", AttributeValue.builder().s("int").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                    AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_KEY,
                    AttributeValue.builder()
                        .l(
                            AttributeValue.builder().s("c2").build(),
                            AttributeValue.builder().s("c3").build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_CLUSTERING_ORDERS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c2", AttributeValue.builder().s("DESC").build())
                                .put("c3", AttributeValue.builder().s("ASC").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_SECONDARY_INDEX,
                    AttributeValue.builder().ss("c4").build())
                .build());

    // for the table metadata table
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(0);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    List<String> tableList = Collections.emptyList();
    when(listTablesResponse.tableNames()).thenReturn(tableList);

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
    List<DeleteTableRequest> actualDeleteTableRequests = deleteTableRequestCaptor.getAllValues();

    assertThat(actualDeleteTableRequests.get(0).tableName()).isEqualTo(FULL_TABLE_NAME);

    // for the table metadata table
    assertThat(actualDeleteTableRequests.get(1).tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);

    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName())
        .isEqualTo(metadataNamespaceName + "." + DynamoAdmin.METADATA_TABLE);
    assertThat(actualDeleteItemRequest.key().get(DynamoAdmin.METADATA_ATTR_TABLE).s())
        .isEqualTo(FULL_TABLE_NAME);

    verify(applicationAutoScalingClient, times(4))
        .deleteScalingPolicy(any(DeleteScalingPolicyRequest.class));
    verify(applicationAutoScalingClient, times(4))
        .deregisterScalableTarget(any(DeregisterScalableTargetRequest.class));
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
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames())
        .thenReturn(ImmutableList.of(NAMESPACE + ".t1", NAMESPACE + ".t2"));

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(NAMESPACE);

    // Assert
    assertThat(actualNames).isEqualTo(tableNames);
  }
}
