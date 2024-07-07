package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
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
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

/**
 * Abstraction that defines unit tests for the {@link DynamoAdmin}. The class purpose is to be able
 * to run the {@link DynamoAdmin} unit tests with different values for the {@link DynamoConfig},
 * notably {@link DynamoConfig#NAMESPACE_PREFIX} and {@link DynamoConfig#TABLE_METADATA_NAMESPACE}
 */
public abstract class DynamoAdminTestBase {

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";

  @Mock private DynamoConfig config;
  @Mock private DynamoDbClient client;
  @Mock private ApplicationAutoScalingClient applicationAutoScalingClient;
  @Mock private DescribeTableResponse tableIsActiveResponse;
  private DynamoAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.getNamespacePrefix()).thenReturn(getNamespacePrefixConfig());
    when(config.getTableMetadataNamespace()).thenReturn(getTableMetadataNamespaceConfig());

    admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
  }

  /**
   * This sets the {@link DynamoConfig#TABLE_METADATA_NAMESPACE} value that will be used to run the
   * tests
   *
   * @return {@link DynamoConfig#TABLE_METADATA_NAMESPACE} value
   */
  abstract Optional<String> getTableMetadataNamespaceConfig();

  /**
   * This sets the {@link DynamoConfig#NAMESPACE_PREFIX} value that will be used to run the tests
   *
   * @return {@link DynamoConfig#NAMESPACE_PREFIX} value
   */
  abstract Optional<String> getNamespacePrefixConfig();

  private String getFullTableName() {
    return getNamespacePrefixConfig().orElse("") + NAMESPACE + "." + TABLE;
  }

  private String getPrefixedNamespace() {
    return getNamespacePrefixConfig().orElse("") + NAMESPACE;
  }

  private String getFullMetadataTableName() {
    return getNamespacePrefixConfig().orElse("")
        + getTableMetadataNamespaceConfig().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME)
        + "."
        + DynamoAdmin.METADATA_TABLE;
  }

  @Test
  public void getTableMetadata_ShouldReturnCorrectTableMetadata() throws ExecutionException {
    // Arrange
    Map<String, AttributeValue> expectedKey = new HashMap<>();
    expectedKey.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
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
    assertThat(actualRequest.tableName()).isEqualTo(getFullMetadataTableName());
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
    assertThat(actualRequest.consistentRead()).isTrue();
  }

  // https://github.com/scalar-labs/scalardb/issues/784
  @Test
  public void namespaceExists_ShouldPerformExactMatch() throws ExecutionException {
    // Arrange
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames())
        .thenReturn(ImmutableList.<String>builder().add(getFullTableName()).build());

    // Act
    // Assert
    assertThat(admin.namespaceExists(NAMESPACE)).isTrue();
    // compare with namespace prefix
    assertThat(admin.namespaceExists(NAMESPACE.substring(0, NAMESPACE.length() - 1))).isFalse();
  }

  @Test
  public void namespaceExists_WithMetadataNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThat(
            admin.namespaceExists(
                getTableMetadataNamespaceConfig()
                    .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME)))
        .isTrue();
  }

  @Test
  public void createTable_WhenMetadataTableNotExist_ShouldCreateTableAndMetadataTable()
      throws ExecutionException {
    // Arrange
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
    when(client.describeTable(DescribeTableRequest.builder().tableName(getFullTableName()).build()))
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
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build()))
        .thenThrow(ResourceNotFoundException.class)
        .thenReturn(describeTableResponse);

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata);

    // Assert
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client, times(2)).createTable(createTableRequestCaptor.capture());
    List<CreateTableRequest> actualCreateTableRequests = createTableRequestCaptor.getAllValues();

    List<AttributeDefinition> attributeDefinitions =
        actualCreateTableRequests.get(1).attributeDefinitions();
    assertThat(attributeDefinitions.size()).isEqualTo(3);
    assertThat(attributeDefinitions.get(0).attributeName()).isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(attributeDefinitions.get(0).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(1).attributeName()).isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(attributeDefinitions.get(1).attributeType()).isEqualTo(ScalarAttributeType.B);
    assertThat(attributeDefinitions.get(2).attributeName()).isEqualTo("c4");
    assertThat(attributeDefinitions.get(2).attributeType()).isEqualTo(ScalarAttributeType.B);

    assertThat(actualCreateTableRequests.get(1).keySchema().size()).isEqualTo(2);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(0).attributeName())
        .isEqualTo(DynamoAdmin.PARTITION_KEY);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(0).keyType())
        .isEqualTo(KeyType.HASH);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(1).attributeName())
        .isEqualTo(DynamoAdmin.CLUSTERING_KEY);
    assertThat(actualCreateTableRequests.get(1).keySchema().get(1).keyType())
        .isEqualTo(KeyType.RANGE);

    assertThat(actualCreateTableRequests.get(1).globalSecondaryIndexes().size()).isEqualTo(1);
    assertThat(actualCreateTableRequests.get(1).globalSecondaryIndexes().get(0).indexName())
        .isEqualTo(getFullTableName() + ".global_index.c4");
    assertThat(actualCreateTableRequests.get(1).globalSecondaryIndexes().get(0).keySchema().size())
        .isEqualTo(1);
    assertThat(
            actualCreateTableRequests
                .get(1)
                .globalSecondaryIndexes()
                .get(0)
                .keySchema()
                .get(0)
                .attributeName())
        .isEqualTo("c4");
    assertThat(
            actualCreateTableRequests
                .get(1)
                .globalSecondaryIndexes()
                .get(0)
                .keySchema()
                .get(0)
                .keyType())
        .isEqualTo(KeyType.HASH);
    assertThat(
            actualCreateTableRequests
                .get(1)
                .globalSecondaryIndexes()
                .get(0)
                .projection()
                .projectionType())
        .isEqualTo(ProjectionType.ALL);
    assertThat(
            actualCreateTableRequests
                .get(1)
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .readCapacityUnits())
        .isEqualTo(10);
    assertThat(
            actualCreateTableRequests
                .get(1)
                .globalSecondaryIndexes()
                .get(0)
                .provisionedThroughput()
                .writeCapacityUnits())
        .isEqualTo(10);

    assertThat(actualCreateTableRequests.get(1).provisionedThroughput().writeCapacityUnits())
        .isEqualTo(10);
    assertThat(actualCreateTableRequests.get(1).provisionedThroughput().readCapacityUnits())
        .isEqualTo(10);

    assertThat(actualCreateTableRequests.get(1).tableName()).isEqualTo(getFullTableName());

    // for the table metadata table
    attributeDefinitions = actualCreateTableRequests.get(0).attributeDefinitions();
    assertThat(attributeDefinitions.size()).isEqualTo(1);
    assertThat(attributeDefinitions.get(0).attributeName())
        .isEqualTo(DynamoAdmin.METADATA_ATTR_TABLE);
    assertThat(attributeDefinitions.get(0).attributeType()).isEqualTo(ScalarAttributeType.S);

    assertThat(actualCreateTableRequests.get(0).keySchema().size()).isEqualTo(1);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(0).attributeName())
        .isEqualTo(DynamoAdmin.METADATA_ATTR_TABLE);
    assertThat(actualCreateTableRequests.get(0).keySchema().get(0).keyType())
        .isEqualTo(KeyType.HASH);

    assertThat(actualCreateTableRequests.get(0).provisionedThroughput().writeCapacityUnits())
        .isEqualTo(1);
    assertThat(actualCreateTableRequests.get(0).provisionedThroughput().readCapacityUnits())
        .isEqualTo(1);

    assertThat(actualCreateTableRequests.get(0).tableName()).isEqualTo(getFullMetadataTableName());

    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName()).isEqualTo(getFullMetadataTableName());

    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());
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

    ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestCaptor =
        ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);
    verify(client, times(2))
        .updateContinuousBackups(updateContinuousBackupsRequestCaptor.capture());
    List<UpdateContinuousBackupsRequest> updateContinuousBackupsRequests =
        updateContinuousBackupsRequestCaptor.getAllValues();
    assertThat(updateContinuousBackupsRequests.size()).isEqualTo(2);
    assertThat(updateContinuousBackupsRequests.get(1).tableName()).isEqualTo(getFullTableName());
    assertThat(updateContinuousBackupsRequests.get(0).tableName())
        .isEqualTo(getFullMetadataTableName());
  }

  @Test
  public void createTable_WhenMetadataTableExists_ShouldCreateOnlyTable()
      throws ExecutionException {
    // Arrange
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
        .isEqualTo(getFullTableName() + ".global_index.c4");
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

    assertThat(actualCreateTableRequest.tableName()).isEqualTo(getFullTableName());

    // for the table metadata table
    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest = putItemRequestCaptor.getValue();
    assertThat(actualPutItemRequest.tableName()).isEqualTo(getFullMetadataTableName());

    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());
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
  public void dropTable_WithNoMetadataLeft_ShouldDropTableAndDeleteMetadata()
      throws ExecutionException {
    // Arrange
    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
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

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(client).deleteTable(deleteTableRequestCaptor.capture());
    DeleteTableRequest actualDeleteTableRequest = deleteTableRequestCaptor.getValue();
    assertThat(actualDeleteTableRequest.tableName()).isEqualTo(getFullTableName());

    // for the table metadata table
    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName()).isEqualTo(getFullMetadataTableName());
    assertThat(actualDeleteItemRequest.key().get(DynamoAdmin.METADATA_ATTR_TABLE).s())
        .isEqualTo(getFullTableName());

    verify(applicationAutoScalingClient, times(4))
        .deleteScalingPolicy(any(DeleteScalingPolicyRequest.class));
    verify(applicationAutoScalingClient, times(4))
        .deregisterScalableTarget(any(DeregisterScalableTargetRequest.class));
  }

  @Test
  public void dropTable_WithMetadataLeft_ShouldDropTableAndDropMetadataTable()
      throws ExecutionException {
    // Arrange
    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
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

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    verify(client, times(2)).deleteTable(deleteTableRequestCaptor.capture());
    List<DeleteTableRequest> actualDeleteTableRequests = deleteTableRequestCaptor.getAllValues();

    assertThat(actualDeleteTableRequests.get(0).tableName()).isEqualTo(getFullTableName());

    // for the table metadata table
    assertThat(actualDeleteTableRequests.get(1).tableName()).isEqualTo(getFullMetadataTableName());

    ArgumentCaptor<DeleteItemRequest> deleteItemRequestCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(deleteItemRequestCaptor.capture());
    DeleteItemRequest actualDeleteItemRequest = deleteItemRequestCaptor.getValue();
    assertThat(actualDeleteItemRequest.tableName()).isEqualTo(getFullMetadataTableName());
    assertThat(actualDeleteItemRequest.key().get(DynamoAdmin.METADATA_ATTR_TABLE).s())
        .isEqualTo(getFullTableName());

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
        .thenReturn(
            ImmutableList.of(getPrefixedNamespace() + ".t1", getPrefixedNamespace() + ".t2"));

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(NAMESPACE);

    // Assert
    assertThat(actualNames).isEqualTo(tableNames);
  }

  @Test
  public void createIndex_ShouldCreateIndexProperly() throws ExecutionException {
    // Arrange
    GetItemResponse getItemResponse = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(getItemResponse);
    when(getItemResponse.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
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
                .build());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    GlobalSecondaryIndexDescription globalSecondaryIndexDescription =
        mock(GlobalSecondaryIndexDescription.class);
    when(tableDescription.globalSecondaryIndexes())
        .thenReturn(Collections.singletonList(globalSecondaryIndexDescription));
    String indexName = getFullTableName() + ".global_index.c4";
    when(globalSecondaryIndexDescription.indexName()).thenReturn(indexName);
    when(globalSecondaryIndexDescription.indexStatus()).thenReturn(IndexStatus.ACTIVE);

    // Act
    admin.createIndex(NAMESPACE, TABLE, "c4", Collections.emptyMap());

    // Assert
    verify(client).updateTable(any(UpdateTableRequest.class));
    verify(applicationAutoScalingClient, times(2))
        .putScalingPolicy(any(PutScalingPolicyRequest.class));
    verify(applicationAutoScalingClient, times(2))
        .registerScalableTarget(any(RegisterScalableTargetRequest.class));
  }

  @Test
  public void createIndex_OnBooleanColumn_ShouldThrowIllegalArgumentException() {
    // Arrange
    GetItemResponse getItemResponse = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(getItemResponse);
    when(getItemResponse.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_COLUMNS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put("c1", AttributeValue.builder().s("text").build())
                                .put("c2", AttributeValue.builder().s("boolean").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                    AttributeValue.builder().l(AttributeValue.builder().s("c1").build()).build())
                .build());

    // Act
    assertThatThrownBy(() -> admin.createIndex(NAMESPACE, TABLE, "c2", Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(client)
        .getItem(
            GetItemRequest.builder()
                .tableName(getFullMetadataTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.METADATA_ATTR_TABLE,
                        AttributeValue.builder().s(getFullTableName()).build()))
                .consistentRead(true)
                .build());
  }

  @Test
  public void dropIndex_ShouldDropIndexProperly() throws ExecutionException {
    // Arrange
    GetItemResponse getItemResponse = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(getItemResponse);
    when(getItemResponse.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
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

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.globalSecondaryIndexes()).thenReturn(Collections.emptyList());

    // Act
    admin.dropIndex(NAMESPACE, TABLE, "c4");

    // Assert
    verify(client).updateTable(any(UpdateTableRequest.class));
    verify(applicationAutoScalingClient, times(2))
        .deleteScalingPolicy(any(DeleteScalingPolicyRequest.class));
    verify(applicationAutoScalingClient, times(2))
        .deregisterScalableTarget(any(DeregisterScalableTargetRequest.class));
  }

  @Test
  public void repairTable_WithNonExistingTableToRepair_shouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames()).thenReturn(ImmutableList.of());

    // Act Assert
    assertThatThrownBy(() -> admin.repairTable(NAMESPACE, TABLE, metadata, ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void repairTable_WithNonExistingMetadataForTable_shouldAddMetadataForTable()
      throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    // The table to repair exists
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames()).thenReturn(ImmutableList.of(getFullTableName()));

    // Wait for metadata table creation
    DescribeTableResponse describeTableResponseMetadataTableCreation =
        mock(DescribeTableResponse.class);
    TableDescription tableDescriptionMetadataTableCreation = mock(TableDescription.class);
    when(describeTableResponseMetadataTableCreation.table())
        .thenReturn(tableDescriptionMetadataTableCreation);
    when(tableDescriptionMetadataTableCreation.tableStatus()).thenReturn(TableStatus.ACTIVE);

    when(client.describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build()))
        .thenThrow(ResourceNotFoundException.class)
        .thenReturn(describeTableResponseMetadataTableCreation);

    // Continuous backup check
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

    // Act
    admin.repairTable(NAMESPACE, TABLE, metadata, ImmutableMap.of());

    // Assert
    // Check metadata table creation
    ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client).createTable(createTableRequestArgumentCaptor.capture());
    CreateTableRequest actualCreateTableRequest = createTableRequestArgumentCaptor.getValue();
    assertThat(actualCreateTableRequest.tableName()).isEqualTo(getFullMetadataTableName());

    // Check continuous backup
    ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestCaptor =
        ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);
    verify(client).updateContinuousBackups(updateContinuousBackupsRequestCaptor.capture());
    UpdateContinuousBackupsRequest updateContinuousBackupsRequest =
        updateContinuousBackupsRequestCaptor.getValue();
    assertThat(updateContinuousBackupsRequest.tableName()).isEqualTo(getFullMetadataTableName());

    // Check added metadata
    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put("c1", AttributeValue.builder().s(DataType.TEXT.name().toLowerCase()).build());
    itemValues.put(DynamoAdmin.METADATA_ATTR_COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
        AttributeValue.builder()
            .l(ImmutableList.of(AttributeValue.builder().s("c1").build()))
            .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullMetadataTableName())
                .item(itemValues)
                .build());
  }

  @Test
  public void addNewColumnToTable_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    String currentColumn = "c1";
    String newColumn = "c2";

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item())
        .thenReturn(
            ImmutableMap.<String, AttributeValue>builder()
                .put(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s(getFullTableName()).build())
                .put(
                    DynamoAdmin.METADATA_ATTR_COLUMNS,
                    AttributeValue.builder()
                        .m(
                            ImmutableMap.<String, AttributeValue>builder()
                                .put(currentColumn, AttributeValue.builder().s("text").build())
                                .build())
                        .build())
                .put(
                    DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
                    AttributeValue.builder()
                        .l(AttributeValue.builder().s(currentColumn).build())
                        .build())
                .build());

    // Act
    admin.addNewColumnToTable(NAMESPACE, TABLE, newColumn, DataType.INT);

    // Assert
    // Get metadata
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());
    verify(client)
        .getItem(
            GetItemRequest.builder()
                .tableName(getFullMetadataTableName())
                .key(key)
                .consistentRead(true)
                .build());

    // Put metadata
    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_TABLE, AttributeValue.builder().s(getFullTableName()).build());
    Map<String, AttributeValue> columns = new HashMap<>();

    columns.put(
        currentColumn, AttributeValue.builder().s(DataType.TEXT.toString().toLowerCase()).build());
    columns.put(
        newColumn, AttributeValue.builder().s(DataType.INT.toString().toLowerCase()).build());

    itemValues.put(DynamoAdmin.METADATA_ATTR_COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        DynamoAdmin.METADATA_ATTR_PARTITION_KEY,
        AttributeValue.builder()
            .l(Collections.singletonList(AttributeValue.builder().s(currentColumn).build()))
            .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullMetadataTableName())
                .item(itemValues)
                .build());
  }

  @Test
  public void unsupportedOperations_ShouldThrowUnsupportedException() {
    // Arrange Act
    Throwable thrown1 = catchThrowable(() -> admin.getImportTableMetadata(NAMESPACE, TABLE));
    Throwable thrown2 =
        catchThrowable(() -> admin.addRawColumnToTable(NAMESPACE, TABLE, "c1", DataType.INT));
    Throwable thrown3 =
        catchThrowable(() -> admin.importTable(NAMESPACE, TABLE, Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown2).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown3).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getNamespaceNames_WithExistingTables_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(mock(DescribeTableResponse.class))
        .thenThrow(mock(ResourceNotFoundException.class))
        .thenReturn(tableIsActiveResponse);

    ScanResponse scanResponse = mock(ScanResponse.class);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);
    Map<String, AttributeValue> lastEvaluatedKeyFirstIteration =
        ImmutableMap.of("", AttributeValue.builder().build());
    Map<String, AttributeValue> lastEvaluatedKeySecondIteration = ImmutableMap.of();
    when(scanResponse.lastEvaluatedKey())
        .thenReturn(lastEvaluatedKeyFirstIteration)
        .thenReturn(lastEvaluatedKeySecondIteration);
    when(scanResponse.items())
        .thenReturn(
            ImmutableList.of(
                ImmutableMap.of(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s("ns1.tbl1").build())))
        .thenReturn(
            ImmutableList.of(
                ImmutableMap.of(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s("ns1.tbl2").build()),
                ImmutableMap.of(
                    DynamoAdmin.METADATA_ATTR_TABLE,
                    AttributeValue.builder().s("ns2.tbl3").build())));

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    verify(client)
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build());
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullMetadataTableName())
                .exclusiveStartKey(null)
                .build());
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullMetadataTableName())
                .exclusiveStartKey(lastEvaluatedKeyFirstIteration)
                .build());
    String metadataNamespace =
        getTableMetadataNamespaceConfig().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(actual).containsOnly("ns1", "ns2", metadataNamespace);
  }

  @Test
  public void getNamespaceNames_WithoutExistingTables_ShouldReturnMetadataNamespaceOnly()
      throws ExecutionException {
    // Arrange
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(mock(ResourceNotFoundException.class));

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    verify(client)
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build());
    String metadataNamespace =
        getTableMetadataNamespaceConfig().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(actual).containsOnly(metadataNamespace);
  }
}
