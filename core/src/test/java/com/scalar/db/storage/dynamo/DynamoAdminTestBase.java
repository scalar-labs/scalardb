package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;
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
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.Assertions;
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
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoverySpecification;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
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
 * notably {@link DynamoConfig#NAMESPACE_PREFIX} and {@link DynamoConfig#METADATA_NAMESPACE}
 */
public abstract class DynamoAdminTestBase {

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";

  @Mock private DynamoConfig config;
  @Mock private DynamoDbClient client;
  @Mock private ApplicationAutoScalingClient applicationAutoScalingClient;
  @Mock private DescribeTableResponse tableIsActiveResponse;
  @Mock private DescribeContinuousBackupsResponse backupIsEnabledResponse;
  private DynamoAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.getNamespacePrefix()).thenReturn(getNamespacePrefixConfig());
    when(config.getMetadataNamespace()).thenReturn(getMetadataNamespaceConfig());

    // prepare tableIsActiveResponse
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableIsActiveResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // prepare backupIsEnabledResponse
    ContinuousBackupsDescription continuousBackupsDescription =
        mock(ContinuousBackupsDescription.class);
    when(backupIsEnabledResponse.continuousBackupsDescription())
        .thenReturn(continuousBackupsDescription);
    when(continuousBackupsDescription.continuousBackupsStatus())
        .thenReturn(ContinuousBackupsStatus.ENABLED);

    admin = new DynamoAdmin(client, applicationAutoScalingClient, config);
  }

  /**
   * This sets the {@link DynamoConfig#METADATA_NAMESPACE} value that will be used to run the tests
   *
   * @return {@link DynamoConfig#METADATA_NAMESPACE} value
   */
  abstract Optional<String> getMetadataNamespaceConfig();

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
        + getMetadataNamespaceConfig().orElse(DynamoAdmin.METADATA_NAMESPACE)
        + "."
        + DynamoAdmin.METADATA_TABLE;
  }

  private String getFullNamespaceTableName() {
    return getNamespacePrefixConfig().orElse("")
        + getMetadataNamespaceConfig().orElse(DynamoAdmin.METADATA_NAMESPACE)
        + "."
        + DynamoAdmin.NAMESPACES_TABLE;
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

  @Test
  public void dropNamespace_WithOtherNamespacesExisting_ShouldDropNamespace()
      throws ExecutionException {
    // Arrange
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(1);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    // Act
    admin.dropNamespace(NAMESPACE);
    // Assert
    verify(client)
        .deleteItem(
            DeleteItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
    verify(client)
        .scan(ScanRequest.builder().tableName(getFullNamespaceTableName()).limit(1).build());
  }

  @Test
  public void dropNamespace_WithNoNamespacesLeft_ShouldDropNamespaceAndNamespacesTable()
      throws ExecutionException {
    // Arrange
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables(any(ListTablesRequest.class))).thenReturn(listTablesResponse);
    when(listTablesResponse.lastEvaluatedTableName()).thenReturn(null);
    when(listTablesResponse.tableNames()).thenReturn(Collections.emptyList());
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.count()).thenReturn(0);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    // Act
    admin.dropNamespace(NAMESPACE);

    // Assert
    verify(client)
        .deleteItem(
            DeleteItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());

    verify(client)
        .scan(ScanRequest.builder().tableName(getFullNamespaceTableName()).limit(1).build());
    verify(client)
        .deleteTable(DeleteTableRequest.builder().tableName(getFullNamespaceTableName()).build());
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

    when(client.describeTable(DescribeTableRequest.builder().tableName(getFullTableName()).build()))
        .thenReturn(tableIsActiveResponse);

    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // for the table metadata table
    when(client.describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build()))
        .thenThrow(ResourceNotFoundException.class)
        .thenReturn(tableIsActiveResponse);

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
        .isEqualTo(getFullTableName() + ".global_index.c4");
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

    assertThat(actualCreateTableRequests.get(0).tableName()).isEqualTo(getFullTableName());

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

    assertThat(actualCreateTableRequests.get(1).tableName()).isEqualTo(getFullMetadataTableName());

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
    assertThat(updateContinuousBackupsRequests.get(0).tableName()).isEqualTo(getFullTableName());
    assertThat(updateContinuousBackupsRequests.get(1).tableName())
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

    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(tableIsActiveResponse);

    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

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
  public void repairTable_WithExistingTableToRepairAndMetadataTables_shouldNotCreateTables()
      throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    // The table to repair exists
    when(client.describeTable(DescribeTableRequest.builder().tableName(getFullTableName()).build()))
        .thenReturn(tableIsActiveResponse);
    // The metadata table exists
    when(client.describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build()))
        .thenReturn(tableIsActiveResponse);

    // Continuous backup check
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.repairTable(NAMESPACE, TABLE, metadata, ImmutableMap.of());

    // Assert
    verify(client, never()).createTable(any(CreateTableRequest.class));

    // Check continuous backup registration
    ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestCaptor =
        ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);
    verify(client, times(2))
        .updateContinuousBackups(updateContinuousBackupsRequestCaptor.capture());
    assertThat(updateContinuousBackupsRequestCaptor.getAllValues().get(0).tableName())
        .isEqualTo(getFullTableName());
    assertThat(updateContinuousBackupsRequestCaptor.getAllValues().get(1).tableName())
        .isEqualTo(getFullMetadataTableName());

    // Check scaling policy
    ArgumentCaptor<RegisterScalableTargetRequest> registerScalableTargetRequestArgumentCaptor =
        ArgumentCaptor.forClass(RegisterScalableTargetRequest.class);
    verify(applicationAutoScalingClient, times(2))
        .registerScalableTarget(registerScalableTargetRequestArgumentCaptor.capture());
    registerScalableTargetRequestArgumentCaptor
        .getAllValues()
        .forEach(
            captor ->
                Assertions.assertThat(captor.resourceId())
                    .isEqualTo("table/" + getFullTableName()));

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
  public void repairTable_WithNonExistingTableAndMetadataTables_shouldCreateBothTables()
      throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    when(client.describeTable(DescribeTableRequest.builder().tableName(getFullTableName()).build()))
        // Check that the table repair do not exist
        .thenThrow(ResourceNotFoundException.class)
        // Check that waits for the table to repair to be created
        .thenReturn(tableIsActiveResponse);

    when(client.describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build()))
        // Check that the metadata table do not exist
        .thenThrow(ResourceNotFoundException.class)
        // Check that wait for the metadata table to be created
        .thenReturn(tableIsActiveResponse);

    // Continuous backup check
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.repairTable(NAMESPACE, TABLE, metadata, ImmutableMap.of());

    // Assert
    // Check metadata table creation
    ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(client, times(2)).createTable(createTableRequestArgumentCaptor.capture());
    CreateTableRequest actualCreateTableToRepairRequest =
        createTableRequestArgumentCaptor.getAllValues().get(0);
    assertThat(actualCreateTableToRepairRequest.tableName()).isEqualTo(getFullTableName());
    CreateTableRequest actualCreateMetadataTableRequest =
        createTableRequestArgumentCaptor.getAllValues().get(1);
    assertThat(actualCreateMetadataTableRequest.tableName()).isEqualTo(getFullMetadataTableName());

    // Check continuous backup registration
    ArgumentCaptor<UpdateContinuousBackupsRequest> updateContinuousBackupsRequestCaptor =
        ArgumentCaptor.forClass(UpdateContinuousBackupsRequest.class);
    verify(client, times(2))
        .updateContinuousBackups(updateContinuousBackupsRequestCaptor.capture());
    assertThat(updateContinuousBackupsRequestCaptor.getAllValues().get(0).tableName())
        .isEqualTo(getFullTableName());
    assertThat(updateContinuousBackupsRequestCaptor.getAllValues().get(1).tableName())
        .isEqualTo(getFullMetadataTableName());

    // Check scaling policy
    ArgumentCaptor<RegisterScalableTargetRequest> registerScalableTargetRequestArgumentCaptor =
        ArgumentCaptor.forClass(RegisterScalableTargetRequest.class);
    verify(applicationAutoScalingClient, times(2))
        .registerScalableTarget(registerScalableTargetRequestArgumentCaptor.capture());
    registerScalableTargetRequestArgumentCaptor
        .getAllValues()
        .forEach(
            captor ->
                Assertions.assertThat(captor.resourceId())
                    .isEqualTo("table/" + getFullTableName()));

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
  public void
      createNamespace_WithNonExistingNamespacesTable_ShouldCreateNamespacesTableAndAddNamespace()
          throws ExecutionException {
    // Arrange
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(mock(ResourceNotFoundException.class))
        .thenReturn(tableIsActiveResponse);
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.createNamespace(NAMESPACE, Collections.emptyMap());

    // Assert
    verify(client, times(2))
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullNamespaceTableName()).build());
    CreateTableRequest createNamespaceTableRequest =
        CreateTableRequest.builder()
            .attributeDefinitions(
                ImmutableList.of(
                    AttributeDefinition.builder()
                        .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                        .attributeType(ScalarAttributeType.S)
                        .build()))
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                    .keyType(KeyType.HASH)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .writeCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .build())
            .tableName(getFullNamespaceTableName())
            .build();
    verify(client).createTable(createNamespaceTableRequest);
    verify(client)
        .describeContinuousBackups(
            DescribeContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .build());
    verify(client)
        .updateContinuousBackups(
            UpdateContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .pointInTimeRecoverySpecification(
                    PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(true)
                        .build())
                .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .item(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void createNamespace_WithExistingNamespacesTable_ShouldAddNamespace()
      throws ExecutionException {
    // Arrange
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.createNamespace(NAMESPACE, Collections.emptyMap());

    // Assert
    verify(client)
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullNamespaceTableName()).build());
    verify(client)
        .describeContinuousBackups(
            DescribeContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .item(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void namespaceExists_WithExistingNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    GetItemResponse getItemResponse = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(getItemResponse);
    when(getItemResponse.hasItem()).thenReturn(true);

    // Act
    boolean actualNamespaceExists = admin.namespaceExists(NAMESPACE);

    // Assert]
    assertThat(actualNamespaceExists).isTrue();
    verify(client)
        .getItem(
            GetItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void namespaceExists_WithNonExistingNamespace_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    GetItemResponse getItemResponse = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(getItemResponse);
    when(getItemResponse.hasItem()).thenReturn(false);

    // Act
    boolean actualNamespaceExists = admin.namespaceExists(NAMESPACE);

    // Assert]
    assertThat(actualNamespaceExists).isFalse();
    verify(client)
        .getItem(
            GetItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void namespaceExists_WithNonExistingNamespacesTable_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    when(client.getItem(any(GetItemRequest.class))).thenThrow(ResourceNotFoundException.class);

    // Act
    boolean actualNamespaceExists = admin.namespaceExists(NAMESPACE);

    // Assert]
    assertThat(actualNamespaceExists).isFalse();
    verify(client)
        .getItem(
            GetItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .key(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void getNamespacesNames_WithExistingNamespacesTable_ShouldReturnNamespaceNames()
      throws ExecutionException {
    // Arrange
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);
    when(scanResponse.items())
        .thenReturn(
            ImmutableList.of(
                ImmutableMap.of(
                    DynamoAdmin.NAMESPACES_ATTR_NAME,
                    AttributeValue.builder().s(getPrefixedNamespace() + 1).build()),
                ImmutableMap.of(
                    DynamoAdmin.NAMESPACES_ATTR_NAME,
                    AttributeValue.builder().s(getPrefixedNamespace() + 2).build())))
        .thenReturn(
            Collections.singletonList(
                ImmutableMap.of(
                    DynamoAdmin.NAMESPACES_ATTR_NAME,
                    AttributeValue.builder().s(getPrefixedNamespace() + 3).build())));
    Map<String, AttributeValue> lastEvaluatedKey =
        ImmutableMap.of("foo", AttributeValue.builder().build());
    when(scanResponse.lastEvaluatedKey())
        .thenReturn(lastEvaluatedKey)
        .thenReturn(Collections.emptyMap());

    // Act
    Set<String> actualNamespaceNames = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaceNames).containsOnly(NAMESPACE + 1, NAMESPACE + 2, NAMESPACE + 3);
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullNamespaceTableName())
                .exclusiveStartKey(null)
                .build());
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullNamespaceTableName())
                .exclusiveStartKey(lastEvaluatedKey)
                .build());
  }

  @Test
  public void getNamespacesNames_WithNonExistingNamespaces_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);
    when(scanResponse.items()).thenReturn(Collections.emptyList());

    when(scanResponse.lastEvaluatedKey()).thenReturn(Collections.emptyMap());

    // Act
    Set<String> actualNamespaceNames = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaceNames).isEmpty();
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullNamespaceTableName())
                .exclusiveStartKey(null)
                .build());
  }

  @Test
  public void getNamespacesNames_WithNonExistingNamespacesTable_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    when(client.scan(any(ScanRequest.class))).thenThrow(ResourceNotFoundException.class);

    // Act
    Set<String> actualNamespaceNames = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaceNames).isEmpty();
    verify(client)
        .scan(
            ScanRequest.builder()
                .tableName(getFullNamespaceTableName())
                .exclusiveStartKey(null)
                .build());
  }

  @Test
  public void unsupportedOperations_ShouldThrowUnsupportedException() {
    // Arrange Act
    Throwable thrown1 = catchThrowable(() -> admin.getImportTableMetadata(NAMESPACE, TABLE));
    Throwable thrown2 =
        catchThrowable(() -> admin.addRawColumnToTable(NAMESPACE, TABLE, "c1", DataType.INT));
    Throwable thrown3 = catchThrowable(() -> admin.importTable(NAMESPACE, TABLE));

    // Assert
    assertThat(thrown1).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown2).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown3).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      repairNamespace_WithNonExistingNamespacesTable_ShouldCreateNamespacesTableAndAddNamespace()
          throws ExecutionException {
    // Arrange
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(mock(ResourceNotFoundException.class))
        .thenReturn(tableIsActiveResponse);
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.repairNamespace(NAMESPACE, Collections.emptyMap());

    // Assert
    verify(client, times(2))
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullNamespaceTableName()).build());
    CreateTableRequest createNamespaceTableRequest =
        CreateTableRequest.builder()
            .attributeDefinitions(
                ImmutableList.of(
                    AttributeDefinition.builder()
                        .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                        .attributeType(ScalarAttributeType.S)
                        .build()))
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                    .keyType(KeyType.HASH)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .writeCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .build())
            .tableName(getFullNamespaceTableName())
            .build();
    verify(client).createTable(createNamespaceTableRequest);
    verify(client)
        .describeContinuousBackups(
            DescribeContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .build());
    verify(client)
        .updateContinuousBackups(
            UpdateContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .pointInTimeRecoverySpecification(
                    PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(true)
                        .build())
                .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .item(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void repairNamespace_WithExistingNamespacesTable_ShouldAddNamespace()
      throws ExecutionException {
    // Arrange
    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);

    // Act
    admin.repairNamespace(NAMESPACE, Collections.emptyMap());

    // Assert
    verify(client)
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullNamespaceTableName()).build());
    verify(client)
        .describeContinuousBackups(
            DescribeContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .build());
    verify(client)
        .putItem(
            PutItemRequest.builder()
                .tableName(getFullNamespaceTableName())
                .item(
                    ImmutableMap.of(
                        DynamoAdmin.NAMESPACES_ATTR_NAME,
                        AttributeValue.builder().s(getPrefixedNamespace()).build()))
                .build());
  }

  @Test
  public void upgrade_WithExistingTables_ShouldUpsertNamespaceNames() throws ExecutionException {
    // Arrange
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(mock(DescribeTableResponse.class))
        .thenThrow(mock(ResourceNotFoundException.class))
        .thenReturn(tableIsActiveResponse);

    when(client.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class)))
        .thenReturn(backupIsEnabledResponse);
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
    admin.upgrade(Collections.emptyMap());

    // Assert
    verify(client)
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullMetadataTableName()).build());
    verify(client, times(2))
        .describeTable(
            DescribeTableRequest.builder().tableName(getFullNamespaceTableName()).build());
    CreateTableRequest createNamespaceTableRequest =
        CreateTableRequest.builder()
            .attributeDefinitions(
                ImmutableList.of(
                    AttributeDefinition.builder()
                        .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                        .attributeType(ScalarAttributeType.S)
                        .build()))
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoAdmin.NAMESPACES_ATTR_NAME)
                    .keyType(KeyType.HASH)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .writeCapacityUnits(DynamoAdmin.METADATA_TABLES_REQUEST_UNIT)
                    .build())
            .tableName(getFullNamespaceTableName())
            .build();
    verify(client).createTable(createNamespaceTableRequest);
    verify(client)
        .describeContinuousBackups(
            DescribeContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .build());
    verify(client)
        .updateContinuousBackups(
            UpdateContinuousBackupsRequest.builder()
                .tableName(getFullNamespaceTableName())
                .pointInTimeRecoverySpecification(
                    PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(true)
                        .build())
                .build());
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
    ArgumentCaptor<PutItemRequest> putItemRequestCaptor =
        ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client, times(2)).putItem(putItemRequestCaptor.capture());
    PutItemRequest actualPutItemRequest1 = putItemRequestCaptor.getAllValues().get(0);
    assertThat(actualPutItemRequest1.tableName()).isEqualTo(getFullNamespaceTableName());
    assertThat(actualPutItemRequest1.item())
        .containsExactly(
            entry(DynamoAdmin.NAMESPACES_ATTR_NAME, AttributeValue.builder().s("ns1").build()));
    PutItemRequest actualPutItemRequest2 = putItemRequestCaptor.getAllValues().get(1);
    assertThat(actualPutItemRequest2.tableName()).isEqualTo(getFullNamespaceTableName());
    assertThat(actualPutItemRequest2.item())
        .containsExactly(
            entry(DynamoAdmin.NAMESPACES_ATTR_NAME, AttributeValue.builder().s("ns2").build()));
  }
}
