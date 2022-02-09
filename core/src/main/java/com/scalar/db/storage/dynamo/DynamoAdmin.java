package com.scalar.db.storage.dynamo;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClientBuilder;
import software.amazon.awssdk.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricType;
import software.amazon.awssdk.services.applicationautoscaling.model.ObjectNotFoundException;
import software.amazon.awssdk.services.applicationautoscaling.model.PolicyType;
import software.amazon.awssdk.services.applicationautoscaling.model.PredefinedMetricSpecification;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.ScalableDimension;
import software.amazon.awssdk.services.applicationautoscaling.model.ServiceNamespace;
import software.amazon.awssdk.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsStatus;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoverySpecification;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;

/**
 * Manages table creating, dropping and truncating in Dynamo DB
 *
 * @author Pham Ba Thong
 */
@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoAdmin.class);

  public static final String NO_SCALING = "no-scaling";
  public static final String NO_BACKUP = "no-backup";
  public static final String REQUEST_UNIT = "ru";
  public static final String DEFAULT_NO_SCALING = "false";
  public static final String DEFAULT_NO_BACKUP = "false";
  public static final String DEFAULT_REQUEST_UNIT = "10";

  @VisibleForTesting static final String PARTITION_KEY = "concatenatedPartitionKey";
  @VisibleForTesting static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  private static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";
  private static final int WAITING_DURATION_SECS = 3;
  private static final int COOL_DOWN_DURATION_SECS = 60;
  private static final double TARGET_USAGE_RATE = 70.0;
  private static final int DELETE_BATCH_SIZE = 100;
  private static final String SCALING_TYPE_READ = "read";
  private static final String SCALING_TYPE_WRITE = "write";
  private static final String SCALING_TYPE_INDEX_READ = "index-read";
  private static final String SCALING_TYPE_INDEX_WRITE = "index-write";

  public static final String METADATA_NAMESPACE = "scalardb";
  public static final String METADATA_TABLE = "metadata";
  @VisibleForTesting static final String METADATA_ATTR_PARTITION_KEY = "partitionKey";
  @VisibleForTesting static final String METADATA_ATTR_CLUSTERING_KEY = "clusteringKey";
  @VisibleForTesting static final String METADATA_ATTR_CLUSTERING_ORDERS = "clusteringOrders";
  @VisibleForTesting static final String METADATA_ATTR_SECONDARY_INDEX = "secondaryIndex";
  @VisibleForTesting static final String METADATA_ATTR_COLUMNS = "columns";
  @VisibleForTesting static final String METADATA_ATTR_TABLE = "table";
  private static final long METADATA_TABLE_REQUEST_UNIT = 1;

  private static final ImmutableMap<DataType, ScalarAttributeType> SECONDARY_INDEX_DATATYPE_MAP =
      ImmutableMap.<DataType, ScalarAttributeType>builder()
          .put(DataType.INT, ScalarAttributeType.N)
          .put(DataType.BIGINT, ScalarAttributeType.N)
          .put(DataType.FLOAT, ScalarAttributeType.N)
          .put(DataType.DOUBLE, ScalarAttributeType.N)
          .put(DataType.TEXT, ScalarAttributeType.S)
          .put(DataType.BLOB, ScalarAttributeType.B)
          .build();
  private static final ImmutableSet<String> TABLE_SCALING_TYPE_SET =
      ImmutableSet.<String>builder().add(SCALING_TYPE_READ).add(SCALING_TYPE_WRITE).build();
  private static final ImmutableSet<String> SECONDARY_INDEX_SCALING_TYPE_SET =
      ImmutableSet.<String>builder()
          .add(SCALING_TYPE_INDEX_READ)
          .add(SCALING_TYPE_INDEX_WRITE)
          .build();
  private static final ImmutableMap<String, ScalableDimension> SCALABLE_DIMENSION_MAP =
      ImmutableMap.<String, ScalableDimension>builder()
          .put(SCALING_TYPE_READ, ScalableDimension.DYNAMODB_TABLE_READ_CAPACITY_UNITS)
          .put(SCALING_TYPE_WRITE, ScalableDimension.DYNAMODB_TABLE_WRITE_CAPACITY_UNITS)
          .put(SCALING_TYPE_INDEX_READ, ScalableDimension.DYNAMODB_INDEX_READ_CAPACITY_UNITS)
          .put(SCALING_TYPE_INDEX_WRITE, ScalableDimension.DYNAMODB_INDEX_WRITE_CAPACITY_UNITS)
          .build();
  private static final ImmutableMap<String, MetricType> SCALING_POLICY_METRIC_TYPE_MAP =
      ImmutableMap.<String, MetricType>builder()
          .put(SCALING_TYPE_READ, MetricType.DYNAMO_DB_READ_CAPACITY_UTILIZATION)
          .put(SCALING_TYPE_WRITE, MetricType.DYNAMO_DB_WRITE_CAPACITY_UTILIZATION)
          .put(SCALING_TYPE_INDEX_READ, MetricType.DYNAMO_DB_READ_CAPACITY_UTILIZATION)
          .put(SCALING_TYPE_INDEX_WRITE, MetricType.DYNAMO_DB_WRITE_CAPACITY_UTILIZATION)
          .build();

  private final DynamoDbClient client;
  private final ApplicationAutoScalingClient applicationAutoScalingClient;
  private final String metadataNamespace;

  @Inject
  public DynamoAdmin(DynamoConfig config) {
    AwsCredentialsProvider credentialsProvider = createCredentialsProvider(config);

    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    client =
        builder
            .credentialsProvider(credentialsProvider)
            .region(Region.of(config.getContactPoints().get(0)))
            .build();

    applicationAutoScalingClient = createApplicationAutoScalingClient(config);
    metadataNamespace = config.getTableMetadataNamespace().orElse(METADATA_NAMESPACE);
  }

  public DynamoAdmin(DynamoDbClient client, DynamoConfig config) {
    this.client = client;
    applicationAutoScalingClient = createApplicationAutoScalingClient(config);
    metadataNamespace = config.getTableMetadataNamespace().orElse(METADATA_NAMESPACE);
  }

  private AwsCredentialsProvider createCredentialsProvider(DynamoConfig config) {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(
            config.getUsername().orElse(null), config.getPassword().orElse(null)));
  }

  private ApplicationAutoScalingClient createApplicationAutoScalingClient(DynamoConfig config) {
    ApplicationAutoScalingClientBuilder builder = ApplicationAutoScalingClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    return builder
        .credentialsProvider(createCredentialsProvider(config))
        .region(Region.of(config.getContactPoints().get(0)))
        .build();
  }

  @VisibleForTesting
  DynamoAdmin(
      DynamoDbClient client,
      ApplicationAutoScalingClient applicationAutoScalingClient,
      DynamoConfig config) {
    this.client = client;
    this.applicationAutoScalingClient = applicationAutoScalingClient;
    metadataNamespace = config.getTableMetadataNamespace().orElse(METADATA_NAMESPACE);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options) {
    // In Dynamo DB storage, namespace will be added to table name as prefix along with dot
    // separator.
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    checkMetadata(metadata);

    long ru = Long.parseLong(options.getOrDefault(REQUEST_UNIT, DEFAULT_REQUEST_UNIT));

    CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();
    buildAttributeDefinitions(requestBuilder, metadata);
    buildPrimaryKey(requestBuilder, metadata);
    buildSecondaryIndexes(namespace, table, requestBuilder, metadata, ru);
    requestBuilder.provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());
    requestBuilder.tableName(getFullTableName(namespace, table));

    try {
      client.createTable(requestBuilder.build());
    } catch (Exception e) {
      throw new ExecutionException("creating the table failed", e);
    }
    waitForTableCreation(getFullTableName(namespace, table));

    addTableMetadata(namespace, table, metadata);

    boolean noScaling = Boolean.parseBoolean(options.getOrDefault(NO_SCALING, DEFAULT_NO_SCALING));
    if (!noScaling) {
      enableAutoScaling(namespace, table, metadata.getSecondaryIndexNames(), ru);
    }

    boolean noBackup = Boolean.parseBoolean(options.getOrDefault(NO_BACKUP, DEFAULT_NO_BACKUP));
    if (!noBackup) {
      enableContinuousBackup(namespace, table);
    }
  }

  private void checkMetadata(TableMetadata metadata) {
    Iterator<String> partitionKeyNameIterator = metadata.getPartitionKeyNames().iterator();
    while (partitionKeyNameIterator.hasNext()) {
      String partitionKeyName = partitionKeyNameIterator.next();
      if (!partitionKeyNameIterator.hasNext()) {
        break;
      }
      if (metadata.getColumnDataType(partitionKeyName) == DataType.BLOB) {
        throw new IllegalArgumentException(
            "BLOB type is supported only for the last value in partition key in DynamoDB: "
                + partitionKeyName);
      }
    }

    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      if (metadata.getColumnDataType(clusteringKeyName) == DataType.BLOB) {
        throw new IllegalArgumentException(
            "Currently, BLOB type is not supported for clustering keys in DynamoDB: "
                + clusteringKeyName);
      }
    }

    for (String secondaryIndexName : metadata.getSecondaryIndexNames()) {
      if (metadata.getColumnDataType(secondaryIndexName) == DataType.BOOLEAN) {
        throw new IllegalArgumentException(
            "Currently, BOOLEAN type is not supported for a secondary index in DynamoDB: "
                + secondaryIndexName);
      }
    }
  }

  private void buildAttributeDefinitions(
      CreateTableRequest.Builder requestBuilder, TableMetadata metadata) {
    List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
    // for partition key
    columnsToAttributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(PARTITION_KEY)
            .attributeType(ScalarAttributeType.B)
            .build());
    // for clustering key
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      columnsToAttributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(CLUSTERING_KEY)
              .attributeType(ScalarAttributeType.B)
              .build());
    }
    // for secondary indexes
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
      for (String secondaryIndex : metadata.getSecondaryIndexNames()) {
        columnsToAttributeDefinitions.add(
            AttributeDefinition.builder()
                .attributeName(secondaryIndex)
                .attributeType(
                    SECONDARY_INDEX_DATATYPE_MAP.get(metadata.getColumnDataType(secondaryIndex)))
                .build());
      }
    }
    requestBuilder.attributeDefinitions(columnsToAttributeDefinitions);
  }

  private void buildPrimaryKey(CreateTableRequest.Builder requestBuilder, TableMetadata metadata) {
    List<KeySchemaElement> keySchemaElementList = new ArrayList<>();
    keySchemaElementList.add(
        KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      keySchemaElementList.add(
          KeySchemaElement.builder().attributeName(CLUSTERING_KEY).keyType(KeyType.RANGE).build());
    }
    requestBuilder.keySchema(keySchemaElementList);
  }

  private void buildSecondaryIndexes(
      String namespace,
      String table,
      CreateTableRequest.Builder requestBuilder,
      TableMetadata metadata,
      long ru) {
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
      List<GlobalSecondaryIndex> globalSecondaryIndexList = new ArrayList<>();
      for (String secondaryIndex : metadata.getSecondaryIndexNames()) {
        globalSecondaryIndexList.add(
            GlobalSecondaryIndex.builder()
                .indexName(getGlobalIndexName(namespace, table, secondaryIndex))
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(secondaryIndex)
                        .keyType(KeyType.HASH)
                        .build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(ru)
                        .writeCapacityUnits(ru)
                        .build())
                .build());
      }
      requestBuilder.globalSecondaryIndexes(globalSecondaryIndexList);
    }
  }

  private String getGlobalIndexName(String namespace, String tableName, String keyName) {
    return getFullTableName(namespace, tableName) + "." + GLOBAL_INDEX_NAME_PREFIX + "." + keyName;
  }

  private void addTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    createMetadataTableIfNotExists();

    // Add metadata
    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(
        METADATA_ATTR_TABLE,
        AttributeValue.builder().s(getFullTableName(namespace, table)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    for (String columnName : metadata.getColumnNames()) {
      columns.put(
          columnName,
          AttributeValue.builder()
              .s(metadata.getColumnDataType(columnName).name().toLowerCase())
              .build());
    }
    itemValues.put(METADATA_ATTR_COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        METADATA_ATTR_PARTITION_KEY,
        AttributeValue.builder()
            .l(
                metadata.getPartitionKeyNames().stream()
                    .map(pKey -> AttributeValue.builder().s(pKey).build())
                    .collect(Collectors.toList()))
            .build());
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      itemValues.put(
          METADATA_ATTR_CLUSTERING_KEY,
          AttributeValue.builder()
              .l(
                  metadata.getClusteringKeyNames().stream()
                      .map(pKey -> AttributeValue.builder().s(pKey).build())
                      .collect(Collectors.toList()))
              .build());

      Map<String, AttributeValue> clusteringOrders = new HashMap<>();
      for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
        clusteringOrders.put(
            clusteringKeyName,
            AttributeValue.builder()
                .s(metadata.getClusteringOrder(clusteringKeyName).name())
                .build());
      }
      itemValues.put(
          METADATA_ATTR_CLUSTERING_ORDERS, AttributeValue.builder().m(clusteringOrders).build());
    }
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
      itemValues.put(
          METADATA_ATTR_SECONDARY_INDEX,
          AttributeValue.builder().ss(metadata.getSecondaryIndexNames()).build());
    }
    try {
      client.putItem(
          PutItemRequest.builder().tableName(getMetadataTable()).item(itemValues).build());
    } catch (Exception e) {
      throw new ExecutionException(
          "adding the meta data for table " + getFullTableName(namespace, table) + " failed", e);
    }
  }

  private void createMetadataTableIfNotExists() throws ExecutionException {
    if (metadataTableExists()) {
      return;
    }

    List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
    columnsToAttributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(METADATA_ATTR_TABLE)
            .attributeType(ScalarAttributeType.S)
            .build());
    try {
      client.createTable(
          CreateTableRequest.builder()
              .attributeDefinitions(columnsToAttributeDefinitions)
              .keySchema(
                  KeySchemaElement.builder()
                      .attributeName(METADATA_ATTR_TABLE)
                      .keyType(KeyType.HASH)
                      .build())
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(METADATA_TABLE_REQUEST_UNIT)
                      .writeCapacityUnits(METADATA_TABLE_REQUEST_UNIT)
                      .build())
              .tableName(getMetadataTable())
              .build());
    } catch (Exception e) {
      throw new ExecutionException("creating the metadata table failed", e);
    }
    waitForTableCreation(getMetadataTable());
  }

  private boolean metadataTableExists() throws ExecutionException {
    try {
      client.describeTable(DescribeTableRequest.builder().tableName(getMetadataTable()).build());
      return true;
    } catch (Exception e) {
      if (e instanceof ResourceNotFoundException) {
        return false;
      } else {
        throw new ExecutionException("checking the metadata table existence failed", e);
      }
    }
  }

  private void waitForTableCreation(String tableFullName) throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(WAITING_DURATION_SECS, TimeUnit.SECONDS);
        DescribeTableResponse describeTableResponse =
            client.describeTable(DescribeTableRequest.builder().tableName(tableFullName).build());
        if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException("waiting for the table creation failed", e);
    }
  }

  private void enableAutoScaling(
      String namespace, String table, Set<String> secondaryIndexes, long ru)
      throws ExecutionException {
    List<RegisterScalableTargetRequest> registerScalableTargetRequestList = new ArrayList<>();
    List<PutScalingPolicyRequest> putScalingPolicyRequestList = new ArrayList<>();

    // write, read scaling of table
    for (String scalingType : TABLE_SCALING_TYPE_SET) {
      registerScalableTargetRequestList.add(
          buildRegisterScalableTargetRequest(
              getTableResourceID(namespace, table), scalingType, (int) ru));
      putScalingPolicyRequestList.add(
          buildPutScalingPolicyRequest(getTableResourceID(namespace, table), scalingType));
    }

    // write, read scaling of global indexes (secondary indexes)
    for (String secondaryIndex : secondaryIndexes) {
      for (String scalingType : SECONDARY_INDEX_SCALING_TYPE_SET) {
        registerScalableTargetRequestList.add(
            buildRegisterScalableTargetRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType, (int) ru));
        putScalingPolicyRequestList.add(
            buildPutScalingPolicyRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
      }
    }

    // request
    for (RegisterScalableTargetRequest registerScalableTargetRequest :
        registerScalableTargetRequestList) {
      try {
        applicationAutoScalingClient.registerScalableTarget(registerScalableTargetRequest);
      } catch (Exception e) {
        throw new ExecutionException(
            "Unable to register scalable target for " + registerScalableTargetRequest.resourceId(),
            e);
      }
    }

    for (PutScalingPolicyRequest putScalingPolicyRequest : putScalingPolicyRequestList) {
      try {
        applicationAutoScalingClient.putScalingPolicy(putScalingPolicyRequest);
      } catch (Exception e) {
        throw new ExecutionException(
            "Unable to put scaling policy request for " + putScalingPolicyRequest.resourceId(), e);
      }
    }
  }

  private RegisterScalableTargetRequest buildRegisterScalableTargetRequest(
      String resourceID, String type, int ruValue) {
    return RegisterScalableTargetRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
        .minCapacity(ruValue > 10 ? ruValue / 10 : ruValue)
        .maxCapacity(ruValue)
        .build();
  }

  private PutScalingPolicyRequest buildPutScalingPolicyRequest(String resourceID, String type) {
    return PutScalingPolicyRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
        .policyName(getPolicyName(resourceID, type))
        .policyType(PolicyType.TARGET_TRACKING_SCALING)
        .targetTrackingScalingPolicyConfiguration(getScalingPolicyConfiguration(type))
        .build();
  }

  private String getTableResourceID(String namespace, String table) {
    return "table/" + getFullTableName(namespace, table);
  }

  private String getGlobalIndexResourceID(String namespace, String table, String globalIndex) {
    return "table/"
        + getFullTableName(namespace, table)
        + "/index/"
        + getGlobalIndexName(namespace, table, globalIndex);
  }

  private String getPolicyName(String resourceID, String type) {
    return resourceID + "-" + type;
  }

  private TargetTrackingScalingPolicyConfiguration getScalingPolicyConfiguration(String type) {
    return TargetTrackingScalingPolicyConfiguration.builder()
        .predefinedMetricSpecification(
            PredefinedMetricSpecification.builder()
                .predefinedMetricType(SCALING_POLICY_METRIC_TYPE_MAP.get(type))
                .build())
        .scaleInCooldown(COOL_DOWN_DURATION_SECS)
        .scaleOutCooldown(COOL_DOWN_DURATION_SECS)
        .targetValue(TARGET_USAGE_RATE)
        .build();
  }

  private void enableContinuousBackup(String namespace, String table) throws ExecutionException {
    waitForTableBackupEnabledAtCreation(namespace, table);

    try {
      client.updateContinuousBackups(buildUpdateContinuousBackupsRequest(namespace, table));
    } catch (Exception e) {
      throw new ExecutionException(
          "Unable to enable continuous backup for " + getFullTableName(namespace, table), e);
    }
  }

  private void waitForTableBackupEnabledAtCreation(String namespace, String table)
      throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(WAITING_DURATION_SECS, TimeUnit.SECONDS);
        DescribeContinuousBackupsResponse describeContinuousBackupsResponse =
            client.describeContinuousBackups(
                DescribeContinuousBackupsRequest.builder()
                    .tableName(getFullTableName(namespace, table))
                    .build());
        if (describeContinuousBackupsResponse
                .continuousBackupsDescription()
                .continuousBackupsStatus()
            == ContinuousBackupsStatus.ENABLED) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException("waiting for the table backup enabled at creation failed", e);
    }
  }

  private PointInTimeRecoverySpecification buildPointInTimeRecoverySpecification() {
    return PointInTimeRecoverySpecification.builder().pointInTimeRecoveryEnabled(true).build();
  }

  private UpdateContinuousBackupsRequest buildUpdateContinuousBackupsRequest(
      String namespace, String table) {
    return UpdateContinuousBackupsRequest.builder()
        .tableName(getFullTableName(namespace, table))
        .pointInTimeRecoverySpecification(buildPointInTimeRecoverySpecification())
        .build();
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    disableAutoScaling(namespace, table);

    String fullTableName = getFullTableName(namespace, table);
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(fullTableName).build());
    } catch (Exception e) {
      throw new ExecutionException("deleting table " + fullTableName + " failed", e);
    }
    waitForTableDeletion(namespace, table);
    deleteTableMetadata(namespace, table);
  }

  private void disableAutoScaling(String namespace, String table) throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      return;
    }

    List<DeregisterScalableTargetRequest> deregisterScalableTargetRequestList = new ArrayList<>();
    List<DeleteScalingPolicyRequest> deleteScalingPolicyRequestList = new ArrayList<>();

    // write, read scaling of table
    for (String scalingType : TABLE_SCALING_TYPE_SET) {
      deregisterScalableTargetRequestList.add(
          buildDeregisterScalableTargetRequest(getTableResourceID(namespace, table), scalingType));
      deleteScalingPolicyRequestList.add(
          buildDeleteScalingPolicyRequest(getTableResourceID(namespace, table), scalingType));
    }

    // write, read scaling of global indexes (secondary indexes)
    Set<String> secondaryIndexes = tableMetadata.getSecondaryIndexNames();
    for (String secondaryIndex : secondaryIndexes) {
      for (String scalingType : SECONDARY_INDEX_SCALING_TYPE_SET) {
        deregisterScalableTargetRequestList.add(
            buildDeregisterScalableTargetRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
        deleteScalingPolicyRequestList.add(
            buildDeleteScalingPolicyRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
      }
    }

    // request
    for (DeleteScalingPolicyRequest deleteScalingPolicyRequest : deleteScalingPolicyRequestList) {
      try {
        applicationAutoScalingClient.deleteScalingPolicy(deleteScalingPolicyRequest);
      } catch (Exception e) {
        if (!(e instanceof ObjectNotFoundException)) {
          LOGGER.warn(
              "The scaling policy " + deleteScalingPolicyRequest.policyName() + " is not found.");
        }
      }
    }

    for (DeregisterScalableTargetRequest deregisterScalableTargetRequest :
        deregisterScalableTargetRequestList) {
      try {
        applicationAutoScalingClient.deregisterScalableTarget(deregisterScalableTargetRequest);
      } catch (Exception e) {
        if (!(e instanceof ObjectNotFoundException)) {
          LOGGER.warn(
              "The scalable target "
                  + deregisterScalableTargetRequest.resourceId()
                  + " is not found");
        }
      }
    }
  }

  private DeregisterScalableTargetRequest buildDeregisterScalableTargetRequest(
      String resourceID, String type) {
    return DeregisterScalableTargetRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
        .build();
  }

  private DeleteScalingPolicyRequest buildDeleteScalingPolicyRequest(
      String resourceID, String type) {
    return DeleteScalingPolicyRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
        .policyName(getPolicyName(resourceID, type))
        .build();
  }

  private void deleteTableMetadata(String namespace, String table) throws ExecutionException {
    Map<String, AttributeValue> keyToDelete = new HashMap<>();
    keyToDelete.put(
        METADATA_ATTR_TABLE,
        AttributeValue.builder().s(getFullTableName(namespace, table)).build());
    try {
      client.deleteItem(
          DeleteItemRequest.builder().tableName(getMetadataTable()).key(keyToDelete).build());
    } catch (Exception e) {
      throw new ExecutionException("deleting the metadata failed", e);
    }

    ScanResponse scanResponse;
    try {
      scanResponse =
          client.scan(ScanRequest.builder().tableName(getMetadataTable()).limit(1).build());
    } catch (Exception e) {
      throw new ExecutionException("scanning the metadata table failed", e);
    }

    if (scanResponse.count() == 0) {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(getMetadataTable()).build());
      } catch (Exception e) {
        throw new ExecutionException("deleting the empty metadata table failed", e);
      }
      waitForTableDeletion(metadataNamespace, METADATA_TABLE);
    }
  }

  private void waitForTableDeletion(String namespace, String tableName) throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(WAITING_DURATION_SECS, TimeUnit.SECONDS);
        Set<String> tableSet = getNamespaceTableNames(namespace);
        if (!tableSet.contains(tableName)) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException("waiting for the table deletion failed", e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    Set<String> tables = getNamespaceTableNames(namespace);
    for (String table : tables) {
      dropTable(namespace, table);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String fullTableName = getFullTableName(namespace, table);
    Map<String, AttributeValue> lastKeyEvaluated = null;
    do {
      ScanResponse scanResponse;
      try {
        scanResponse =
            client.scan(
                ScanRequest.builder()
                    .tableName(fullTableName)
                    .limit(DELETE_BATCH_SIZE)
                    .exclusiveStartKey(lastKeyEvaluated)
                    .build());
      } catch (Exception e) {
        throw new ExecutionException("scanning items from table " + fullTableName + " failed.", e);
      }

      for (Map<String, AttributeValue> item : scanResponse.items()) {
        Map<String, AttributeValue> keyToDelete = new HashMap<>();
        keyToDelete.put(PARTITION_KEY, item.get(PARTITION_KEY));
        if (item.containsKey(CLUSTERING_KEY)) {
          keyToDelete.put(CLUSTERING_KEY, item.get(CLUSTERING_KEY));
        }
        try {
          client.deleteItem(
              DeleteItemRequest.builder().tableName(fullTableName).key(keyToDelete).build());
        } catch (Exception e) {
          throw new ExecutionException("deleting item from table " + fullTableName + " failed.", e);
        }
      }
      lastKeyEvaluated = scanResponse.lastEvaluatedKey();
    } while (!lastKeyEvaluated.isEmpty());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      String fullName = getFullTableName(namespace, table);
      return readMetadata(fullName);
    } catch (RuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private TableMetadata readMetadata(String fullName) throws ExecutionException {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(METADATA_ATTR_TABLE, AttributeValue.builder().s(fullName).build());

    try {
      Map<String, AttributeValue> metadata =
          client
              .getItem(
                  GetItemRequest.builder()
                      .tableName(getMetadataTable())
                      .key(key)
                      .consistentRead(true)
                      .build())
              .item();
      if (metadata.isEmpty()) {
        // The specified table is not found
        return null;
      }
      return createTableMetadata(metadata);
    } catch (Exception e) {
      throw new ExecutionException("Failed to read the table metadata", e);
    }
  }

  private String getMetadataTable() {
    return getFullTableName(metadataNamespace, METADATA_TABLE);
  }

  private TableMetadata createTableMetadata(Map<String, AttributeValue> metadata)
      throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    for (Entry<String, AttributeValue> entry : metadata.get(METADATA_ATTR_COLUMNS).m().entrySet()) {
      builder.addColumn(entry.getKey(), convertDataType(entry.getValue().s()));
    }
    metadata.get(METADATA_ATTR_PARTITION_KEY).l().stream()
        .map(AttributeValue::s)
        .forEach(builder::addPartitionKey);
    if (metadata.containsKey(METADATA_ATTR_CLUSTERING_KEY)) {
      Map<String, AttributeValue> clusteringOrders =
          metadata.get(METADATA_ATTR_CLUSTERING_ORDERS).m();
      metadata.get(METADATA_ATTR_CLUSTERING_KEY).l().stream()
          .map(AttributeValue::s)
          .forEach(n -> builder.addClusteringKey(n, Order.valueOf(clusteringOrders.get(n).s())));
    }
    if (metadata.containsKey(METADATA_ATTR_SECONDARY_INDEX)) {
      metadata.get(METADATA_ATTR_SECONDARY_INDEX).ss().forEach(builder::addSecondaryIndex);
    }
    return builder.build();
  }

  private DataType convertDataType(String columnType) throws ExecutionException {
    switch (columnType) {
      case "int":
        return DataType.INT;
      case "bigint":
        return DataType.BIGINT;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "text":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      default:
        throw new ExecutionException("unknown column type: " + columnType);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      Set<String> tableSet = new HashSet<>();
      String lastEvaluatedTableName = null;
      do {
        ListTablesRequest listTablesRequest =
            ListTablesRequest.builder().exclusiveStartTableName(lastEvaluatedTableName).build();
        ListTablesResponse listTablesResponse = client.listTables(listTablesRequest);
        lastEvaluatedTableName = listTablesResponse.lastEvaluatedTableName();
        List<String> tableNames = listTablesResponse.tableNames();
        String prefix = namespace + ".";
        for (String tableName : tableNames) {
          if (tableName.startsWith(prefix)) {
            tableSet.add(tableName.substring(prefix.length()));
          }
        }
      } while (lastEvaluatedTableName != null);

      return tableSet;
    } catch (Exception e) {
      throw new ExecutionException("getting list of tables failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    boolean namespaceExists = false;
    try {
      ListTablesResponse listTablesResponse = client.listTables();
      List<String> tableNames = listTablesResponse.tableNames();
      for (String tableName : tableNames) {
        if (tableName.startsWith(namespace)) {
          namespaceExists = true;
          break;
        }
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException("getting list of namespaces failed", e);
    }
    return namespaceExists;
  }

  @Override
  public void close() {
    client.close();
    applicationAutoScalingClient.close();
  }
}
