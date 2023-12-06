package com.scalar.db.storage.dynamo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClientBuilder;
import software.amazon.awssdk.services.applicationautoscaling.model.ApplicationAutoScalingException;
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
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
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
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

/**
 * Manages table creating, dropping and truncating in Dynamo DB
 *
 * @author Pham Ba Thong
 */
@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {
  public static final String NO_SCALING = "no-scaling";
  public static final String NO_BACKUP = "no-backup";
  public static final String REQUEST_UNIT = "ru";
  public static final String DEFAULT_NO_SCALING = "false";
  public static final String DEFAULT_NO_BACKUP = "false";
  public static final String DEFAULT_REQUEST_UNIT = "10";
  private static final int DEFAULT_WAITING_DURATION_SECS = 3;

  @VisibleForTesting static final String PARTITION_KEY = "concatenatedPartitionKey";
  @VisibleForTesting static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  private static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";
  private static final int COOL_DOWN_DURATION_SECS = 60;
  private static final double TARGET_USAGE_RATE = 70.0;
  private static final int DELETE_BATCH_SIZE = 100;
  private static final String SCALING_TYPE_READ = "read";
  private static final String SCALING_TYPE_WRITE = "write";
  private static final String SCALING_TYPE_INDEX_READ = "index-read";
  private static final String SCALING_TYPE_INDEX_WRITE = "index-write";

  public static final String METADATA_TABLE = "metadata";
  @VisibleForTesting static final String METADATA_ATTR_PARTITION_KEY = "partitionKey";
  @VisibleForTesting static final String METADATA_ATTR_CLUSTERING_KEY = "clusteringKey";
  @VisibleForTesting static final String METADATA_ATTR_CLUSTERING_ORDERS = "clusteringOrders";
  @VisibleForTesting static final String METADATA_ATTR_SECONDARY_INDEX = "secondaryIndex";
  @VisibleForTesting static final String METADATA_ATTR_COLUMNS = "columns";
  @VisibleForTesting static final String METADATA_ATTR_TABLE = "table";
  @VisibleForTesting static final long METADATA_TABLES_REQUEST_UNIT = 1;
  public static final String NAMESPACES_TABLE = "namespaces";

  @VisibleForTesting static final String NAMESPACES_ATTR_NAME = "namespace_name";

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
  private final String namespacePrefix;
  private final int waitingDurationSecs;

  @Inject
  public DynamoAdmin(DatabaseConfig databaseConfig) {
    DynamoConfig config = new DynamoConfig(databaseConfig);
    AwsCredentialsProvider credentialsProvider = createCredentialsProvider(config);

    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    client =
        builder
            .credentialsProvider(credentialsProvider)
            .region(Region.of(config.getRegion()))
            .build();

    applicationAutoScalingClient = createApplicationAutoScalingClient(config);
    metadataNamespace = config.getNamespacePrefix().orElse("") + config.getMetadataNamespace();
    namespacePrefix = config.getNamespacePrefix().orElse("");
    waitingDurationSecs = DEFAULT_WAITING_DURATION_SECS;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  DynamoAdmin(DynamoDbClient client, DynamoConfig config) {
    this.client = client;
    applicationAutoScalingClient = createApplicationAutoScalingClient(config);
    metadataNamespace = config.getNamespacePrefix().orElse("") + config.getMetadataNamespace();
    namespacePrefix = config.getNamespacePrefix().orElse("");
    waitingDurationSecs = DEFAULT_WAITING_DURATION_SECS;
  }

  @VisibleForTesting
  DynamoAdmin(
      DynamoDbClient client,
      ApplicationAutoScalingClient applicationAutoScalingClient,
      DynamoConfig config) {
    this.client = client;
    this.applicationAutoScalingClient = applicationAutoScalingClient;
    metadataNamespace = config.getNamespacePrefix().orElse("") + config.getMetadataNamespace();
    namespacePrefix = config.getNamespacePrefix().orElse("");
    waitingDurationSecs = 0;
  }

  private AwsCredentialsProvider createCredentialsProvider(DynamoConfig config) {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey()));
  }

  private ApplicationAutoScalingClient createApplicationAutoScalingClient(DynamoConfig config) {
    ApplicationAutoScalingClientBuilder builder = ApplicationAutoScalingClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    return builder
        .credentialsProvider(createCredentialsProvider(config))
        .region(Region.of(config.getRegion()))
        .build();
  }

  @Override
  public void createNamespace(String nonPrefixedNamespace, Map<String, String> options)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      boolean noBackup = Boolean.parseBoolean(options.getOrDefault(NO_BACKUP, DEFAULT_NO_BACKUP));
      createNamespacesTableIfNotExists(noBackup);
      upsertIntoNamespacesTable(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException("Creating the " + namespace + " namespace failed", e);
    }
  }

  private void upsertIntoNamespacesTable(Namespace namespace) throws ExecutionException {
    Map<String, AttributeValue> itemValues = new HashMap<>();
    itemValues.put(NAMESPACES_ATTR_NAME, AttributeValue.builder().s(namespace.prefixed()).build());
    try {
      client.putItem(
          PutItemRequest.builder()
              .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE))
              .item(itemValues)
              .build());
    } catch (Exception e) {
      throw new ExecutionException(
          "Inserting the " + namespace + " namespace into the namespaces table failed", e);
    }
  }

  @Override
  public void createTable(
      String nonPrefixedNamespace,
      String table,
      TableMetadata metadata,
      Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(nonPrefixedNamespace, table, metadata, false, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          String.format(
              "Creating the %s table failed",
              getFullTableName(Namespace.of(namespacePrefix, nonPrefixedNamespace), table)),
          e);
    }
  }

  private void createTableInternal(
      String nonPrefixedNamespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
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
      if (!(ifNotExists && tableExistsInternal(namespace, table))) {
        client.createTable(requestBuilder.build());
        waitForTableCreation(namespace, table);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          "Creating the " + getFullTableName(namespace, table) + " table failed", e);
    }

    boolean noScaling = Boolean.parseBoolean(options.getOrDefault(NO_SCALING, DEFAULT_NO_SCALING));
    if (!noScaling) {
      enableAutoScaling(namespace, table, metadata.getSecondaryIndexNames(), ru);
    }

    boolean noBackup = Boolean.parseBoolean(options.getOrDefault(NO_BACKUP, DEFAULT_NO_BACKUP));
    if (!noBackup) {
      enableContinuousBackup(namespace, table);
    }

    createMetadataTableIfNotExists(noBackup);
    upsertTableMetadata(namespace, table, metadata);
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
            "BLOB type is supported only for the last column in partition key in DynamoDB: "
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
      Namespace namespace,
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

  private String getGlobalIndexName(Namespace namespace, String tableName, String keyName) {
    return getFullTableName(namespace, tableName) + "." + GLOBAL_INDEX_NAME_PREFIX + "." + keyName;
  }

  private void upsertTableMetadata(Namespace namespace, String table, TableMetadata metadata)
      throws ExecutionException {
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
          PutItemRequest.builder()
              .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, METADATA_TABLE))
              .item(itemValues)
              .build());
    } catch (Exception e) {
      throw new ExecutionException(
          "Adding the metadata for the " + getFullTableName(namespace, table) + " table failed", e);
    }
  }

  private void createMetadataTableIfNotExists(boolean noBackup) throws ExecutionException {
    try {
      if (!metadataTableExists()) {
        List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
        columnsToAttributeDefinitions.add(
            AttributeDefinition.builder()
                .attributeName(METADATA_ATTR_TABLE)
                .attributeType(ScalarAttributeType.S)
                .build());
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
                        .readCapacityUnits(METADATA_TABLES_REQUEST_UNIT)
                        .writeCapacityUnits(METADATA_TABLES_REQUEST_UNIT)
                        .build())
                .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, METADATA_TABLE))
                .build());
        waitForTableCreation(Namespace.of(metadataNamespace), METADATA_TABLE);
      }
    } catch (Exception e) {
      throw new ExecutionException("Creating the metadata table failed", e);
    }

    if (!noBackup) {
      enableContinuousBackup(Namespace.of(metadataNamespace), METADATA_TABLE);
    }
  }

  private boolean metadataTableExists() throws ExecutionException {
    return tableExistsInternal(Namespace.of(metadataNamespace), METADATA_TABLE);
  }

  private boolean namespacesTableExists() throws ExecutionException {
    return tableExistsInternal(Namespace.of(metadataNamespace), NAMESPACES_TABLE);
  }

  private boolean tableExistsInternal(Namespace namespace, String table) throws ExecutionException {
    try {
      client.describeTable(
          DescribeTableRequest.builder().tableName(getFullTableName(namespace, table)).build());
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new ExecutionException(
          "Checking the " + getFullTableName(namespace, table) + " table existence failed", e);
    }
  }

  private void waitForTableCreation(Namespace namespace, String table) throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(waitingDurationSecs, TimeUnit.SECONDS);
        DescribeTableResponse describeTableResponse =
            client.describeTable(
                DescribeTableRequest.builder()
                    .tableName(getFullTableName(namespace, table))
                    .build());
        if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException(
          "Waiting for the " + getFullTableName(namespace, table) + " table creation failed", e);
    }
  }

  private void enableAutoScaling(
      Namespace namespace, String table, Set<String> secondaryIndexes, long ru)
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

    registerScalableTarget(registerScalableTargetRequestList);
    putScalingPolicy(putScalingPolicyRequestList);
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

  private String getTableResourceID(Namespace namespace, String table) {
    return "table/" + getFullTableName(namespace, table);
  }

  private String getGlobalIndexResourceID(Namespace namespace, String table, String globalIndex) {
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

  private void enableContinuousBackup(Namespace namespace, String table) throws ExecutionException {
    waitForTableBackupEnabledAtCreation(namespace, table);

    try {
      client.updateContinuousBackups(buildUpdateContinuousBackupsRequest(namespace, table));
    } catch (Exception e) {
      throw new ExecutionException(
          "Unable to enable continuous backup for " + getFullTableName(namespace, table), e);
    }
  }

  private void waitForTableBackupEnabledAtCreation(Namespace namespace, String table)
      throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(waitingDurationSecs, TimeUnit.SECONDS);
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
      throw new ExecutionException(
          "Waiting for enabling the "
              + getFullTableName(namespace, table)
              + " table backup at creation failed",
          e);
    }
  }

  private PointInTimeRecoverySpecification buildPointInTimeRecoverySpecification() {
    return PointInTimeRecoverySpecification.builder().pointInTimeRecoveryEnabled(true).build();
  }

  private UpdateContinuousBackupsRequest buildUpdateContinuousBackupsRequest(
      Namespace namespace, String table) {
    return UpdateContinuousBackupsRequest.builder()
        .tableName(getFullTableName(namespace, table))
        .pointInTimeRecoverySpecification(buildPointInTimeRecoverySpecification())
        .build();
  }

  @Override
  public void dropTable(String nonPrefixedNamespace, String table) throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    disableAutoScaling(namespace, table);

    String fullTableName = getFullTableName(namespace, table);
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(fullTableName).build());
    } catch (Exception e) {
      throw new ExecutionException("Deleting the " + fullTableName + " table failed", e);
    }
    waitForTableDeletion(namespace, table);
    deleteTableMetadata(namespace, table);
  }

  private void disableAutoScaling(Namespace namespace, String table) throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace.nonPrefixed(), table);
    if (tableMetadata == null) {
      return;
    }

    List<DeleteScalingPolicyRequest> deleteScalingPolicyRequestList = new ArrayList<>();
    List<DeregisterScalableTargetRequest> deregisterScalableTargetRequestList = new ArrayList<>();

    // write, read scaling of table
    for (String scalingType : TABLE_SCALING_TYPE_SET) {
      deleteScalingPolicyRequestList.add(
          buildDeleteScalingPolicyRequest(getTableResourceID(namespace, table), scalingType));
      deregisterScalableTargetRequestList.add(
          buildDeregisterScalableTargetRequest(getTableResourceID(namespace, table), scalingType));
    }

    // write, read scaling of global indexes (secondary indexes)
    Set<String> secondaryIndexes = tableMetadata.getSecondaryIndexNames();
    for (String secondaryIndex : secondaryIndexes) {
      for (String scalingType : SECONDARY_INDEX_SCALING_TYPE_SET) {
        deleteScalingPolicyRequestList.add(
            buildDeleteScalingPolicyRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
        deregisterScalableTargetRequestList.add(
            buildDeregisterScalableTargetRequest(
                getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
      }
    }

    deleteScalingPolicy(deleteScalingPolicyRequestList);
    deregisterScalableTarget(deregisterScalableTargetRequestList);
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

  private void deleteTableMetadata(Namespace namespace, String table) throws ExecutionException {
    String metadataTable = ScalarDbUtils.getFullTableName(metadataNamespace, METADATA_TABLE);

    Map<String, AttributeValue> keyToDelete = new HashMap<>();
    keyToDelete.put(
        METADATA_ATTR_TABLE,
        AttributeValue.builder().s(getFullTableName(namespace, table)).build());
    try {
      client.deleteItem(
          DeleteItemRequest.builder().tableName(metadataTable).key(keyToDelete).build());
    } catch (Exception e) {
      throw new ExecutionException(
          "Deleting the metadata for the " + getFullTableName(namespace, table) + " table failed",
          e);
    }

    ScanResponse scanResponse;
    try {
      scanResponse = client.scan(ScanRequest.builder().tableName(metadataTable).limit(1).build());
    } catch (Exception e) {
      throw new ExecutionException("Scanning the metadata table failed", e);
    }

    if (scanResponse.count() == 0) {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(metadataTable).build());
      } catch (Exception e) {
        throw new ExecutionException("Deleting the empty metadata table failed", e);
      }
      waitForTableDeletion(Namespace.of(metadataNamespace), METADATA_TABLE);
    }
  }

  private void waitForTableDeletion(Namespace namespace, String tableName)
      throws ExecutionException {
    try {
      while (true) {
        Uninterruptibles.sleepUninterruptibly(waitingDurationSecs, TimeUnit.SECONDS);
        Set<String> tableSet = getNamespaceTableNames(namespace.nonPrefixed());
        if (!tableSet.contains(tableName)) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException(
          "Waiting for the " + getFullTableName(namespace, tableName) + " table deletion failed",
          e);
    }
  }

  @Override
  public void dropNamespace(String nonPrefixedNamespace) throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      deleteFromNamespacesTable(namespace);
      dropNamespacesTableIfEmpty();
    } catch (Exception e) {
      throw new ExecutionException("Dropping the " + namespace + " namespace failed", e);
    }
  }

  private void dropNamespacesTableIfEmpty() throws ExecutionException {
    String namespaceTableFullName =
        ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE);
    ScanResponse scanResponse =
        client.scan(ScanRequest.builder().tableName(namespaceTableFullName).limit(2).build());

    Set<String> namespaceNames = new HashSet<>();
    for (Map<String, AttributeValue> namespace : scanResponse.items()) {
      String prefixedNamespaceName = namespace.get(NAMESPACES_ATTR_NAME).s();
      namespaceNames.add(prefixedNamespaceName);
    }

    if (namespaceNames.isEmpty()
        || (namespaceNames.size() == 1 && namespaceNames.contains(metadataNamespace))) {
      client.deleteTable(DeleteTableRequest.builder().tableName(namespaceTableFullName).build());
      waitForTableDeletion(Namespace.of(metadataNamespace), NAMESPACES_TABLE);
    }
  }

  private void deleteFromNamespacesTable(Namespace namespace) throws ExecutionException {
    Map<String, AttributeValue> keyToDelete = new HashMap<>();
    keyToDelete.put(NAMESPACES_ATTR_NAME, AttributeValue.builder().s(namespace.prefixed()).build());
    String namespacesTableFullName =
        ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE);
    try {
      client.deleteItem(
          DeleteItemRequest.builder().tableName(namespacesTableFullName).key(keyToDelete).build());
    } catch (Exception e) {
      throw new ExecutionException(
          "Deleting the " + namespace + " namespace from the namespaces table failed", e);
    }
  }

  @Override
  public void truncateTable(String nonPrefixedNamespace, String table) throws ExecutionException {
    String fullTableName =
        getFullTableName(Namespace.of(namespacePrefix, nonPrefixedNamespace), table);
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
        throw new ExecutionException(
            "Scanning items from the " + fullTableName + " table failed", e);
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
          throw new ExecutionException(
              "Deleting item from the " + fullTableName + " table failed", e);
        }
      }
      lastKeyEvaluated = scanResponse.lastEvaluatedKey();
    } while (!lastKeyEvaluated.isEmpty());
  }

  @Override
  public void createIndex(
      String nonPrefixedNamespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    TableMetadata metadata = getTableMetadata(nonPrefixedNamespace, table);

    if (metadata == null) {
      throw new IllegalArgumentException(
          "The " + getFullTableName(namespace, table) + " table does not exist");
    }

    if (metadata.getColumnDataType(columnName) == DataType.BOOLEAN) {
      throw new IllegalArgumentException(
          "Currently, BOOLEAN type is not supported for a secondary index in DynamoDB: "
              + columnName);
    }

    long ru = Long.parseLong(options.getOrDefault(REQUEST_UNIT, DEFAULT_REQUEST_UNIT));

    try {
      client.updateTable(
          UpdateTableRequest.builder()
              .tableName(getFullTableName(namespace, table))
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName(columnName)
                      .attributeType(
                          SECONDARY_INDEX_DATATYPE_MAP.get(metadata.getColumnDataType(columnName)))
                      .build())
              .globalSecondaryIndexUpdates(
                  GlobalSecondaryIndexUpdate.builder()
                      .create(
                          CreateGlobalSecondaryIndexAction.builder()
                              .indexName(getGlobalIndexName(namespace, table, columnName))
                              .keySchema(
                                  KeySchemaElement.builder()
                                      .attributeName(columnName)
                                      .keyType(KeyType.HASH)
                                      .build())
                              .projection(
                                  Projection.builder().projectionType(ProjectionType.ALL).build())
                              .provisionedThroughput(
                                  ProvisionedThroughput.builder()
                                      .readCapacityUnits(ru)
                                      .writeCapacityUnits(ru)
                                      .build())
                              .build())
                      .build())
              .build());
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Creating the secondary index for the %s column of the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }

    waitForIndexCreation(namespace, table, columnName);

    // enable auto scaling
    boolean noScaling = Boolean.parseBoolean(options.getOrDefault(NO_SCALING, DEFAULT_NO_SCALING));
    if (!noScaling) {
      List<RegisterScalableTargetRequest> registerScalableTargetRequestList = new ArrayList<>();
      List<PutScalingPolicyRequest> putScalingPolicyRequestList = new ArrayList<>();

      // write, read scaling of global indexes (secondary indexes)
      for (String scalingType : SECONDARY_INDEX_SCALING_TYPE_SET) {
        registerScalableTargetRequestList.add(
            buildRegisterScalableTargetRequest(
                getGlobalIndexResourceID(namespace, table, columnName), scalingType, (int) ru));
        putScalingPolicyRequestList.add(
            buildPutScalingPolicyRequest(
                getGlobalIndexResourceID(namespace, table, columnName), scalingType));
      }

      registerScalableTarget(registerScalableTargetRequestList);
      putScalingPolicy(putScalingPolicyRequestList);
    }

    // update metadata
    TableMetadata tableMetadata = getTableMetadata(nonPrefixedNamespace, table);
    upsertTableMetadata(
        namespace,
        table,
        TableMetadata.newBuilder(tableMetadata).addSecondaryIndex(columnName).build());
  }

  private void waitForIndexCreation(Namespace namespace, String table, String columnName)
      throws ExecutionException {
    try {
      String indexName = getGlobalIndexName(namespace, table, columnName);
      while (true) {
        Uninterruptibles.sleepUninterruptibly(waitingDurationSecs, TimeUnit.SECONDS);
        DescribeTableResponse response =
            client.describeTable(
                DescribeTableRequest.builder()
                    .tableName(getFullTableName(namespace, table))
                    .build());
        for (GlobalSecondaryIndexDescription globalSecondaryIndex :
            response.table().globalSecondaryIndexes()) {
          if (globalSecondaryIndex.indexName().equals(indexName)) {
            if (globalSecondaryIndex.indexStatus() == IndexStatus.ACTIVE) {
              return;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Waiting for the secondary index creation on the %s column of the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void registerScalableTarget(
      List<RegisterScalableTargetRequest> registerScalableTargetRequestList)
      throws ExecutionException {
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
  }

  private void putScalingPolicy(List<PutScalingPolicyRequest> putScalingPolicyRequestList)
      throws ExecutionException {
    for (PutScalingPolicyRequest putScalingPolicyRequest : putScalingPolicyRequestList) {
      try {
        applicationAutoScalingClient.putScalingPolicy(putScalingPolicyRequest);
      } catch (Exception e) {
        throw new ExecutionException(
            "Unable to put scaling policy request for " + putScalingPolicyRequest.resourceId(), e);
      }
    }
  }

  @Override
  public void dropIndex(String nonPrefixedNamespace, String table, String columnName)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      client.updateTable(
          UpdateTableRequest.builder()
              .tableName(getFullTableName(namespace, table))
              .globalSecondaryIndexUpdates(
                  GlobalSecondaryIndexUpdate.builder()
                      .delete(
                          DeleteGlobalSecondaryIndexAction.builder()
                              .indexName(getGlobalIndexName(namespace, table, columnName))
                              .build())
                      .build())
              .build());
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Dropping the secondary index on the %s column of the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }

    waitForIndexDeletion(namespace, table, columnName);

    // disable auto scaling
    List<DeleteScalingPolicyRequest> deleteScalingPolicyRequestList = new ArrayList<>();
    List<DeregisterScalableTargetRequest> deregisterScalableTargetRequestList = new ArrayList<>();

    for (String scalingType : SECONDARY_INDEX_SCALING_TYPE_SET) {
      deleteScalingPolicyRequestList.add(
          buildDeleteScalingPolicyRequest(
              getGlobalIndexResourceID(namespace, table, columnName), scalingType));
      deregisterScalableTargetRequestList.add(
          buildDeregisterScalableTargetRequest(
              getGlobalIndexResourceID(namespace, table, columnName), scalingType));
    }

    deleteScalingPolicy(deleteScalingPolicyRequestList);
    deregisterScalableTarget(deregisterScalableTargetRequestList);

    // update metadata
    TableMetadata tableMetadata = getTableMetadata(nonPrefixedNamespace, table);
    upsertTableMetadata(
        namespace,
        table,
        TableMetadata.newBuilder(tableMetadata).removeSecondaryIndex(columnName).build());
  }

  private void waitForIndexDeletion(Namespace namespace, String table, String columnName)
      throws ExecutionException {
    try {
      String indexName = getGlobalIndexName(namespace, table, columnName);
      while (true) {
        Uninterruptibles.sleepUninterruptibly(waitingDurationSecs, TimeUnit.SECONDS);
        DescribeTableResponse response =
            client.describeTable(
                DescribeTableRequest.builder()
                    .tableName(getFullTableName(namespace, table))
                    .build());
        boolean deleted = true;
        for (GlobalSecondaryIndexDescription globalSecondaryIndex :
            response.table().globalSecondaryIndexes()) {
          if (globalSecondaryIndex.indexName().equals(indexName)) {
            deleted = false;
            break;
          }
        }
        if (deleted) {
          break;
        }
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Waiting for the secondary index deletion on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void deleteScalingPolicy(
      List<DeleteScalingPolicyRequest> deleteScalingPolicyRequestList) {
    for (DeleteScalingPolicyRequest deleteScalingPolicyRequest : deleteScalingPolicyRequestList) {
      try {
        applicationAutoScalingClient.deleteScalingPolicy(deleteScalingPolicyRequest);
        // Suppress exceptions when the scaling policy does not exist
      } catch (ObjectNotFoundException ignored) {
        // ObjectNotFoundException is thrown when using a regular Dynamo DB instance
      } catch (ApplicationAutoScalingException e) {
        // The auto-scaling service is not supported with Dynamo DB local. Any API call to the
        // 'applicationAutoScalingClient' will raise an ApplicationAutoScalingException
        if (!(e.awsErrorDetails().errorCode().equals("InvalidAction") && e.statusCode() == 400)) {
          throw e;
        }
      }
    }
  }

  private void deregisterScalableTarget(
      List<DeregisterScalableTargetRequest> deregisterScalableTargetRequestList) {
    for (DeregisterScalableTargetRequest deregisterScalableTargetRequest :
        deregisterScalableTargetRequestList) {
      try {
        applicationAutoScalingClient.deregisterScalableTarget(deregisterScalableTargetRequest);
        // Suppress exceptions when the scalable target does not exist
      } catch (ObjectNotFoundException ignored) {
        // ObjectNotFoundException is thrown when using a regular Dynamo DB instance
      } catch (ApplicationAutoScalingException e) {
        // The auto-scaling service is not supported with Dynamo DB local. Any API call to the
        // 'applicationAutoScalingClient' will raise an ApplicationAutoScalingException
        if (!(e.awsErrorDetails().errorCode().equals("InvalidAction") && e.statusCode() == 400)) {
          throw e;
        }
      }
    }
  }

  @Override
  public TableMetadata getTableMetadata(String nonPrefixedNamespace, String table)
      throws ExecutionException {
    String fullName = getFullTableName(Namespace.of(namespacePrefix, nonPrefixedNamespace), table);
    try {
      return readMetadata(fullName);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          "Getting a table metadata for the " + fullName + " table failed", e);
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
                      .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, METADATA_TABLE))
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
      throw new ExecutionException(
          "Failed to read the table metadata for the " + fullName + " table", e);
    }
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
        throw new ExecutionException("Unknown column type: " + columnType);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String nonPrefixedNamespace) throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      Set<String> tableSet = new HashSet<>();
      String lastEvaluatedTableName = null;
      do {
        ListTablesRequest listTablesRequest =
            ListTablesRequest.builder().exclusiveStartTableName(lastEvaluatedTableName).build();
        ListTablesResponse listTablesResponse = client.listTables(listTablesRequest);
        lastEvaluatedTableName = listTablesResponse.lastEvaluatedTableName();
        List<String> tableNames = listTablesResponse.tableNames();
        String prefix = namespace.prefixed() + ".";
        for (String tableName : tableNames) {
          if (tableName.startsWith(prefix)) {
            tableSet.add(tableName.substring(prefix.length()));
          }
        }
      } while (lastEvaluatedTableName != null);

      return tableSet;
    } catch (Exception e) {
      throw new ExecutionException(
          "Getting the list of tables of the " + namespace + " namespace failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String nonPrefixedNamespace) throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      Map<String, AttributeValue> key =
          ImmutableMap.of(
              NAMESPACES_ATTR_NAME, AttributeValue.builder().s(namespace.prefixed()).build());
      GetItemResponse response =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE))
                  .key(key)
                  .build());

      return response.hasItem();
    } catch (ResourceNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new ExecutionException("Checking the " + namespace + " namespace existence failed", e);
    }
  }

  @Override
  public void repairNamespace(String nonPrefixedNamespace, Map<String, String> options)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      boolean noBackup = Boolean.parseBoolean(options.getOrDefault(NO_BACKUP, DEFAULT_NO_BACKUP));
      createNamespacesTableIfNotExists(noBackup);
      upsertIntoNamespacesTable(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException("Repairing the " + namespace + " namespace failed", e);
    }
  }

  @Override
  public void repairTable(
      String nonPrefixedNamespace,
      String table,
      TableMetadata metadata,
      Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(nonPrefixedNamespace, table, metadata, true, options);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Repairing the %s table failed",
              getFullTableName(Namespace.of(namespacePrefix, nonPrefixedNamespace), table)),
          e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String nonPrefixedNamespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    Namespace namespace = Namespace.of(namespacePrefix, nonPrefixedNamespace);
    try {
      TableMetadata currentTableMetadata = getTableMetadata(nonPrefixedNamespace, table);
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).addColumn(columnName, columnType).build();

      upsertTableMetadata(namespace, table, updatedTableMetadata);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new %s column to the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table) {
    throw new UnsupportedOperationException(
        "Import-related functionality is not supported in DynamoDB");
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType) {
    throw new UnsupportedOperationException(
        "Import-related functionality is not supported in DynamoDB");
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options) {
    throw new UnsupportedOperationException(
        "Import-related functionality is not supported in DynamoDB");
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      Set<String> namespaceNames = new HashSet<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;
      do {
        ScanResponse scanResponse =
            client.scan(
                ScanRequest.builder()
                    .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE))
                    .exclusiveStartKey(lastEvaluatedKey)
                    .build());
        lastEvaluatedKey = scanResponse.lastEvaluatedKey();

        for (Map<String, AttributeValue> namespace : scanResponse.items()) {
          String prefixedNamespaceName = namespace.get(NAMESPACES_ATTR_NAME).s();
          String nonPrefixedNamespaceName =
              prefixedNamespaceName.substring(namespacePrefix.length());
          namespaceNames.add(nonPrefixedNamespaceName);
        }
      } while (!lastEvaluatedKey.isEmpty());

      return namespaceNames;
    } catch (ResourceNotFoundException e) {
      return Collections.emptySet();
    } catch (Exception e) {
      throw new ExecutionException("Retrieving the existing namespace names failed", e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    if (!metadataTableExists()) {
      return;
    }
    boolean noBackup = Boolean.parseBoolean(options.getOrDefault(NO_BACKUP, DEFAULT_NO_BACKUP));
    createNamespacesTableIfNotExists(noBackup);
    try {
      for (Namespace namespace : getNamespacesOfExistingTables()) {
        upsertIntoNamespacesTable(namespace);
      }
    } catch (ExecutionException e) {
      throw new ExecutionException("Upgrading the ScalarDB environment failed", e);
    }
  }

  private Set<Namespace> getNamespacesOfExistingTables() throws ExecutionException {
    Set<Namespace> namespaceNames = new HashSet<>();
    Map<String, AttributeValue> lastEvaluatedKey = null;
    do {
      ScanResponse scanResponse;
      try {
        scanResponse =
            client.scan(
                ScanRequest.builder()
                    .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, METADATA_TABLE))
                    .exclusiveStartKey(lastEvaluatedKey)
                    .build());
      } catch (RuntimeException e) {
        throw new ExecutionException(
            "Failed to retrieve the namespaces names of existing tables", e);
      }
      lastEvaluatedKey = scanResponse.lastEvaluatedKey();

      for (Map<String, AttributeValue> tableMetadata : scanResponse.items()) {
        String fullTableName = tableMetadata.get(METADATA_ATTR_TABLE).s();
        String namespaceName = fullTableName.substring(0, fullTableName.indexOf('.'));
        namespaceNames.add(Namespace.of(namespaceName));
      }
    } while (!lastEvaluatedKey.isEmpty());

    return namespaceNames;
  }

  private void createNamespacesTableIfNotExists(boolean noBackup) throws ExecutionException {
    try {
      if (!namespacesTableExists()) {
        List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
        columnsToAttributeDefinitions.add(
            AttributeDefinition.builder()
                .attributeName(NAMESPACES_ATTR_NAME)
                .attributeType(ScalarAttributeType.S)
                .build());
        client.createTable(
            CreateTableRequest.builder()
                .attributeDefinitions(columnsToAttributeDefinitions)
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(NAMESPACES_ATTR_NAME)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(METADATA_TABLES_REQUEST_UNIT)
                        .writeCapacityUnits(METADATA_TABLES_REQUEST_UNIT)
                        .build())
                .tableName(ScalarDbUtils.getFullTableName(metadataNamespace, NAMESPACES_TABLE))
                .build());
        waitForTableCreation(Namespace.of(metadataNamespace), NAMESPACES_TABLE);

        // Insert the system namespace to the namespaces table
        upsertIntoNamespacesTable(Namespace.of(metadataNamespace));
      }
    } catch (Exception e) {
      throw new ExecutionException("Creating the namespaces table failed", e);
    }

    if (!noBackup) {
      enableContinuousBackup(Namespace.of(metadataNamespace), NAMESPACES_TABLE);
    }
  }

  private String getFullTableName(Namespace namespace, String table) {
    return ScalarDbUtils.getFullTableName(namespace.prefixed(), table);
  }

  @Override
  public void close() {
    client.close();
    applicationAutoScalingClient.close();
  }
}
