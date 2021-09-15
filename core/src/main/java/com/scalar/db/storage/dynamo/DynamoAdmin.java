package com.scalar.db.storage.dynamo;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoverySpecification;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;

@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoAdmin.class);
  private static final String PARTITION_KEY = "concatenatedPartitionKey";
  private static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  private static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";
  private static final int CREATING_WAITING_TIME = 3000;
  private static final int COOL_TIME_SEC = 60;
  private static final double TARGET_USAGE_RATE = 70.0;
  private static final int DELETE_BATCH_SIZE = 100;

  private static final String NO_SCALING = "no-scaling";
  private static final String NO_BACKUP = "no-backup";
  private static final String REQUEST_UNIT = "ru";
  private static final Boolean DEFAULT_NO_SCALING = false;
  private static final Boolean DEFAULT_NO_BACKUP = false;
  private static final long DEFAULT_RU = 10;

  private static final String SCALING_TYPE_READ = "read";
  private static final String SCALING_TYPE_WRITE = "write";
  private static final String SCALING_TYPE_INDEX_READ = "index-read";
  private static final String SCALING_TYPE_INDEX_WRITE = "index-write";

  private static final ImmutableMap<DataType, ScalarAttributeType> DATATYPE_MAP =
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
  private final Optional<String> namespacePrefix;
  private final DynamoTableMetadataManager metadataManager;
  private final ApplicationAutoScalingClient applicationAutoScalingClient;

  @Inject
  public DynamoAdmin(DynamoConfig config) {
    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                config.getUsername().orElse(null), config.getPassword().orElse(null)));

    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    client =
        builder
            .credentialsProvider(awsCredentialsProvider)
            .region(Region.of(config.getContactPoints().get(0)))
            .build();

    ApplicationAutoScalingClientBuilder applicationAutoScalingClientBuilder =
        ApplicationAutoScalingClient.builder();
    config
        .getEndpointOverride()
        .ifPresent(e -> applicationAutoScalingClientBuilder.endpointOverride(URI.create(e)));
    applicationAutoScalingClientBuilder.credentialsProvider(awsCredentialsProvider);
    applicationAutoScalingClientBuilder.region(Region.of(config.getContactPoints().get(0)));
    applicationAutoScalingClient = applicationAutoScalingClientBuilder.build();

    namespacePrefix = config.getNamespacePrefix();
    metadataManager = new DynamoTableMetadataManager(client, namespacePrefix);
  }

  @VisibleForTesting
  DynamoAdmin(DynamoTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    client = null;
    applicationAutoScalingClient = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
  }

  @VisibleForTesting
  DynamoAdmin(
      DynamoDbClient dynamoDbClient,
      ApplicationAutoScalingClient applicationAutoScalingClient,
      DynamoTableMetadataManager metadataManager,
      Optional<String> namespacePrefix) {
    this.client = dynamoDbClient;
    this.applicationAutoScalingClient = applicationAutoScalingClient;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix;
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
    CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();

    List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
    makeAttribute(PARTITION_KEY, metadata, columnsToAttributeDefinitions);
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      makeAttribute(CLUSTERING_KEY, metadata, columnsToAttributeDefinitions);
      for (String clusteringKey : metadata.getClusteringKeyNames()) {
        makeAttribute(clusteringKey, metadata, columnsToAttributeDefinitions);
      }
    }
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
      for (String secondaryIndex : metadata.getSecondaryIndexNames()) {
        makeAttribute(secondaryIndex, metadata, columnsToAttributeDefinitions);
      }
    }
    requestBuilder.attributeDefinitions(columnsToAttributeDefinitions);

    // build keys
    buildPrimaryKey(requestBuilder, metadata);

    // build local indexes that corresponding to clustering keys
    buildLocalIndexes(namespace, table, requestBuilder, metadata);

    // build secondary indexes
    long ru = DEFAULT_RU;
    if (options.containsKey(REQUEST_UNIT)) {
      ru = Long.parseLong(options.get(REQUEST_UNIT));
    }
    buildGlobalIndexes(namespace, table, requestBuilder, metadata, ru);

    // ru
    requestBuilder.provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());

    // table name
    requestBuilder.tableName(getFullTableName(namespacePrefix, namespace, table));

    // create table
    try {
      client.createTable(requestBuilder.build());
      metadataManager.addTableMetadata(namespace, table, metadata);
    } catch (Exception e) {
      throw new ExecutionException("creating table failed", e);
    }

    while (true) {
      try {
        Uninterruptibles.sleepUninterruptibly(CREATING_WAITING_TIME, TimeUnit.MILLISECONDS);
        DescribeTableRequest describeTableRequest =
            DescribeTableRequest.builder()
                .tableName(getFullTableName(namespacePrefix, namespace, table))
                .build();
        DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);
        if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
          break;
        }
      } catch (DynamoDbException e) {
        throw new ExecutionException("getting table description failed", e);
      }
    }

    // scaling control
    boolean noScaling = DEFAULT_NO_SCALING;
    if (options.containsKey(NO_SCALING)) {
      if (Boolean.parseBoolean(options.get(NO_SCALING))) {
        noScaling = true;
      }
    }
    if (!noScaling) {
      enableAutoScaling(namespace, table, metadata.getSecondaryIndexNames(), ru);
    }

    // backup control
    boolean noBackup = DEFAULT_NO_BACKUP;
    if (options.containsKey(NO_BACKUP)) {
      if (Boolean.parseBoolean(options.get(NO_BACKUP))) {
        noBackup = true;
      }
    }
    if (!noBackup) {
      enableContinuousBackup(namespace, table);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    disableAutoScaling(namespace, table);
    DeleteTableRequest request =
        DeleteTableRequest.builder()
            .tableName(getFullTableName(namespacePrefix, namespace, table))
            .build();
    try {
      client.deleteTable(request);
      metadataManager.deleteTableMetadata(namespace, table);
    } catch (Exception e) {
      if (e instanceof ResourceNotFoundException) {
        LOGGER.warn("table " + request.tableName() + " not existed for deleting");
      } else {
        throw new ExecutionException("deleting table " + request.tableName() + " failed", e);
      }
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
    Map<String, AttributeValue> lastKeyEvaluated = null;
    do {
      ScanRequest scanRequest =
          ScanRequest.builder()
              .tableName(getFullTableName(namespacePrefix, namespace, table))
              .limit(DELETE_BATCH_SIZE)
              .exclusiveStartKey(lastKeyEvaluated)
              .build();
      ScanResponse scanResponse = client.scan(scanRequest);
      for (Map<String, AttributeValue> item : scanResponse.items()) {
        Map<String, AttributeValue> keyToDelete = new HashMap<>();
        keyToDelete.put(PARTITION_KEY, item.get(PARTITION_KEY));
        if (item.containsKey(CLUSTERING_KEY)) {
          keyToDelete.put(CLUSTERING_KEY, item.get(CLUSTERING_KEY));
        }
        DeleteItemRequest deleteItemRequest =
            DeleteItemRequest.builder()
                .tableName(getFullTableName(namespacePrefix, namespace, table))
                .key(keyToDelete)
                .build();
        try {
          client.deleteItem(deleteItemRequest);
        } catch (DynamoDbException e) {
          throw new ExecutionException(
              "Delete item from table "
                  + getFullTableName(namespacePrefix, namespace, table)
                  + " failed.",
              e);
        }
      }
      lastKeyEvaluated = scanResponse.lastEvaluatedKey();
    } while (!lastKeyEvaluated.isEmpty());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      return metadataManager.getTableNames(namespace);
    } catch (RuntimeException e) {
      throw new ExecutionException("getting list of tables failed");
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    boolean namespaceExists = false;
    try {
      ListTablesResponse listTablesResponse = client.listTables();
      List<String> tableNames = listTablesResponse.tableNames();
      for (String tableName : tableNames) {
        if (tableName.startsWith(getFullNamespaceName(namespacePrefix, namespace))) {
          namespaceExists = true;
          break;
        }
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException("getting list of namespaces failed");
    }
    return namespaceExists;
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  private void makeAttribute(
      String column,
      TableMetadata metadata,
      List<AttributeDefinition> columnsToAttributeDefinitions)
      throws ExecutionException {
    if (metadata.getColumnDataType(column) == DataType.BOOLEAN) {
      throw new ExecutionException(
          "BOOLEAN type is not supported for a clustering key or a secondary index in DynamoDB");
    } else {
      ScalarAttributeType columnType;
      if (column.equals(PARTITION_KEY) || column.equals(CLUSTERING_KEY)) {
        columnType = ScalarAttributeType.S;
      } else {
        columnType = DATATYPE_MAP.get(metadata.getColumnDataType(column));
      }
      columnsToAttributeDefinitions.add(
          AttributeDefinition.builder().attributeName(column).attributeType(columnType).build());
    }
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

  private void buildLocalIndexes(
      String namespace,
      String table,
      CreateTableRequest.Builder requestBuilder,
      TableMetadata metadata) {
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      List<LocalSecondaryIndex> localSecondaryIndexList = new ArrayList<>();
      for (String clusteringKey : metadata.getClusteringKeyNames()) {
        LocalSecondaryIndex.Builder localSecondaryIndexBuilder =
            LocalSecondaryIndex.builder()
                .indexName(getLocalIndexName(namespace, table, clusteringKey))
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(PARTITION_KEY)
                        .keyType(KeyType.HASH)
                        .build(),
                    KeySchemaElement.builder()
                        .attributeName(clusteringKey)
                        .keyType(KeyType.RANGE)
                        .build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build());
        localSecondaryIndexList.add(localSecondaryIndexBuilder.build());
      }
      requestBuilder.localSecondaryIndexes(localSecondaryIndexList);
    }
  }

  private void buildGlobalIndexes(
      String namespace,
      String table,
      CreateTableRequest.Builder requestBuilder,
      TableMetadata metadata,
      long ru) {
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
      List<GlobalSecondaryIndex> globalSecondaryIndexList = new ArrayList<>();
      for (String secondaryIndex : metadata.getSecondaryIndexNames()) {
        GlobalSecondaryIndex.Builder globalSecondaryIndexBuilder =
            GlobalSecondaryIndex.builder()
                .indexName(getGlobalIndexName(namespace, table, secondaryIndex))
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(secondaryIndex)
                        .keyType(KeyType.HASH)
                        .build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build());
        globalSecondaryIndexBuilder.provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());
        globalSecondaryIndexList.add(globalSecondaryIndexBuilder.build());
      }
      requestBuilder.globalSecondaryIndexes(globalSecondaryIndexList);
    }
  }

  private void enableContinuousBackup(String namespace, String table) throws ExecutionException {
    try {
      client.updateContinuousBackups(buildUpdateContinuousBackupsRequest(namespace, table));
    } catch (Exception e) {
      throw new ExecutionException(
          "Unable to enable continuous backup for "
              + getFullTableName(namespacePrefix, namespace, table),
          e);
    }
  }

  private PointInTimeRecoverySpecification buildPointInTimeRecoverySpecification() {
    return PointInTimeRecoverySpecification.builder().pointInTimeRecoveryEnabled(true).build();
  }

  private UpdateContinuousBackupsRequest buildUpdateContinuousBackupsRequest(
      String namespace, String table) {
    return UpdateContinuousBackupsRequest.builder()
        .tableName(getFullTableName(namespacePrefix, namespace, table))
        .pointInTimeRecoverySpecification(buildPointInTimeRecoverySpecification())
        .build();
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
      } catch (ApplicationAutoScalingException e) {
        throw new ExecutionException(
            "Unable to register scalable target for " + registerScalableTargetRequest.resourceId(),
            e);
      }
    }

    for (PutScalingPolicyRequest putScalingPolicyRequest : putScalingPolicyRequestList) {
      try {
        applicationAutoScalingClient.putScalingPolicy(putScalingPolicyRequest);
      } catch (ApplicationAutoScalingException e) {
        throw new ExecutionException(
            "Unable to put scaling policy request for " + putScalingPolicyRequest.resourceId(), e);
      }
    }
  }

  public void disableAutoScaling(String namespace, String table) {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      return;
    }
    Set<String> secondaryIndexes = tableMetadata.getSecondaryIndexNames();
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
      } catch (ApplicationAutoScalingException e) {
        if (!(e instanceof ObjectNotFoundException)) {
          LOGGER.warn(
              "Delete scaling policy " + deleteScalingPolicyRequest.policyName() + " failed. " + e);
        }
      }
    }
    for (DeregisterScalableTargetRequest deregisterScalableTargetRequest :
        deregisterScalableTargetRequestList) {
      try {
        applicationAutoScalingClient.deregisterScalableTarget(deregisterScalableTargetRequest);
      } catch (ApplicationAutoScalingException e) {
        if (!(e instanceof ObjectNotFoundException)) {
          LOGGER.warn(
              "Deregister scalable target "
                  + deregisterScalableTargetRequest.resourceId()
                  + " failed. "
                  + e);
        }
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

  private DeregisterScalableTargetRequest buildDeregisterScalableTargetRequest(
      String resourceID, String type) {
    return DeregisterScalableTargetRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
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

  private DeleteScalingPolicyRequest buildDeleteScalingPolicyRequest(
      String resourceID, String type) {
    return DeleteScalingPolicyRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAP.get(type))
        .policyName(getPolicyName(resourceID, type))
        .build();
  }

  private String getPolicyName(String resourceID, String type) {
    return resourceID + "-" + type;
  }

  private String getTableResourceID(String namespace, String table) {
    return "table/" + getFullTableName(namespacePrefix, namespace, table);
  }

  private String getGlobalIndexResourceID(String namespace, String table, String globalIndex) {
    return "table/"
        + getFullTableName(namespacePrefix, namespace, table)
        + "/index/"
        + getGlobalIndexName(namespace, table, globalIndex);
  }

  private TargetTrackingScalingPolicyConfiguration getScalingPolicyConfiguration(String type) {
    return TargetTrackingScalingPolicyConfiguration.builder()
        .predefinedMetricSpecification(
            PredefinedMetricSpecification.builder()
                .predefinedMetricType(SCALING_POLICY_METRIC_TYPE_MAP.get(type))
                .build())
        .scaleInCooldown(COOL_TIME_SEC)
        .scaleOutCooldown(COOL_TIME_SEC)
        .targetValue(TARGET_USAGE_RATE)
        .build();
  }

  private String getLocalIndexName(String namespace, String tableName, String keyName) {
    return getFullTableName(namespacePrefix, namespace, tableName)
        + "."
        + INDEX_NAME_PREFIX
        + "."
        + keyName;
  }

  private String getGlobalIndexName(String namespace, String tableName, String keyName) {
    return getFullTableName(namespacePrefix, namespace, tableName)
        + "."
        + GLOBAL_INDEX_NAME_PREFIX
        + "."
        + keyName;
  }

  @Override
  public void close() {
    client.close();
  }
}
