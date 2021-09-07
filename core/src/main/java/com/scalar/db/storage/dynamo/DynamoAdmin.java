package com.scalar.db.storage.dynamo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.Utility;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
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
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;

@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {
  static final Logger LOGGER = LoggerFactory.getLogger(DynamoAdmin.class);
  static final String PARTITION_KEY = "concatenatedPartitionKey";
  static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";

  private final int COOL_TIME_SEC = 60;
  private final double TARGET_USAGE_RATE = 70.0;

  private final DynamoDbClient client;
  private final Optional<String> namespacePrefix;
  private final DynamoTableMetadataManager metadataManager;
  private final String NO_SCALING = "no-scaling";
  private final String NO_BACKUP = "no-backup";
  private final String REQUEST_UNIT = "ru";
  private final Boolean DEFAULT_NO_SCALING = false;
  private final Boolean DEFAULT_NO_BACKUP = false;
  private final long DEFAULT_RU = 10;
  private final int DELETE_BATCH_SIZE = 100;
  private final ImmutableMap<DataType, ScalarAttributeType> DATATYPE_MAPPING =
      ImmutableMap.<DataType, ScalarAttributeType>builder()
          .put(DataType.INT, ScalarAttributeType.N)
          .put(DataType.BIGINT, ScalarAttributeType.N)
          .put(DataType.FLOAT, ScalarAttributeType.N)
          .put(DataType.DOUBLE, ScalarAttributeType.N)
          .put(DataType.TEXT, ScalarAttributeType.S)
          .put(DataType.BLOB, ScalarAttributeType.B)
          .build();
  private final ImmutableSet<String> TABLE_SCALING_TYPE =
      ImmutableSet.<String>builder().add("read").add("write").build();
  private final ImmutableSet<String> SECONDARY_INDEX_SCALING_TYPE =
      ImmutableSet.<String>builder().add("index-read").add("index-write").build();
  private final ImmutableMap<String, ScalableDimension> SCALABLE_DIMENSION_MAPPING =
      ImmutableMap.<String, ScalableDimension>builder()
          .put("read", ScalableDimension.DYNAMODB_TABLE_READ_CAPACITY_UNITS)
          .put("write", ScalableDimension.DYNAMODB_TABLE_WRITE_CAPACITY_UNITS)
          .put("index-read", ScalableDimension.DYNAMODB_INDEX_READ_CAPACITY_UNITS)
          .put("index-write", ScalableDimension.DYNAMODB_INDEX_READ_CAPACITY_UNITS)
          .build();
  private ApplicationAutoScalingClient applicationAutoScalingClient;

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
    LOGGER.info(
        "In Dynamo DB storage, namespace will be added to table name as prefix along with dot separator.");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();

    List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
    for (String column : metadata.getColumnNames()) {
      if (metadata.getColumnDataType(column) == DataType.BOOLEAN) {
        columnsToAttributeDefinitions.add(
            AttributeDefinition.builder().attributeName(column).attributeType("BOOL").build());
      } else {
        ScalarAttributeType columnType = DATATYPE_MAPPING.get(metadata.getColumnDataType(column));
        columnsToAttributeDefinitions.add(
            AttributeDefinition.builder().attributeName(column).attributeType(columnType).build());
      }
    }
    requestBuilder.attributeDefinitions(columnsToAttributeDefinitions);

    // build keys
    requestBuilder.keySchema(
        KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      requestBuilder.keySchema(
          KeySchemaElement.builder().attributeName(CLUSTERING_KEY).keyType(KeyType.RANGE).build());
    }

    // build local indexes that corresponding to clustering keys
    if (!metadata.getClusteringKeyNames().isEmpty()) {
      for (String clusteringKey : metadata.getClusteringKeyNames()) {
        LocalSecondaryIndex.Builder localSecondaryIndexBuilder =
            LocalSecondaryIndex.builder()
                .indexName(getLocalIndexName(namespace, table, clusteringKey))
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(clusteringKey)
                        .keyType(KeyType.HASH)
                        .build(),
                    KeySchemaElement.builder()
                        .attributeName(DynamoOperation.CLUSTERING_KEY)
                        .keyType(KeyType.RANGE)
                        .build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build());
        requestBuilder.localSecondaryIndexes(localSecondaryIndexBuilder.build());
      }
    }

    // build secondary indexes
    if (!metadata.getSecondaryIndexNames().isEmpty()) {
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
        Long ru = DEFAULT_RU;
        if (options.containsKey(REQUEST_UNIT)) {
          ru = Long.parseLong(options.get(REQUEST_UNIT));
        }
        globalSecondaryIndexBuilder.provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());
        requestBuilder.globalSecondaryIndexes(globalSecondaryIndexBuilder.build());
      }
    }

    // ru
    Long ru = DEFAULT_RU;
    if (options.containsKey(REQUEST_UNIT)) {
      ru = Long.parseLong(options.get(REQUEST_UNIT));
    }
    requestBuilder.provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());

    // table name
    requestBuilder.tableName(Utility.getFullTableName(namespacePrefix, namespace, table));

    // scaling control
    boolean noScaling = DEFAULT_NO_SCALING;
    if (options.containsKey(NO_SCALING)) {
      if (Boolean.parseBoolean(options.get(NO_SCALING))) {
        noScaling = true;
      }
    }
    if (!noScaling) {
      controlAutoScaling(
          namespace, table, false, metadata.getSecondaryIndexNames(), Optional.ofNullable(ru));
    }

    // backup control
    boolean noBackup = DEFAULT_NO_BACKUP;
    if (options.containsKey(NO_BACKUP)) {
      if (Boolean.parseBoolean(options.get(NO_BACKUP))) {
        noBackup = true;
      }
    }
    controlContinuousBackup(namespace, table, noBackup);

    try {
      client.createTable(requestBuilder.build());
      metadataManager.addTableMetadata(namespace, table, metadata);
    } catch (Exception e) {
      throw new ExecutionException("creating table failed", e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    DeleteTableRequest request =
        DeleteTableRequest.builder()
            .tableName(Utility.getFullTableName(namespacePrefix, namespace, table))
            .build();
    try {
      client.deleteTable(request);
      metadataManager.deleteTableMetadata(namespace, table);
    } catch (Exception e) {
      if (e instanceof ObjectNotFoundException) {
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
              .tableName(Utility.getFullTableName(namespacePrefix, namespace, table))
              .attributesToGet()
              .limit(DELETE_BATCH_SIZE)
              .exclusiveStartKey(lastKeyEvaluated)
              .build();
      ScanResponse scanResponse = client.scan(scanRequest);
      for (Map<String, AttributeValue> item : scanResponse.items()) {
        DeleteItemRequest deleteItemRequest =
            DeleteItemRequest.builder()
                .tableName(Utility.getFullTableName(namespacePrefix, namespace, table))
                .key(item)
                .build();

        try {
          client.deleteItem(deleteItemRequest);
        } catch (DynamoDbException e) {
          throw new ExecutionException(
              "Delete item from table "
                  + Utility.getFullTableName(namespacePrefix, namespace, table)
                  + " failed.");
        }
      }
      lastKeyEvaluated = scanResponse.lastEvaluatedKey();
    } while (lastKeyEvaluated != null);
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
    boolean nameSpaceExists = false;
    try {
      ListTablesResponse listTablesResponse = client.listTables();
      List<String> tableNames = listTablesResponse.tableNames();
      for (String tableName : tableNames) {
        if (tableName.startsWith(Utility.getFullNamespaceName(namespacePrefix, namespace))) {
          nameSpaceExists = true;
          break;
        }
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException("getting list of namespaces failed");
    }
    return nameSpaceExists;
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  private void controlContinuousBackup(String namespace, String table, boolean noBackup)
      throws ExecutionException {
    if (!noBackup) {
      try {
        client.updateContinuousBackups(buildUpdateContinuousBackupsRequest(namespace, table));
      } catch (Exception e) {
        throw new ExecutionException(
            "Unable to enable continuous backup for "
                + Utility.getFullTableName(namespacePrefix, namespace, table));
      }
    } else {
      LOGGER.info(
          "Continuous backup for "
              + Utility.getFullTableName(namespacePrefix, namespace, table)
              + " is disable by default from Dynamo DB");
    }
  }

  private PointInTimeRecoverySpecification buildPointInTimeRecoverySpecification() {
    return PointInTimeRecoverySpecification.builder().pointInTimeRecoveryEnabled(true).build();
  }

  private UpdateContinuousBackupsRequest buildUpdateContinuousBackupsRequest(
      String namespace, String table) {
    return UpdateContinuousBackupsRequest.builder()
        .tableName(Utility.getFullTableName(namespacePrefix, namespace, table))
        .pointInTimeRecoverySpecification(buildPointInTimeRecoverySpecification())
        .build();
  }

  private void controlAutoScaling(
      String namespace,
      String table,
      boolean noScaling,
      Set<String> secondaryIndexes,
      Optional<Long> ru)
      throws ExecutionException {
    if (!noScaling) {
      List<RegisterScalableTargetRequest> registerScalableTargetRequestList = new ArrayList<>();
      List<PutScalingPolicyRequest> putScalingPolicyRequestList = new ArrayList<>();

      // write, read scaling of table
      for (String scalingType : TABLE_SCALING_TYPE) {
        registerScalableTargetRequestList.add(
            buildRegisterScalableTargetRequest(
                getTableResourceID(namespace, table), scalingType, ru));
        putScalingPolicyRequestList.add(
            buildPutScalingPolicyRequest(getTableResourceID(namespace, table), scalingType));
      }

      // write, read scaling of global indexes (secondary indexes)
      for (String secondaryIndex : secondaryIndexes) {
        for (String scalingType : SECONDARY_INDEX_SCALING_TYPE) {
          registerScalableTargetRequestList.add(
              buildRegisterScalableTargetRequest(
                  getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType, ru));
          putScalingPolicyRequestList.add(
              buildPutScalingPolicyRequest(
                  getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
        }
      }

      // make request
      Iterator<RegisterScalableTargetRequest> registerScalableTargetRequestIterator =
          registerScalableTargetRequestList.iterator();
      Iterator<PutScalingPolicyRequest> putScalingPolicyRequestIterator =
          putScalingPolicyRequestList.iterator();
      while (registerScalableTargetRequestIterator.hasNext()
          && putScalingPolicyRequestIterator.hasNext()) {
        RegisterScalableTargetRequest registerScalableTargetRequest =
            registerScalableTargetRequestIterator.next();
        try {
          applicationAutoScalingClient.registerScalableTarget(registerScalableTargetRequest);
        } catch (Exception e) {
          throw new ExecutionException(
              "Unable to register scalable target for "
                  + registerScalableTargetRequest.resourceId());
        }

        PutScalingPolicyRequest putScalingPolicyRequest = putScalingPolicyRequestIterator.next();
        try {
          applicationAutoScalingClient.putScalingPolicy(putScalingPolicyRequest);
        } catch (Exception e) {
          throw new ExecutionException(
              "Unable to put scaling policy request for " + putScalingPolicyRequest.resourceId());
        }
      }

    } else {
      List<DeregisterScalableTargetRequest> deregisterScalableTargetRequestList = new ArrayList<>();
      List<DeleteScalingPolicyRequest> deleteScalingPolicyRequestList = new ArrayList<>();

      // write, read scaling of table
      for (String scalingType : TABLE_SCALING_TYPE) {
        deregisterScalableTargetRequestList.add(
            buildDeregisterScalableTargetRequest(
                getTableResourceID(namespace, table), scalingType));
        deleteScalingPolicyRequestList.add(
            buildDeleteScalingPolicyRequest(getTableResourceID(namespace, table), scalingType));
      }

      // write, read scaling of global indexes (secondary indexes)
      for (String secondaryIndex : secondaryIndexes) {
        for (String scalingType : SECONDARY_INDEX_SCALING_TYPE) {
          deregisterScalableTargetRequestList.add(
              buildDeregisterScalableTargetRequest(
                  getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
          deleteScalingPolicyRequestList.add(
              buildDeleteScalingPolicyRequest(
                  getGlobalIndexResourceID(namespace, table, secondaryIndex), scalingType));
        }
      }

      // make request
      Iterator<DeregisterScalableTargetRequest> deregisterScalableTargetRequestIterator =
          deregisterScalableTargetRequestList.iterator();
      Iterator<DeleteScalingPolicyRequest> deleteScalingPolicyRequestIterator =
          deleteScalingPolicyRequestList.iterator();
      while (deregisterScalableTargetRequestIterator.hasNext()
          && deleteScalingPolicyRequestIterator.hasNext()) {
        DeregisterScalableTargetRequest deregisterScalableTargetRequest =
            deregisterScalableTargetRequestIterator.next();
        try {
          applicationAutoScalingClient.deregisterScalableTarget(deregisterScalableTargetRequest);
        } catch (Exception e) {
          if (e instanceof ObjectNotFoundException) {
            LOGGER.warn(
                "Scalable target for "
                    + deregisterScalableTargetRequest.resourceId()
                    + " not existed for deregister");
          } else {
            throw new ExecutionException(
                "Unable to deregister scalable target for "
                    + deregisterScalableTargetRequest.resourceId());
          }
        }

        DeleteScalingPolicyRequest deleteScalingPolicyRequest =
            deleteScalingPolicyRequestIterator.next();
        try {
          applicationAutoScalingClient.deleteScalingPolicy(deleteScalingPolicyRequest);
        } catch (Exception e) {
          if (e instanceof ObjectNotFoundException) {
            LOGGER.warn(
                "Scaling policy for "
                    + deleteScalingPolicyRequest.resourceId()
                    + " not existed for deregister");
          } else {
            throw new ExecutionException(
                "Unable to delete scaling policy request for "
                    + deleteScalingPolicyRequest.resourceId());
          }
        }
      }
    }
  }

  private RegisterScalableTargetRequest buildRegisterScalableTargetRequest(
      String resourceID, String type, Optional<Long> ru) {
    int ruValue = Math.toIntExact(DEFAULT_RU);
    if (ru.isPresent()) {
      ruValue = Math.toIntExact(ru.get());
    }
    return RegisterScalableTargetRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAPPING.get(type))
        .minCapacity(ruValue > 10 ? ruValue / 10 : ruValue)
        .maxCapacity(ruValue)
        .build();
  }

  private DeregisterScalableTargetRequest buildDeregisterScalableTargetRequest(
      String resourceID, String type) {
    return DeregisterScalableTargetRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAPPING.get(type))
        .build();
  }

  private PutScalingPolicyRequest buildPutScalingPolicyRequest(String resourceID, String type) {
    return PutScalingPolicyRequest.builder()
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceID)
        .scalableDimension(SCALABLE_DIMENSION_MAPPING.get(type))
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
        .scalableDimension(SCALABLE_DIMENSION_MAPPING.get(type))
        .policyName(getPolicyName(resourceID, type))
        .build();
  }

  private String getPolicyName(String resourceID, String type) {
    return resourceID + "-" + type;
  }

  private String getTableResourceID(String namespace, String table) {
    return "table/" + Utility.getFullTableName(namespacePrefix, namespace, table);
  }

  private String getGlobalIndexResourceID(String namespace, String table, String globalIndex) {
    return "table/"
        + Utility.getFullTableName(namespacePrefix, namespace, table)
        + "/index/"
        + getGlobalIndexName(namespace, table, globalIndex);
  }

  private TargetTrackingScalingPolicyConfiguration getScalingPolicyConfiguration(String type) {
    return TargetTrackingScalingPolicyConfiguration.builder()
        .predefinedMetricSpecification(
            PredefinedMetricSpecification.builder()
                .predefinedMetricType(
                    type.contains("read")
                        ? MetricType.DYNAMO_DB_READ_CAPACITY_UTILIZATION
                        : MetricType.DYNAMO_DB_WRITE_CAPACITY_UTILIZATION)
                .build())
        .scaleInCooldown(COOL_TIME_SEC)
        .scaleOutCooldown(COOL_TIME_SEC)
        .targetValue(TARGET_USAGE_RATE)
        .build();
  }

  private String getLocalIndexName(String namespace, String tableName, String keyName) {
    return Utility.getFullTableName(namespacePrefix, namespace, tableName)
        + "."
        + INDEX_NAME_PREFIX
        + "."
        + keyName;
  }

  private String getGlobalIndexName(String namespace, String tableName, String keyName) {
    return Utility.getFullTableName(namespacePrefix, namespace, tableName)
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
