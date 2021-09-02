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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClientBuilder;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricType;
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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest.Builder;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {
  static final String PARTITION_KEY = "concatenatedPartitionKey";
  static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";

  private final int COOL_TIME_SEC = 60;
  private final double TARGET_USAGE_RATE = 70.0;

  private final DynamoDbClient client;
  private ApplicationAutoScalingClient applicationAutoScalingClient;
  private final Optional<String> namespacePrefix;
  private final DynamoTableMetadataManager metadataManager;

  private final String NO_SCALING = "no-scaling";
  private final String NO_BACKUP = "no-backup";
  private final String RU = "ru";

  private final Boolean DEFAULT_NO_SCALING = false;
  private final Boolean DEFAULT_NO_BACKUP = false;
  private final int DEFAULT_RU = 10;

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

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    Builder requestBuilder = CreateTableRequest.builder();

    List<AttributeDefinition> columnsToAttributeDefinitions = new LinkedList<>();
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
        if (options.containsKey(RU)) {
          Long ru = Long.parseLong(options.get(RU));
          globalSecondaryIndexBuilder.provisionedThroughput(
              ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());
        }
        requestBuilder.globalSecondaryIndexes(globalSecondaryIndexBuilder.build());
      }
    }

    // ru
    Long ru = null;
    if (options.containsKey(RU)) {
      ru = Long.parseLong(options.get(RU));
      requestBuilder.provisionedThroughput(
          ProvisionedThroughput.builder().readCapacityUnits(ru).writeCapacityUnits(ru).build());
    }

    // table name
    requestBuilder.tableName(Utility.getFullTableName(namespacePrefix, namespace, table));

    // scaling control
    boolean noScaling = DEFAULT_NO_SCALING;
    if (options.containsKey(NO_SCALING)) {
      if (Boolean.parseBoolean(options.get(NO_SCALING))) {
        noScaling = true;
      }
    }
    controlAutoScaling(
        namespace, table, noScaling, metadata.getSecondaryIndexNames(), Optional.ofNullable(ru));

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
    } catch (DynamoDbException e) {
      throw new ExecutionException("creating table failed");
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
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
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  private void controlAutoScaling(
      String namespace,
      String table,
      boolean noScaling,
      Set<String> secondaryIndexes,
      Optional<Long> ru)
      throws ExecutionException {
    if (!noScaling) {
      List<RegisterScalableTargetRequest> registerScalableTargetRequestList = new LinkedList<>();
      List<PutScalingPolicyRequest> putScalingPolicyRequestList = new LinkedList<>();

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
      // TODO
    }
  }

  private RegisterScalableTargetRequest buildRegisterScalableTargetRequest(
      String resourceID, String type, Optional<Long> ru) {
    int ruValue = DEFAULT_RU;
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

  private String getPolicyName(String resourceID, String type) {
    return resourceID + "-" + type;
  }

  private void controlContinuousBackup(String namespace, String table, boolean noBackup) {}

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
