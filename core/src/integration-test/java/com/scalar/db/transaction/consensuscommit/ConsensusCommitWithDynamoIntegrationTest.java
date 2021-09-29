package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class ConsensusCommitWithDynamoIntegrationTest extends ConsensusCommitIntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String PARTITION_KEY = "concatenatedPartitionKey";
  private static final String CLUSTERING_KEY = "concatenatedClusteringKey";

  private static Optional<String> namespacePrefix;
  private static DynamoDbClient client;
  private static DistributedStorage originalStorage;
  private static DynamoConfig config;

  @Before
  public void setUp() {
    ConsensusCommitConfig consensusCommitConfig = new ConsensusCommitConfig(config.getProperties());
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, consensusCommitConfig, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() {
    truncateTable(NAMESPACE, TABLE_1);
    truncateTable(NAMESPACE, TABLE_2);
    truncateTable(Coordinator.NAMESPACE, Coordinator.TABLE);
  }

  private static void truncateTable(String namespace, String table) {
    ScanRequest request =
        ScanRequest.builder().tableName(table(namespace, table)).consistentRead(true).build();
    client
        .scan(request)
        .items()
        .forEach(
            i -> {
              Map<String, AttributeValue> key = new HashMap<>();
              key.put(PARTITION_KEY, i.get(PARTITION_KEY));
              key.put(CLUSTERING_KEY, i.get(CLUSTERING_KEY));
              DeleteItemRequest delRequest =
                  DeleteItemRequest.builder().tableName(table(namespace, table)).key(key).build();
              client.deleteItem(delRequest);
            });
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    String endpointOverride =
        System.getProperty("scalardb.dynamo.endpoint_override", "http://localhost:8000");
    String region = System.getProperty("scalardb.dynamo.region", "us-west-2");
    String accessKeyId = System.getProperty("scalardb.dynamo.access_key_id", "fakeMyKeyId");
    String secretAccessKey =
        System.getProperty("scalardb.dynamo.secret_access_key", "fakeSecretAccessKey");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    DynamoDbClientBuilder builder = DynamoDbClient.builder();

    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride));
    }

    client =
        builder
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();
    createMetadataTable();
    createCoordinatorTable();
    createUserTables();

    // wait for the creation
    Uninterruptibles.sleepUninterruptibly(15000, TimeUnit.MILLISECONDS);

    insertMetadata();

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new DynamoConfig(props);
    originalStorage = new Dynamo(config);
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String table(String database, String table) {
    return namespacePrefix() + database + "." + table;
  }

  @AfterClass
  public static void tearDownAfterClass() {
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      client.deleteTable(DeleteTableRequest.builder().tableName(table(NAMESPACE, table)).build());
    }

    client.deleteTable(
        DeleteTableRequest.builder()
            .tableName(table(Coordinator.NAMESPACE, Coordinator.TABLE))
            .build());

    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
  }

  private static void createCoordinatorTable() {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(Coordinator.NAMESPACE, Coordinator.TABLE));

    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(PARTITION_KEY)
            .attributeType(ScalarAttributeType.S)
            .build());
    builder.attributeDefinitions(attributeDefinitions);

    List<KeySchemaElement> keySchemaElements = new ArrayList<>();
    keySchemaElements.add(
        KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
    builder.keySchema(keySchemaElements);

    client.createTable(builder.build());
  }

  private static void createUserTables() {
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      CreateTableRequest.Builder builder =
          CreateTableRequest.builder()
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(10L)
                      .writeCapacityUnits(10L)
                      .build())
              .tableName(table(NAMESPACE, table));

      List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(PARTITION_KEY)
              .attributeType(ScalarAttributeType.S)
              .build());
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(CLUSTERING_KEY)
              .attributeType(ScalarAttributeType.S)
              .build());
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE)
              .attributeType(ScalarAttributeType.N)
              .build());
      builder.attributeDefinitions(attributeDefinitions);

      List<KeySchemaElement> keySchemaElements = new ArrayList<>();
      keySchemaElements.add(
          KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
      keySchemaElements.add(
          KeySchemaElement.builder().attributeName(CLUSTERING_KEY).keyType(KeyType.RANGE).build());
      builder.keySchema(keySchemaElements);

      List<KeySchemaElement> indexKeys = new ArrayList<>();
      indexKeys.add(
          KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
      indexKeys.add(
          KeySchemaElement.builder()
              .attributeName(ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE)
              .keyType(KeyType.RANGE)
              .build());
      LocalSecondaryIndex index =
          LocalSecondaryIndex.builder()
              .indexName(
                  NAMESPACE
                      + "."
                      + table
                      + ".index."
                      + ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE)
              .keySchema(indexKeys)
              .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
              .build();
      builder.localSecondaryIndexes(index);

      client.createTable(builder.build());
    }
  }

  private static void createMetadataTable() {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("table")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder().attributeName("table").keyType(KeyType.HASH).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(METADATA_DATABASE, METADATA_TABLE))
            .build();

    client.createTable(request);
  }

  private static void insertMetadata() {
    final String metadataTable = table(METADATA_DATABASE, METADATA_TABLE);

    // For the coordinator table
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(
        "table",
        AttributeValue.builder().s(table(Coordinator.NAMESPACE, Coordinator.TABLE)).build());
    values.put(
        "partitionKey",
        AttributeValue.builder().l(AttributeValue.builder().s(Attribute.ID).build()).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(Attribute.ID, AttributeValue.builder().s("text").build());
    columns.put(Attribute.STATE, AttributeValue.builder().s("int").build());
    columns.put(Attribute.CREATED_AT, AttributeValue.builder().s("bigint").build());
    values.put("columns", AttributeValue.builder().m(columns).build());

    client.putItem(PutItemRequest.builder().tableName(metadataTable).item(values).build());

    // For the user tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      values = new HashMap<>();
      values.put("table", AttributeValue.builder().s(table(NAMESPACE, table)).build());
      values.put(
          "partitionKey",
          AttributeValue.builder()
              .l(AttributeValue.builder().s(ConsensusCommitIntegrationTestBase.ACCOUNT_ID).build())
              .build());
      values.put(
          "clusteringKey",
          AttributeValue.builder()
              .l(
                  AttributeValue.builder()
                      .s(ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE)
                      .build())
              .build());
      columns = new HashMap<>();
      columns.put(
          ConsensusCommitIntegrationTestBase.ACCOUNT_ID, AttributeValue.builder().s("int").build());
      columns.put(
          ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE,
          AttributeValue.builder().s("int").build());
      columns.put(
          ConsensusCommitIntegrationTestBase.BALANCE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.ID, AttributeValue.builder().s("text").build());
      columns.put(Attribute.VERSION, AttributeValue.builder().s("int").build());
      columns.put(Attribute.STATE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.PREPARED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(Attribute.COMMITTED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(
          Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTestBase.BALANCE,
          AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_ID, AttributeValue.builder().s("text").build());
      columns.put(Attribute.BEFORE_VERSION, AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_STATE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_PREPARED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(Attribute.BEFORE_COMMITTED_AT, AttributeValue.builder().s("bigint").build());
      values.put("columns", AttributeValue.builder().m(columns).build());

      client.putItem(PutItemRequest.builder().tableName(metadataTable).item(values).build());
    }
  }
}
