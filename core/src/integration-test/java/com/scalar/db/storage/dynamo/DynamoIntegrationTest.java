package com.scalar.db.storage.dynamo;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.IntegrationTestBase;
import java.net.URI;
import java.util.ArrayList;
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
import org.junit.Ignore;
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
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class DynamoIntegrationTest extends IntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String PARTITION_KEY = "concatenatedPartitionKey";
  private static final String CLUSTERING_KEY = "concatenatedClusteringKey";

  private static Optional<String> namespacePrefix;
  private static DynamoDbClient client;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() {
    // truncate the TABLE
    ScanRequest request =
        ScanRequest.builder().tableName(table(NAMESPACE, TABLE)).consistentRead(true).build();
    client
        .scan(request)
        .items()
        .forEach(
            i -> {
              Map<String, AttributeValue> key = new HashMap<>();
              key.put(PARTITION_KEY, i.get(PARTITION_KEY));
              key.put(CLUSTERING_KEY, i.get(CLUSTERING_KEY));
              DeleteItemRequest delRequest =
                  DeleteItemRequest.builder().tableName(table(NAMESPACE, TABLE)).key(key).build();
              client.deleteItem(delRequest);
            });
  }

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}

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
    createUserTable();

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
    storage = new Dynamo(new DynamoConfig(props));
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String table(String database, String table) {
    return namespacePrefix() + database + "." + table;
  }

  @AfterClass
  public static void tearDownAfterClass() {
    client.deleteTable(DeleteTableRequest.builder().tableName(table(NAMESPACE, TABLE)).build());

    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
  }

  private static void createUserTable() {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(NAMESPACE, TABLE));

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
            .attributeName(COL_NAME4)
            .attributeType(ScalarAttributeType.N)
            .build());
    attributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(COL_NAME3)
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
        KeySchemaElement.builder().attributeName(COL_NAME4).keyType(KeyType.RANGE).build());
    LocalSecondaryIndex index =
        LocalSecondaryIndex.builder()
            .indexName(NAMESPACE + "." + TABLE + ".index." + COL_NAME4)
            .keySchema(indexKeys)
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build();
    builder.localSecondaryIndexes(index);

    List<KeySchemaElement> globalIndexKeys = new ArrayList<>();
    globalIndexKeys.add(
        KeySchemaElement.builder().attributeName(COL_NAME3).keyType(KeyType.HASH).build());
    GlobalSecondaryIndex globalIndex =
        GlobalSecondaryIndex.builder()
            .indexName(NAMESPACE + "." + TABLE + ".global_index." + COL_NAME3)
            .keySchema(globalIndexKeys)
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .build();
    builder.globalSecondaryIndexes(globalIndex);

    client.createTable(builder.build());
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
    Map<String, AttributeValue> values = new HashMap<>();
    values.put("table", AttributeValue.builder().s(table(NAMESPACE, TABLE)).build());
    values.put(
        "partitionKey",
        AttributeValue.builder().l(AttributeValue.builder().s(COL_NAME1).build()).build());
    values.put(
        "clusteringKey",
        AttributeValue.builder().l(AttributeValue.builder().s(COL_NAME4).build()).build());
    values.put("secondaryIndex", AttributeValue.builder().ss(COL_NAME3).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(COL_NAME1, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME2, AttributeValue.builder().s("text").build());
    columns.put(COL_NAME3, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME4, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME5, AttributeValue.builder().s("boolean").build());
    values.put("columns", AttributeValue.builder().m(columns).build());

    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(table(METADATA_DATABASE, METADATA_TABLE))
            .item(values)
            .build();

    client.putItem(request);
  }
}
