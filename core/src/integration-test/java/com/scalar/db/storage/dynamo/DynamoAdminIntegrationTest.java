package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class DynamoAdminIntegrationTest extends AdminIntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";

  private static Optional<String> namespacePrefix;
  private static DynamoDbClient client;
  private static DistributedStorageAdmin admin;

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }

  @Test
  @Override
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringOrders()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, TABLE);

    // Fow now, the clustering order is always ASC in the DynamoDB adapter
    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME6)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME7)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME8)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME9)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME10)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME11)).isNull();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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

    // create a metadata table
    CreateTableRequest createTableRequest =
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

    client.createTable(createTableRequest);

    // wait for the creation
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // insert metadata
    Map<String, AttributeValue> values = new HashMap<>();
    values.put("table", AttributeValue.builder().s(table(NAMESPACE, TABLE)).build());
    values.put(
        "partitionKey",
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s(COL_NAME2).build(),
                    AttributeValue.builder().s(COL_NAME1).build()))
            .build());
    values.put(
        "clusteringKey",
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s(COL_NAME4).build(),
                    AttributeValue.builder().s(COL_NAME3).build()))
            .build());
    values.put(
        "secondaryIndex", AttributeValue.builder().ss(Arrays.asList(COL_NAME5, COL_NAME6)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(COL_NAME1, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME2, AttributeValue.builder().s("text").build());
    columns.put(COL_NAME3, AttributeValue.builder().s("text").build());
    columns.put(COL_NAME4, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME5, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME6, AttributeValue.builder().s("text").build());
    columns.put(COL_NAME7, AttributeValue.builder().s("bigint").build());
    columns.put(COL_NAME8, AttributeValue.builder().s("float").build());
    columns.put(COL_NAME9, AttributeValue.builder().s("double").build());
    columns.put(COL_NAME10, AttributeValue.builder().s("boolean").build());
    columns.put(COL_NAME11, AttributeValue.builder().s("blob").build());
    values.put("columns", AttributeValue.builder().m(columns).build());

    PutItemRequest putItemRequest =
        PutItemRequest.builder()
            .tableName(table(METADATA_DATABASE, METADATA_TABLE))
            .item(values)
            .build();

    client.putItem(putItemRequest);

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoDatabaseConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    admin = new DynamoAdmin(new DynamoDatabaseConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
    admin.close();
  }

  private static Optional<String> namespacePrefix() {
    return namespacePrefix.map(n -> n + "_");
  }

  private static String table(String database, String table) {
    return namespacePrefix().orElse("") + database + "." + table;
  }
}
