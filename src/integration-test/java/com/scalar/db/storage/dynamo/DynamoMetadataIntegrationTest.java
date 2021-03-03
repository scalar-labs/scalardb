package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.storage.common.metadata.DataType;
import org.junit.After;
import org.junit.Before;
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

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoMetadataIntegrationTest {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String DATABASE = "integration_testing";
  private static final String TABLE = "test_table";

  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";
  private static final String COL_NAME8 = "c8";
  private static final String COL_NAME9 = "c9";
  private static final String COL_NAME10 = "c10";
  private static final String COL_NAME11 = "c11";

  private Optional<String> namespacePrefix;
  private DynamoDbClient client;

  @Before
  public void setUp() throws Exception {
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
    values.put("table", AttributeValue.builder().s(table(DATABASE, TABLE)).build());
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
  }

  @After
  public void tearDown() throws Exception {
    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
  }

  private Optional<String> namespacePrefix() {
    return namespacePrefix.map(n -> n + "_");
  }

  private String database(String database) {
    return namespacePrefix().orElse("") + database;
  }

  private String table(String database, String table) {
    return database(database) + "." + table;
  }

  @Test
  public void test() throws Exception {
    TableMetadataManager tableMetadataManager = new TableMetadataManager(client, namespacePrefix());

    Get dummyOperation = new Get(new Key()).forNamespace(DATABASE).forTable(TABLE);
    namespacePrefix.ifPresent(n -> dummyOperation.forNamespacePrefix(namespacePrefix().get()));
    DynamoTableMetadata tableMetadata = tableMetadataManager.getTableMetadata(dummyOperation);

    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME2);
    assertThat(iterator.next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME4);
    assertThat(iterator.next()).isEqualTo(COL_NAME3);

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(11);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME6)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME7)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME8)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME9)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME10)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME11)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME6)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME7)).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME8)).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME9)).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType(COL_NAME10)).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType(COL_NAME11)).isEqualTo(DataType.BLOB);

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

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME6)).isTrue();
  }
}
