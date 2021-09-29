package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamoWithReservedKeywordIntegrationTest {

  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "test_table";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "status"; // Reserved keyword

  private static Optional<String> namespacePrefix;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage dynamo;

  @Before
  public void setUp() {
    dynamo.with(NAMESPACE, TABLE);
  }

  @After
  public void tearDown() throws ExecutionException {
    // truncate the TABLE
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @Test
  public void get_GetWithReservedKeywordProjection_ShouldReturnWhatsPut()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    String status = "s0";
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME2, cKey);
    dynamo.put(new Put(partitionKey, clusteringKey).withValue(COL_NAME4, status));

    // Act
    Get get = new Get(partitionKey, clusteringKey).withProjection(COL_NAME4);
    Optional<Result> result = dynamo.get(get);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getValue(COL_NAME4)).isPresent();
    assertThat(result.get().getValue(COL_NAME4).get().getAsString().get()).isEqualTo(status);
  }

  @Test
  public void get_getWithSecondaryIndexWithReservedKeywordProjection_ShouldReturnWhatsPut()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    String status = "s0";
    String col3Value = "value3";
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME2, cKey);
    dynamo.put(
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, col3Value)
            .withValue(COL_NAME4, status));

    // Act
    Get get = new Get(new Key(COL_NAME3, col3Value)).withProjection(COL_NAME4);
    Optional<Result> result = dynamo.get(get);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getValue(COL_NAME4)).isPresent();
    assertThat(result.get().getValue(COL_NAME4).get().getAsString().get()).isEqualTo(status);
  }

  @Test
  public void scan_ScanWithReservedKeywordProjection_ShouldReturnWhatsPut()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 0);
    for (int cKey = 0; cKey < 3; cKey++) {
      Key clusteringKey = new Key(COL_NAME2, cKey);
      String status = "s" + cKey;
      dynamo.put(new Put(partitionKey, clusteringKey).withValue(COL_NAME4, status));
    }

    // Act
    Scan scan =
        new Scan(partitionKey)
            .withProjection(COL_NAME4)
            .withOrdering(new Ordering(COL_NAME2, Order.ASC));
    List<Result> results;
    try (Scanner scanner = dynamo.scan(scan)) {
      results = scanner.all();
    }

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s0");
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s1");
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s2");
  }

  @Test
  public void scan_ScanWithSecondaryIndexWithReservedKeywordProjection_ShouldReturnWhatsPut()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 0);
    String col3Value = "value3";
    for (int cKey = 0; cKey < 3; cKey++) {
      Key clusteringKey = new Key(COL_NAME2, cKey);
      String status = "s" + cKey;
      dynamo.put(
          new Put(partitionKey, clusteringKey)
              .withValue(COL_NAME3, col3Value)
              .withValue(COL_NAME4, status));
    }

    // Act
    Scan scan = new Scan(new Key(COL_NAME3, col3Value)).withProjection(COL_NAME4);
    List<Result> results;
    try (Scanner scanner = dynamo.scan(scan)) {
      results = scanner.all();
    }

    // Assert
    assertThat(results.size()).isEqualTo(3);
    for (Result result : results) {
      switch (result.getValue(COL_NAME2).get().getAsInt()) {
        case 0:
          assertThat(result.getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s0");
          break;
        case 1:
          assertThat(result.getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s1");
          break;
        case 2:
          assertThat(result.getValue(COL_NAME4).get().getAsString().get()).isEqualTo("s2");
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    String endpointOverride =
        System.getProperty("scalardb.dynamo.endpoint_override", "http://localhost:8000");
    String region = System.getProperty("scalardb.dynamo.region", "us-west-2");
    String accessKeyId = System.getProperty("scalardb.dynamo.access_key_id", "fakeMyKeyId");
    String secretAccessKey =
        System.getProperty("scalardb.dynamo.secret_access_key", "fakeSecretAccessKey");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    DynamoConfig config = new DynamoConfig(props);
    admin = new DynamoAdmin(config);
    dynamo = new Dynamo(config);

    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.INT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addColumn(COL_NAME4, DataType.TEXT)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME2)
            .addSecondaryIndex(COL_NAME3)
            .build(),
        ImmutableMap.of("no-scaling", "true", "no-backup", "true"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE);
    admin.close();
    dynamo.close();
  }
}
