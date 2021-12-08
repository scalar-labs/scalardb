package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.storage.TestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamoWithReservedKeywordIntegrationTest {

  private static final String TEST_NAME = "reserved_keyword";
  private static final String NAMESPACE = "integration_testing_dynamo_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "status"; // Reserved keyword

  private static DynamoAdmin admin;
  private static Dynamo dynamo;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    DynamoConfig config =
        new DynamoConfig(
            TestUtils.addSuffix(DynamoEnv.getDynamoConfig(), TEST_NAME).getProperties());
    admin = new DynamoAdmin(config);
    dynamo = new Dynamo(config);
    createTable();
  }

  private static void createTable() throws ExecutionException {
    Map<String, String> options = DynamoEnv.getCreateOptions();
    admin.createNamespace(NAMESPACE, true, options);
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
        true,
        options);
  }

  @Before
  public void setUp() throws ExecutionException {
    admin.truncateTable(NAMESPACE, TABLE);
    dynamo.with(NAMESPACE, TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE);
    admin.dropNamespace(NAMESPACE);
    admin.close();
    dynamo.close();
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
    assertThat(getCol4Value(result.get())).isEqualTo(status);
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
    assertThat(getCol4Value(result.get())).isEqualTo(status);
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
    assertThat(getCol4Value(results.get(0))).isEqualTo("s0");
    assertThat(getCol4Value(results.get(1))).isEqualTo("s1");
    assertThat(getCol4Value(results.get(2))).isEqualTo("s2");
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
      assertThat(result.getValue(COL_NAME2).isPresent()).isTrue();
      switch (result.getValue(COL_NAME2).get().getAsInt()) {
        case 0:
          assertThat(getCol4Value(result)).isEqualTo("s0");
          break;
        case 1:
          assertThat(getCol4Value(result)).isEqualTo("s1");
          break;
        case 2:
          assertThat(getCol4Value(result)).isEqualTo("s2");
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  private String getCol4Value(Result result) {
    assertThat(result.getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(result.getValue(COL_NAME4).get().getAsString().isPresent()).isTrue();
    return result.getValue(COL_NAME4).get().getAsString().get();
  }
}
