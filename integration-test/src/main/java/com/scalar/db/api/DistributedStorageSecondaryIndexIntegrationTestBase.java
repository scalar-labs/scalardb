package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageSecondaryIndexIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageSecondaryIndexIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_secondary_idx";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String INDEX_COL_NAME = "idx_col";
  private static final String COL_NAME = "col";

  private static final int ATTEMPT_COUNT = 50;
  private static final int DATA_NUM = 10;

  private static final Random random = new Random();

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespace;
  private Set<DataType> secondaryIndexTypes;

  private long seed;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    secondaryIndexTypes = getSecondaryIndexTypes();
    createTables();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the secondary index integration test is " + seed);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getSecondaryIndexTypes() {
    return new HashSet<>(Arrays.asList(DataType.values()));
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType secondaryIndexType : secondaryIndexTypes) {
      createTable(secondaryIndexType, options);
    }
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void createTable(DataType secondaryIndexType, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        namespace,
        getTableName(secondaryIndexType),
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, DataType.INT)
            .addColumn(INDEX_COL_NAME, secondaryIndexType)
            .addColumn(COL_NAME, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
            .addSecondaryIndex(INDEX_COL_NAME)
            .build(),
        true,
        options);
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTables();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage", e);
    }
  }

  private void dropTables() throws ExecutionException {
    for (DataType secondaryIndexType : secondaryIndexTypes) {
      admin.dropTable(namespace, getTableName(secondaryIndexType));
    }
    admin.dropNamespace(namespace);
  }

  private void truncateTable(DataType secondaryIndexType) throws ExecutionException {
    admin.truncateTable(namespace, getTableName(secondaryIndexType));
  }

  private String getTableName(DataType secondaryIndexType) {
    return secondaryIndexType.toString();
  }

  @Test
  public void scan_WithRandomSecondaryIndexValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType secondaryIndexType : secondaryIndexTypes) {
      random.setSeed(seed);

      truncateTable(secondaryIndexType);

      for (int i = 0; i < ATTEMPT_COUNT; i++) {
        // Arrange
        Column<?> secondaryIndexValue =
            getColumnWithRandomValue(random, INDEX_COL_NAME, secondaryIndexType);
        prepareRecords(secondaryIndexType, secondaryIndexValue);
        Scan scan =
            Scan.newBuilder()
                .namespace(namespace)
                .table(getTableName(secondaryIndexType))
                .partitionKey(Key.newBuilder().add(secondaryIndexValue).build())
                .build();

        // Act
        List<Result> results = scanAll(scan);

        // Assert
        assertResults(results, secondaryIndexValue);
      }
    }
  }

  @Test
  public void scan_WithMaxSecondaryIndexValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {

    for (DataType secondaryIndexType : secondaryIndexTypes) {
      truncateTable(secondaryIndexType);

      // Arrange
      Column<?> secondaryIndexValue = getColumnWithMaxValue(INDEX_COL_NAME, secondaryIndexType);
      prepareRecords(secondaryIndexType, secondaryIndexValue);
      Scan scan =
          Scan.newBuilder()
              .namespace(namespace)
              .table(getTableName(secondaryIndexType))
              .partitionKey(Key.newBuilder().add(secondaryIndexValue).build())
              .build();

      // Act
      List<Result> results = scanAll(scan);

      // Assert
      assertResults(results, secondaryIndexValue);
    }
  }

  @Test
  public void scan_WithMinSecondaryIndexValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {

    for (DataType secondaryIndexType : secondaryIndexTypes) {
      truncateTable(secondaryIndexType);

      // Arrange
      Column<?> secondaryIndexValue = getColumnWithMinValue(INDEX_COL_NAME, secondaryIndexType);
      prepareRecords(secondaryIndexType, secondaryIndexValue);
      Scan scan =
          Scan.newBuilder()
              .namespace(namespace)
              .table(getTableName(secondaryIndexType))
              .partitionKey(Key.newBuilder().add(secondaryIndexValue).build())
              .build();

      // Act
      List<Result> results = scanAll(scan);

      // Assert
      assertResults(results, secondaryIndexValue);
    }
  }

  @Test
  public void scan_WithSecondaryIndexValueAndConjunctions_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    DataType secondaryIndexType = DataType.INT;
    truncateTable(secondaryIndexType);

    // Arrange
    Column<?> secondaryIndexValue =
        getColumnWithRandomValue(random, INDEX_COL_NAME, secondaryIndexType);
    prepareRecords(secondaryIndexType, secondaryIndexValue);
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespace())
            .table(getTableName(secondaryIndexType))
            .indexKey(Key.ofInt(INDEX_COL_NAME, secondaryIndexValue.getIntValue()))
            .where(ConditionBuilder.column(PARTITION_KEY).isEqualToInt(1))
            .or(ConditionBuilder.column(PARTITION_KEY).isEqualToInt(2))
            .build();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).contains(PARTITION_KEY)).isTrue();
    assertThat(results.get(0).getInt(PARTITION_KEY)).isEqualTo(1);
    assertThat(results.get(0).contains(INDEX_COL_NAME)).isTrue();
    assertThat(results.get(0).getInt(INDEX_COL_NAME)).isEqualTo(secondaryIndexValue.getIntValue());
    assertThat(results.get(1).contains(PARTITION_KEY)).isTrue();
    assertThat(results.get(1).getInt(PARTITION_KEY)).isEqualTo(2);
    assertThat(results.get(1).contains(INDEX_COL_NAME)).isTrue();
    assertThat(results.get(1).getInt(INDEX_COL_NAME)).isEqualTo(secondaryIndexValue.getIntValue());
  }

  private void prepareRecords(DataType secondaryIndexType, Column<?> secondaryIndexValue)
      throws ExecutionException {
    for (int i = 0; i < DATA_NUM; i++) {
      Put put =
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName(secondaryIndexType))
              .partitionKey(Key.ofInt(PARTITION_KEY, i))
              .value(secondaryIndexValue)
              .intValue(COL_NAME, 1)
              .build();
      storage.put(put);
    }
  }

  private void assertResults(List<Result> results, Column<?> secondaryIndexValue) {
    assertThat(results.size()).isEqualTo(DATA_NUM);

    Set<Integer> partitionKeySet = new HashSet<>();
    for (int i = 0; i < DATA_NUM; i++) {
      partitionKeySet.add(i);
    }

    for (Result result : results) {
      assertThat(result.contains(PARTITION_KEY)).isTrue();
      partitionKeySet.remove(result.getInt(PARTITION_KEY));
      assertThat(result.contains(INDEX_COL_NAME)).isTrue();
      assertThat(result.getColumns().get(INDEX_COL_NAME)).isEqualTo(secondaryIndexValue);
      assertThat(result.contains(COL_NAME)).isTrue();
      assertThat(result.getInt(COL_NAME)).isEqualTo(1);
    }

    assertThat(partitionKeySet).isEmpty();
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    return TestUtils.getColumnWithRandomValue(random, columnName, dataType, true);
  }

  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMinValue(columnName, dataType, true);
  }

  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMaxValue(columnName, dataType);
  }
}
