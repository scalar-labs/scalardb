package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class StorageSecondaryIndexIntegrationTestBase {

  private static final String TEST_NAME = "secondary_idx";
  private static final String NAMESPACE = "integration_testing_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String INDEX_COL_NAME = "idx_col";
  private static final String COL_NAME = "col";

  private static final int ATTEMPT_COUNT = 50;
  private static final int DATA_NUM = 10;

  private static final Random RANDOM = new Random();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace;
  private static Set<DataType> secondaryIndexTypes;

  private static long seed;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespace = getNamespace();
      secondaryIndexTypes = getSecondaryIndexTypes();
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println("The seed used in the secondary index integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getSecondaryIndexTypes() {
    return new HashSet<>(Arrays.asList(DataType.values()));
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType secondaryIndexType : secondaryIndexTypes) {
      createTable(secondaryIndexType, options);
    }
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    admin.close();
    storage.close();
  }

  private static void deleteTables() throws ExecutionException {
    for (DataType secondaryIndexType : secondaryIndexTypes) {
      admin.dropTable(namespace, getTableName(secondaryIndexType));
    }
    admin.dropNamespace(namespace);
  }

  private void truncateTable(DataType secondaryIndexType) throws ExecutionException {
    admin.truncateTable(namespace, getTableName(secondaryIndexType));
  }

  private static String getTableName(DataType secondaryIndexType) {
    return secondaryIndexType.toString();
  }

  @Test
  public void scan_WithRandomSecondaryIndexValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    RANDOM.setSeed(seed);

    for (DataType secondaryIndexType : secondaryIndexTypes) {
      truncateTable(secondaryIndexType);

      for (int i = 0; i < ATTEMPT_COUNT; i++) {
        // Arrange
        Value<?> secondaryIndexValue = getRandomValue(RANDOM, INDEX_COL_NAME, secondaryIndexType);
        prepareRecords(secondaryIndexType, secondaryIndexValue);
        Scan scan =
            new Scan(new Key(secondaryIndexValue))
                .forNamespace(namespace)
                .forTable(getTableName(secondaryIndexType));

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
      Value<?> secondaryIndexValue = getMaxValue(INDEX_COL_NAME, secondaryIndexType);
      prepareRecords(secondaryIndexType, secondaryIndexValue);
      Scan scan =
          new Scan(new Key(secondaryIndexValue))
              .forNamespace(namespace)
              .forTable(getTableName(secondaryIndexType));

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
      Value<?> secondaryIndexValue = getMinValue(INDEX_COL_NAME, secondaryIndexType);
      prepareRecords(secondaryIndexType, secondaryIndexValue);
      Scan scan =
          new Scan(new Key(secondaryIndexValue))
              .forNamespace(namespace)
              .forTable(getTableName(secondaryIndexType));

      // Act
      List<Result> results = scanAll(scan);

      // Assert
      assertResults(results, secondaryIndexValue);
    }
  }

  private void prepareRecords(DataType secondaryIndexType, Value<?> secondaryIndexValue)
      throws ExecutionException {
    for (int i = 0; i < DATA_NUM; i++) {
      Put put =
          new Put(new Key(PARTITION_KEY, i))
              .withValue(secondaryIndexValue)
              .withValue(COL_NAME, 1)
              .forNamespace(namespace)
              .forTable(getTableName(secondaryIndexType));
      storage.put(put);
    }
  }

  private void assertResults(List<Result> results, Value<?> secondaryIndexValue) {
    assertThat(results.size()).isEqualTo(DATA_NUM);

    Set<Integer> partitionKeySet = new HashSet<>();
    for (int i = 0; i < DATA_NUM; i++) {
      partitionKeySet.add(i);
    }

    for (Result result : results) {
      assertThat(result.getValue(PARTITION_KEY).isPresent()).isTrue();
      partitionKeySet.remove(result.getValue(PARTITION_KEY).get().getAsInt());
      assertThat(result.getValue(INDEX_COL_NAME).isPresent()).isTrue();
      assertThat(result.getValue(INDEX_COL_NAME).get()).isEqualTo(secondaryIndexValue);
      assertThat(result.getValue(COL_NAME).isPresent()).isTrue();
      assertThat(result.getValue(COL_NAME).get().getAsInt()).isEqualTo(1);
    }

    assertThat(partitionKeySet).isEmpty();
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return TestUtils.getRandomValue(random, columnName, dataType, true);
  }

  protected Value<?> getMinValue(String columnName, DataType dataType) {
    return TestUtils.getMinValue(columnName, dataType, true);
  }

  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    return TestUtils.getMaxValue(columnName, dataType);
  }
}
