package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageSinglePartitionKeyIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageSinglePartitionKeyIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_single_pkey";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String COL_NAME = "col";

  private static final int PARTITION_KEY_NUM = 5;

  private static final Random random = new Random();

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespace;
  private Set<DataType> partitionKeyTypes;

  private long seed;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    partitionKeyTypes = getPartitionKeyTypes();
    createTables();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the single partition key integration test is " + seed);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getPartitionKeyTypes() {
    return new HashSet<>(Arrays.asList(DataType.values()));
  }

  protected boolean isFloatTypeKeySupported() {
    return true;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType partitionKeyType : partitionKeyTypes) {
      createTable(partitionKeyType, options);
    }
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void createTable(DataType partitionKeyType, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        namespace,
        getTableName(partitionKeyType),
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, partitionKeyType)
            .addColumn(COL_NAME, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
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
    for (DataType partitionKeyType : partitionKeyTypes) {
      admin.dropTable(namespace, getTableName(partitionKeyType));
    }

    admin.dropNamespace(namespace);
  }

  private void truncateTable(DataType partitionKeyType) throws ExecutionException {
    admin.truncateTable(namespace, getTableName(partitionKeyType));
  }

  private String getTableName(DataType partitionKeyType) {
    return partitionKeyType.toString();
  }

  @Test
  public void getAndScanAndDelete_ShouldBehaveCorrectly() throws ExecutionException, IOException {
    for (DataType partitionKeyType : partitionKeyTypes) {
      if (!isFloatTypeKeySupported()
          && (partitionKeyType == DataType.FLOAT || partitionKeyType == DataType.DOUBLE)) {
        continue;
      }
      random.setSeed(seed);

      truncateTable(partitionKeyType);
      List<Column<?>> partitionKeyValues = prepareRecords(partitionKeyType);

      String description = description(partitionKeyType);

      // for get
      for (Column<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Get get = prepareGet(partitionKeyType, partitionKeyValue);

        // Act
        Optional<Result> optResult = storage.get(get);

        // Assert
        assertThat(optResult).describedAs(description).isPresent();
        Result result = optResult.get();
        assertThat(result.contains(PARTITION_KEY)).describedAs(description).isTrue();
        assertThat(result.getColumns().get(PARTITION_KEY))
            .describedAs(description)
            .isEqualTo(partitionKeyValue);
        assertThat(result.contains(COL_NAME)).describedAs(description).isTrue();
        assertThat(result.getInt(COL_NAME)).describedAs(description).isEqualTo(1);
      }

      // for scan
      for (Column<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Scan scan = prepareScan(partitionKeyType, partitionKeyValue);

        // Act Assert
        try (Scanner scanner = storage.scan(scan)) {
          Optional<Result> optResult = scanner.one();

          assertThat(optResult).describedAs(description).isPresent();
          Result result = optResult.get();
          assertThat(result.contains(PARTITION_KEY)).describedAs(description).isTrue();
          assertThat(result.getColumns().get(PARTITION_KEY))
              .describedAs(description)
              .isEqualTo(partitionKeyValue);
          assertThat(result.contains(COL_NAME)).describedAs(description).isTrue();
          assertThat(result.getInt(COL_NAME)).describedAs(description).isEqualTo(1);

          assertThat(scanner.one()).isNotPresent();
        }
      }

      // for delete
      for (Column<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Delete delete = prepareDelete(partitionKeyType, partitionKeyValue);

        // Act
        storage.delete(delete);

        // Assert
        Optional<Result> result = storage.get(prepareGet(partitionKeyType, partitionKeyValue));
        assertThat(result).describedAs(description).isNotPresent();
      }
    }
  }

  private List<Column<?>> prepareRecords(DataType partitionKeyType) throws ExecutionException {
    List<Column<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (partitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanColumns(PARTITION_KEY)
          .forEach(
              partitionKeyValue -> {
                ret.add(partitionKeyValue);
                puts.add(preparePut(partitionKeyType, partitionKeyValue));
              });
    } else {
      Set<Column<?>> valueSet = new HashSet<>();

      // Add min and max partition key values
      Arrays.asList(
              getColumnWithMinValue(PARTITION_KEY, partitionKeyType),
              getColumnWithMaxValue(PARTITION_KEY, partitionKeyType))
          .forEach(
              partitionKeyValue -> {
                valueSet.add(partitionKeyValue);
                ret.add(partitionKeyValue);
                puts.add(preparePut(partitionKeyType, partitionKeyValue));
              });

      IntStream.range(0, PARTITION_KEY_NUM - 2)
          .forEach(
              i -> {
                Column<?> partitionKeyValue;
                while (true) {
                  partitionKeyValue =
                      getColumnWithRandomValue(random, PARTITION_KEY, partitionKeyType);
                  // reject duplication
                  if (!valueSet.contains(partitionKeyValue)) {
                    valueSet.add(partitionKeyValue);
                    break;
                  }
                }

                ret.add(partitionKeyValue);
                puts.add(preparePut(partitionKeyType, partitionKeyValue));
              });
    }
    try {
      for (Put put : puts) {
        storage.put(put);
      }
    } catch (ExecutionException e) {
      throw new ExecutionException("Put data to database failed", e);
    }
    return ret;
  }

  private Put preparePut(DataType partitionKeyType, Column<?> partitionKeyValue) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(getTableName(partitionKeyType))
        .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
        .intValue(COL_NAME, 1)
        .build();
  }

  private Get prepareGet(DataType partitionKeyType, Column<?> partitionKeyValue) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(getTableName(partitionKeyType))
        .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
        .build();
  }

  private Scan prepareScan(DataType partitionKeyType, Column<?> partitionKeyValue) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(getTableName(partitionKeyType))
        .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
        .build();
  }

  private Delete prepareDelete(DataType partitionKeyType, Column<?> partitionKeyValue) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(getTableName(partitionKeyType))
        .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
        .build();
  }

  private String description(DataType partitionKeyType) {
    return String.format("failed with partitionKeyType: %s", partitionKeyType);
  }

  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    return TestUtils.getColumnWithRandomValue(random, columnName, dataType);
  }

  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMinValue(columnName, dataType);
  }

  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMaxValue(columnName, dataType);
  }
}
