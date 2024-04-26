package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
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
      random.setSeed(seed);

      truncateTable(partitionKeyType);
      List<Value<?>> partitionKeyValues = prepareRecords(partitionKeyType);

      String description = description(partitionKeyType);

      // for get
      for (Value<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Get get = prepareGet(partitionKeyType, partitionKeyValue);

        // Act
        Optional<Result> result = storage.get(get);

        // Assert
        assertThat(result).describedAs(description).isPresent();
        assertThat(result.get().getValue(PARTITION_KEY).isPresent())
            .describedAs(description)
            .isTrue();
        assertThat(result.get().getValue(PARTITION_KEY).get())
            .describedAs(description)
            .isEqualTo(partitionKeyValue);
        assertThat(result.get().getValue(COL_NAME).isPresent()).describedAs(description).isTrue();
        assertThat(result.get().getValue(COL_NAME).get().getAsInt())
            .describedAs(description)
            .isEqualTo(1);
      }

      // for scan
      for (Value<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Scan scan = prepareScan(partitionKeyType, partitionKeyValue);

        // Act Assert
        try (Scanner scanner = storage.scan(scan)) {
          Optional<Result> result = scanner.one();

          assertThat(result).describedAs(description).isPresent();
          assertThat(result.get().getValue(PARTITION_KEY).isPresent())
              .describedAs(description)
              .isTrue();
          assertThat(result.get().getValue(PARTITION_KEY).get())
              .describedAs(description)
              .isEqualTo(partitionKeyValue);
          assertThat(result.get().getValue(COL_NAME).isPresent()).describedAs(description).isTrue();
          assertThat(result.get().getValue(COL_NAME).get().getAsInt())
              .describedAs(description)
              .isEqualTo(1);

          assertThat(scanner.one()).isNotPresent();
        }
      }

      // for delete
      for (Value<?> partitionKeyValue : partitionKeyValues) {
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

  private List<Value<?>> prepareRecords(DataType partitionKeyType) throws ExecutionException {
    List<Value<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (partitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(PARTITION_KEY)
          .forEach(
              partitionKeyValue -> {
                ret.add(partitionKeyValue);
                puts.add(preparePut(partitionKeyType, partitionKeyValue));
              });
    } else {
      Set<Value<?>> valueSet = new HashSet<>();

      // Add min and max partition key values
      Arrays.asList(
              getMinValue(PARTITION_KEY, partitionKeyType),
              getMaxValue(PARTITION_KEY, partitionKeyType))
          .forEach(
              partitionKeyValue -> {
                valueSet.add(partitionKeyValue);
                ret.add(partitionKeyValue);
                puts.add(preparePut(partitionKeyType, partitionKeyValue));
              });

      IntStream.range(0, PARTITION_KEY_NUM - 2)
          .forEach(
              i -> {
                Value<?> partitionKeyValue;
                while (true) {
                  partitionKeyValue = getRandomValue(random, PARTITION_KEY, partitionKeyType);
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

  private Put preparePut(DataType partitionKeyType, Value<?> partitionKeyValue) {
    return new Put(new Key(partitionKeyValue))
        .withValue(COL_NAME, 1)
        .forNamespace(namespace)
        .forTable(getTableName(partitionKeyType));
  }

  private Get prepareGet(DataType partitionKeyType, Value<?> partitionKeyValue) {
    return new Get(new Key(partitionKeyValue))
        .forNamespace(namespace)
        .forTable(getTableName(partitionKeyType));
  }

  private Scan prepareScan(DataType partitionKeyType, Value<?> partitionKeyValue) {
    return new Scan(new Key(partitionKeyValue))
        .forNamespace(namespace)
        .forTable(getTableName(partitionKeyType));
  }

  private Delete prepareDelete(DataType partitionKeyType, Value<?> partitionKeyValue) {
    return new Delete(new Key(partitionKeyValue))
        .forNamespace(namespace)
        .forTable(getTableName(partitionKeyType));
  }

  private String description(DataType partitionKeyType) {
    return String.format("failed with partitionKeyType: %s", partitionKeyType);
  }

  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return TestUtils.getRandomValue(random, columnName, dataType);
  }

  protected Value<?> getMinValue(String columnName, DataType dataType) {
    return TestUtils.getMinValue(columnName, dataType);
  }

  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    return TestUtils.getMaxValue(columnName, dataType);
  }
}
