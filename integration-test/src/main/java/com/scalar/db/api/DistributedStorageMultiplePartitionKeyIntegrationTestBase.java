package com.scalar.db.api;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageMultiplePartitionKeyIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_mul_pkey";
  private static final String NAMESPACE_BASE_NAME = "int_test_" + TEST_NAME + "_";
  private static final String FIRST_PARTITION_KEY = "pkey1";
  private static final String SECOND_PARTITION_KEY = "pkey2";
  private static final String COL_NAME = "col";

  private static final int FIRST_PARTITION_KEY_NUM = 5;
  private static final int SECOND_PARTITION_KEY_NUM = 5;

  private static final Random random = new Random();

  private static final int THREAD_NUM = 10;

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespaceBaseName;

  // Key: firstPartitionKeyType, Value: secondPartitionKeyType
  private ListMultimap<DataType, DataType> partitionKeyTypes;

  private long seed;

  private ExecutorService executorService;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespaceBaseName = getNamespaceBaseName();
    partitionKeyTypes = getPartitionKeyTypes();
    executorService = Executors.newFixedThreadPool(getThreadNum());
    createTables();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the multiple partition key integration test is " + seed);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected ListMultimap<DataType, DataType> getPartitionKeyTypes() {
    ListMultimap<DataType, DataType> partitionKeyTypes = ArrayListMultimap.create();
    for (DataType firstPartitionKeyType : getDataTypes()) {
      for (DataType secondPartitionKeyType : getDataTypes()) {
        partitionKeyTypes.put(firstPartitionKeyType, secondPartitionKeyType);
      }
    }
    return partitionKeyTypes;
  }

  protected int getThreadNum() {
    return THREAD_NUM;
  }

  protected boolean isParallelDdlSupported() {
    return true;
  }

  private void createTables() throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();

    Map<String, String> options = getCreationOptions();
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            admin.createNamespace(getNamespaceName(firstPartitionKeyType), true, options);
            for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
              createTable(firstPartitionKeyType, secondPartitionKeyType, options);
            }
            return null;
          };
      testCallables.add(testCallable);
    }

    // We firstly execute the first one and then the rest. This is because the first table creation
    // creates the metadata table, and this process can't be handled in multiple threads/processes
    // at the same time.
    executeDdls(testCallables.subList(0, 1));
    executeDdls(testCallables.subList(1, testCallables.size()));
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void createTable(
      DataType firstPartitionKeyType, DataType secondPartitionKeyType, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(firstPartitionKeyType),
        getTableName(firstPartitionKeyType, secondPartitionKeyType),
        TableMetadata.newBuilder()
            .addColumn(FIRST_PARTITION_KEY, firstPartitionKeyType)
            .addColumn(SECOND_PARTITION_KEY, secondPartitionKeyType)
            .addColumn(COL_NAME, DataType.INT)
            .addPartitionKey(FIRST_PARTITION_KEY)
            .addPartitionKey(SECOND_PARTITION_KEY)
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

  private void dropTables() throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
              admin.dropTable(
                  getNamespaceName(firstPartitionKeyType),
                  getTableName(firstPartitionKeyType, secondPartitionKeyType));
            }
            admin.dropNamespace(getNamespaceName(firstPartitionKeyType));
            return null;
          };
      testCallables.add(testCallable);
    }

    // We firstly execute the callables without the last one. And then we execute the last one. This
    // is because the last table deletion deletes the metadata table, and this process can't be
    // handled in multiple threads/processes at the same time.
    executeDdls(testCallables.subList(0, testCallables.size() - 1));
    executeDdls(testCallables.subList(testCallables.size() - 1, testCallables.size()));
  }

  private void truncateTable(DataType firstPartitionKeyType, DataType secondPartitionKeyType)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(firstPartitionKeyType),
        getTableName(firstPartitionKeyType, secondPartitionKeyType));
  }

  private String getTableName(DataType firstPartitionKeyType, DataType secondPartitionKeyType) {
    return String.join("_", firstPartitionKeyType.toString(), secondPartitionKeyType.toString());
  }

  private String getNamespaceName(DataType firstPartitionKeyType) {
    return namespaceBaseName + firstPartitionKeyType;
  }

  private void executeDdls(List<Callable<Void>> ddls)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    if (isParallelDdlSupported()) {
      executeInParallel(ddls);
    } else {
      ddls.forEach(
          ddl -> {
            try {
              ddl.call();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  private void executeInParallel(List<Callable<Void>> testCallables)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Void>> futures = executorService.invokeAll(testCallables);
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @Test
  public void getAndDelete_ShouldBehaveCorrectly() throws ExecutionException {
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
        random.setSeed(seed);

        truncateTable(firstPartitionKeyType, secondPartitionKeyType);
        List<PartitionKey> partitionKeys =
            prepareRecords(firstPartitionKeyType, secondPartitionKeyType);

        String description = description(firstPartitionKeyType, secondPartitionKeyType);

        // for get
        for (PartitionKey partitionKey : partitionKeys) {
          // Arrange
          Get get =
              prepareGet(
                  firstPartitionKeyType,
                  partitionKey.first,
                  secondPartitionKeyType,
                  partitionKey.second);

          // Act
          Optional<Result> optResult = storage.get(get);

          // Assert
          Assertions.assertThat(optResult).describedAs(description).isPresent();
          Result result = optResult.get();
          Assertions.assertThat(result.contains(FIRST_PARTITION_KEY))
              .describedAs(description)
              .isTrue();
          Assertions.assertThat(result.getColumns().get(FIRST_PARTITION_KEY))
              .describedAs(description)
              .isEqualTo(partitionKey.first);
          Assertions.assertThat(result.contains(SECOND_PARTITION_KEY))
              .describedAs(description)
              .isTrue();
          Assertions.assertThat(result.getColumns().get(SECOND_PARTITION_KEY))
              .describedAs(description)
              .isEqualTo(partitionKey.second);
          Assertions.assertThat(result.contains(COL_NAME)).describedAs(description).isTrue();
          Assertions.assertThat(result.getInt(COL_NAME)).describedAs(description).isEqualTo(1);
        }

        // for delete
        for (PartitionKey partitionKey : partitionKeys) {
          // Arrange
          Delete delete =
              prepareDelete(
                  firstPartitionKeyType,
                  partitionKey.first,
                  secondPartitionKeyType,
                  partitionKey.second);

          // Act
          storage.delete(delete);

          // Assert
          Optional<Result> result =
              storage.get(
                  prepareGet(
                      firstPartitionKeyType,
                      partitionKey.first,
                      secondPartitionKeyType,
                      partitionKey.second));
          Assertions.assertThat(result).describedAs(description).isNotPresent();
        }
      }
    }
  }

  private List<PartitionKey> prepareRecords(
      DataType firstPartitionKeyType, DataType secondPartitionKeyType) throws ExecutionException {
    List<Put> puts = new ArrayList<>();
    List<PartitionKey> ret = new ArrayList<>();

    if (firstPartitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanColumns(FIRST_PARTITION_KEY)
          .forEach(
              firstPartitionKeyValue ->
                  prepareRecords(
                      firstPartitionKeyType,
                      firstPartitionKeyValue,
                      secondPartitionKeyType,
                      puts,
                      ret));
    } else {
      Set<Column<?>> valueSet = new HashSet<>();

      // Add min and max partition key values
      Arrays.asList(
              getColumnWithMinValue(FIRST_PARTITION_KEY, firstPartitionKeyType),
              getColumnWithMaxValue(FIRST_PARTITION_KEY, firstPartitionKeyType))
          .forEach(
              firstPartitionKeyValue -> {
                valueSet.add(firstPartitionKeyValue);
                prepareRecords(
                    firstPartitionKeyType,
                    firstPartitionKeyValue,
                    secondPartitionKeyType,
                    puts,
                    ret);
              });

      IntStream.range(0, FIRST_PARTITION_KEY_NUM - 2)
          .forEach(
              i -> {
                Column<?> firstPartitionKeyValue;
                while (true) {
                  firstPartitionKeyValue =
                      getColumnWithRandomValue(random, FIRST_PARTITION_KEY, firstPartitionKeyType);
                  // reject duplication
                  if (!valueSet.contains(firstPartitionKeyValue)) {
                    valueSet.add(firstPartitionKeyValue);
                    break;
                  }
                }
                prepareRecords(
                    firstPartitionKeyType,
                    firstPartitionKeyValue,
                    secondPartitionKeyType,
                    puts,
                    ret);
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

  private void prepareRecords(
      DataType firstPartitionKeyType,
      Column<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      List<Put> puts,
      List<PartitionKey> ret) {
    if (secondPartitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanColumns(SECOND_PARTITION_KEY)
          .forEach(
              secondPartitionKeyValue -> {
                ret.add(new PartitionKey(firstPartitionKeyValue, secondPartitionKeyValue));
                puts.add(
                    preparePut(
                        firstPartitionKeyType,
                        firstPartitionKeyValue,
                        secondPartitionKeyType,
                        secondPartitionKeyValue));
              });
    } else {
      Set<Column<?>> valueSet = new HashSet<>();

      // min and max second partition key values
      Arrays.asList(
              getColumnWithMinValue(SECOND_PARTITION_KEY, secondPartitionKeyType),
              getColumnWithMaxValue(SECOND_PARTITION_KEY, secondPartitionKeyType))
          .forEach(
              secondPartitionKeyValue -> {
                ret.add(new PartitionKey(firstPartitionKeyValue, secondPartitionKeyValue));
                puts.add(
                    preparePut(
                        firstPartitionKeyType,
                        firstPartitionKeyValue,
                        secondPartitionKeyType,
                        secondPartitionKeyValue));
                valueSet.add(secondPartitionKeyValue);
              });

      for (int i = 0; i < SECOND_PARTITION_KEY_NUM - 2; i++) {
        while (true) {
          Column<?> secondPartitionKeyValue =
              getColumnWithRandomValue(random, SECOND_PARTITION_KEY, secondPartitionKeyType);
          // reject duplication
          if (!valueSet.contains(secondPartitionKeyValue)) {
            ret.add(new PartitionKey(firstPartitionKeyValue, secondPartitionKeyValue));
            puts.add(
                preparePut(
                    firstPartitionKeyType,
                    firstPartitionKeyValue,
                    secondPartitionKeyType,
                    secondPartitionKeyValue));
            valueSet.add(secondPartitionKeyValue);
            break;
          }
        }
      }
    }
  }

  private Put preparePut(
      DataType firstPartitionKeyType,
      Column<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Column<?> secondPartitionKeyValue) {
    return Put.newBuilder()
        .namespace(getNamespaceName(firstPartitionKeyType))
        .table(getTableName(firstPartitionKeyType, secondPartitionKeyType))
        .partitionKey(
            Key.newBuilder().add(firstPartitionKeyValue).add(secondPartitionKeyValue).build())
        .intValue(COL_NAME, 1)
        .build();
  }

  private Get prepareGet(
      DataType firstPartitionKeyType,
      Column<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Column<?> secondPartitionKeyValue) {
    return Get.newBuilder()
        .namespace(getNamespaceName(firstPartitionKeyType))
        .table(getTableName(firstPartitionKeyType, secondPartitionKeyType))
        .partitionKey(
            Key.newBuilder().add(firstPartitionKeyValue).add(secondPartitionKeyValue).build())
        .build();
  }

  private Delete prepareDelete(
      DataType firstPartitionKeyType,
      Column<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Column<?> secondPartitionKeyValue) {
    return Delete.newBuilder()
        .namespace(getNamespaceName(firstPartitionKeyType))
        .table(getTableName(firstPartitionKeyType, secondPartitionKeyType))
        .partitionKey(
            Key.newBuilder().add(firstPartitionKeyValue).add(secondPartitionKeyValue).build())
        .build();
  }

  private String description(DataType firstPartitionKeyType, DataType secondPartitionKeyType) {
    return String.format(
        "failed with firstPartitionKeyType: %s, secondPartitionKeyType: %s",
        firstPartitionKeyType, secondPartitionKeyType);
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

  private static class PartitionKey {
    public final Column<?> first;
    public final Column<?> second;

    public PartitionKey(Column<?> first, Column<?> second) {
      this.first = first;
      this.second = second;
    }
  }

  protected List<DataType> getDataTypes() {
    return Arrays.asList(DataType.values());
  }
}
