package com.scalar.db.storage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class StorageSinglePartitionKeyIntegrationTestBase {

  private static final String TEST_NAME = "single_pkey";
  private static final String NAMESPACE = "integration_testing_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String COL_NAME = "col";

  private static final int PARTITION_KEY_NUM = 5;

  private static final Random RANDOM = new Random();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace;
  private static Set<DataType> partitionKeyTypes;

  private static long seed;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespace = getNamespace();
      partitionKeyTypes = getPartitionKeyTypes();
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println("The seed used in the single partition key integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getPartitionKeyTypes() {
    return new HashSet<>(Arrays.asList(DataType.values()));
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType partitionKeyType : partitionKeyTypes) {
      createTable(partitionKeyType, options);
    }
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    admin.close();
    storage.close();
  }

  private static void deleteTables() throws ExecutionException {
    for (DataType partitionKeyType : partitionKeyTypes) {
      admin.dropTable(namespace, getTableName(partitionKeyType));
    }

    admin.dropNamespace(namespace);
  }

  private void truncateTable(DataType partitionKeyType) throws ExecutionException {
    admin.truncateTable(namespace, getTableName(partitionKeyType));
  }

  private static String getTableName(DataType partitionKeyType) {
    return partitionKeyType.toString();
  }

  @Test
  public void getAndDelete_ShouldBehaveCorrectly() throws ExecutionException {
    for (DataType partitionKeyType : partitionKeyTypes) {
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
        Assertions.assertThat(result).describedAs(description).isPresent();
        Assertions.assertThat(result.get().getValue(PARTITION_KEY).isPresent())
            .describedAs(description)
            .isTrue();
        Assertions.assertThat(result.get().getValue(PARTITION_KEY).get())
            .describedAs(description)
            .isEqualTo(partitionKeyValue);
        Assertions.assertThat(result.get().getValue(COL_NAME).isPresent())
            .describedAs(description)
            .isTrue();
        Assertions.assertThat(result.get().getValue(COL_NAME).get().getAsInt())
            .describedAs(description)
            .isEqualTo(1);
      }

      // for delete
      for (Value<?> partitionKeyValue : partitionKeyValues) {
        // Arrange
        Delete delete = prepareDelete(partitionKeyType, partitionKeyValue);

        // Act
        storage.delete(delete);

        // Assert
        Optional<Result> result = storage.get(prepareGet(partitionKeyType, partitionKeyValue));
        Assertions.assertThat(result).describedAs(description).isNotPresent();
      }
    }
  }

  private List<Value<?>> prepareRecords(DataType partitionKeyType) throws ExecutionException {
    RANDOM.setSeed(seed);

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
                  partitionKeyValue = getRandomValue(RANDOM, PARTITION_KEY, partitionKeyType);
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
      throw new ExecutionException("put data to database failed", e);
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
