package com.scalar.db.storage;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
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

@SuppressFBWarnings(value = {"MS_PKGPROTECT", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
public abstract class StorageMultiplePartitionKeyIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "integration_testing_";
  protected static final String TABLE_BASE_NAME = "mul_pkey_";
  protected static final String FIRST_PARTITION_KEY = "pkey1";
  protected static final String SECOND_PARTITION_KEY = "pkey2";
  protected static final String COL_NAME = "col";

  private static final int FIRST_PARTITION_KEY_NUM = 5;
  private static final int SECOND_PARTITION_KEY_NUM = 5;

  private static final Random RANDOM = new Random();

  private static boolean initialized;
  protected static DistributedStorageAdmin admin;
  protected static DistributedStorage storage;
  private static String namespaceBaseName;

  // Key: firstPartitionKeyType, Value: secondPartitionKeyType
  protected static ListMultimap<DataType, DataType> partitionKeyTypes;

  private static long seed;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory = new StorageFactory(getDatabaseConfig());
      admin = factory.getAdmin();
      namespaceBaseName = getNamespaceBaseName();
      partitionKeyTypes = getPartitionKeyTypes();
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println("The seed used in the multiple partition key integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected ListMultimap<DataType, DataType> getPartitionKeyTypes() {
    ListMultimap<DataType, DataType> partitionKeyTypes = ArrayListMultimap.create();
    for (DataType firstPartitionKeyType : DataType.values()) {
      for (DataType secondPartitionKeyType : DataType.values()) {
        partitionKeyTypes.put(firstPartitionKeyType, secondPartitionKeyType);
      }
    }
    return partitionKeyTypes;
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      admin.createNamespace(getNamespaceName(firstPartitionKeyType), true, options);
      for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
        createTable(firstPartitionKeyType, secondPartitionKeyType, options);
      }
    }
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    admin.close();
    storage.close();
  }

  private static void deleteTables() throws ExecutionException {
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
        admin.dropTable(
            getNamespaceName(firstPartitionKeyType),
            getTableName(firstPartitionKeyType, secondPartitionKeyType));
      }
      admin.dropNamespace(getNamespaceName(firstPartitionKeyType));
    }
  }

  private void truncateTable(DataType firstPartitionKeyType, DataType secondPartitionKeyType)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(firstPartitionKeyType),
        getTableName(firstPartitionKeyType, secondPartitionKeyType));
  }

  private static String getTableName(
      DataType firstPartitionKeyType, DataType secondPartitionKeyType) {
    return TABLE_BASE_NAME
        + String.join("_", firstPartitionKeyType.toString(), secondPartitionKeyType.toString());
  }

  private static String getNamespaceName(DataType firstPartitionKeyType) {
    return namespaceBaseName + firstPartitionKeyType;
  }

  @Test
  public void getAndDelete_ShouldBehaveCorrectly() throws ExecutionException {
    for (DataType firstPartitionKeyType : partitionKeyTypes.keySet()) {
      for (DataType secondPartitionKeyType : partitionKeyTypes.get(firstPartitionKeyType)) {
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
          Optional<Result> result = storage.get(get);

          // Assert
          Assertions.assertThat(result).describedAs(description).isPresent();
          Assertions.assertThat(result.get().getValue(FIRST_PARTITION_KEY).isPresent())
              .describedAs(description)
              .isTrue();
          Assertions.assertThat(result.get().getValue(FIRST_PARTITION_KEY).get())
              .describedAs(description)
              .isEqualTo(partitionKey.first);
          Assertions.assertThat(result.get().getValue(SECOND_PARTITION_KEY).isPresent())
              .describedAs(description)
              .isTrue();
          Assertions.assertThat(result.get().getValue(SECOND_PARTITION_KEY).get())
              .describedAs(description)
              .isEqualTo(partitionKey.second);
          Assertions.assertThat(result.get().getValue(COL_NAME).isPresent())
              .describedAs(description)
              .isTrue();
          Assertions.assertThat(result.get().getValue(COL_NAME).get().getAsInt())
              .describedAs(description)
              .isEqualTo(1);
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
    RANDOM.setSeed(seed);

    List<Put> puts = new ArrayList<>();
    List<PartitionKey> ret = new ArrayList<>();

    if (firstPartitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(FIRST_PARTITION_KEY)
          .forEach(
              firstPartitionKeyValue ->
                  prepareRecords(
                      firstPartitionKeyType,
                      firstPartitionKeyValue,
                      secondPartitionKeyType,
                      puts,
                      ret));
    } else {
      Set<Value<?>> valueSet = new HashSet<>();

      // Add min and max partition key values
      Arrays.asList(
              getMinValue(FIRST_PARTITION_KEY, firstPartitionKeyType),
              getMaxValue(FIRST_PARTITION_KEY, firstPartitionKeyType))
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
                Value<?> firstPartitionKeyValue;
                while (true) {
                  firstPartitionKeyValue =
                      getRandomValue(RANDOM, FIRST_PARTITION_KEY, firstPartitionKeyType);
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
      throw new ExecutionException("put data to database failed", e);
    }
    return ret;
  }

  private void prepareRecords(
      DataType firstPartitionKeyType,
      Value<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      List<Put> puts,
      List<PartitionKey> ret) {
    if (secondPartitionKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(SECOND_PARTITION_KEY)
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
      Set<Value<?>> valueSet = new HashSet<>();

      // min and max second partition key values
      Arrays.asList(
              getMinValue(SECOND_PARTITION_KEY, secondPartitionKeyType),
              getMaxValue(SECOND_PARTITION_KEY, secondPartitionKeyType))
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
          Value<?> secondPartitionKeyValue =
              getRandomValue(RANDOM, SECOND_PARTITION_KEY, secondPartitionKeyType);
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
      Value<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Value<?> secondPartitionKeyValue) {
    return new Put(new Key(firstPartitionKeyValue, secondPartitionKeyValue))
        .withValue(COL_NAME, 1)
        .forNamespace(getNamespaceName(firstPartitionKeyType))
        .forTable(getTableName(firstPartitionKeyType, secondPartitionKeyType));
  }

  private Get prepareGet(
      DataType firstPartitionKeyType,
      Value<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Value<?> secondPartitionKeyValue) {
    return new Get(new Key(firstPartitionKeyValue, secondPartitionKeyValue))
        .forNamespace(getNamespaceName(firstPartitionKeyType))
        .forTable(getTableName(firstPartitionKeyType, secondPartitionKeyType));
  }

  private Delete prepareDelete(
      DataType firstPartitionKeyType,
      Value<?> firstPartitionKeyValue,
      DataType secondPartitionKeyType,
      Value<?> secondPartitionKeyValue) {
    return new Delete(new Key(firstPartitionKeyValue, secondPartitionKeyValue))
        .forNamespace(getNamespaceName(firstPartitionKeyType))
        .forTable(getTableName(firstPartitionKeyType, secondPartitionKeyType));
  }

  private String description(DataType firstPartitionKeyType, DataType secondPartitionKeyType) {
    return String.format(
        "failed with firstPartitionKeyType: %s, secondPartitionKeyType: %s",
        firstPartitionKeyType, secondPartitionKeyType);
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

  private static class PartitionKey {
    public final Value<?> first;
    public final Value<?> second;

    public PartitionKey(Value<?> first, Value<?> second) {
      this.first = first;
      this.second = second;
    }
  }
}
