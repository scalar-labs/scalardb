package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
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
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class StorageSingleClusteringKeyScanIntegrationTestBase {

  private enum OrderingType {
    SPECIFIED,
    REVERSED,
    NOTHING,
  }

  private static final String TEST_NAME = "single_ckey";
  private static final String NAMESPACE = "integration_testing_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String CLUSTERING_KEY = "ckey";
  private static final String COL_NAME = "col";

  private static final int CLUSTERING_KEY_NUM = 20;

  private static final Random RANDOM = new Random();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace;
  private static Set<DataType> clusteringKeyTypes;

  private static long seed;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespace = getNamespace();
      clusteringKeyTypes = getClusteringKeyTypes();
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println(
          "The seed used in the single clustering key scan integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getClusteringKeyTypes() {
    return new HashSet<>(Arrays.asList(DataType.values()));
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        createTable(clusteringKeyType, clusteringOrder, options);
      }
    }
  }

  private void createTable(
      DataType clusteringKeyType, Order clusteringOrder, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        namespace,
        getTableName(clusteringKeyType, clusteringOrder),
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, DataType.INT)
            .addColumn(CLUSTERING_KEY, clusteringKeyType)
            .addColumn(COL_NAME, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
            .addClusteringKey(CLUSTERING_KEY, clusteringOrder)
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
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        admin.dropTable(namespace, getTableName(clusteringKeyType, clusteringOrder));
      }
    }
    admin.dropNamespace(namespace);
  }

  private void truncateTable(DataType clusteringKeyType, Order clusteringOrder)
      throws ExecutionException {
    admin.truncateTable(namespace, getTableName(clusteringKeyType, clusteringOrder));
  }

  private static String getTableName(DataType clusteringKeyType, Order clusteringOrder) {
    return clusteringKeyType + "_" + clusteringOrder;
  }

  @Test
  public void scan_WithoutClusteringKeyRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (OrderingType orderingType : OrderingType.values()) {
          for (boolean withLimit : Arrays.asList(false, true)) {
            scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
                clusteringKeyValues, clusteringKeyType, clusteringOrder, orderingType, withLimit);
          }
        }
      }
    }
  }

  private void scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    List<Value<?>> expected =
        getExpected(clusteringKeyValues, null, null, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(clusteringKeyType, clusteringOrder, null, null, null, null, orderingType, limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(clusteringKeyType, clusteringOrder, null, null, orderingType, withLimit));
  }

  @Test
  public void scan_WithClusteringKeyRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean startInclusive : Arrays.asList(true, false)) {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithClusteringKeyRange_ShouldReturnProperResult(
                    clusteringKeyValues,
                    clusteringKeyType,
                    clusteringOrder,
                    startInclusive,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyRange_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> startClusteringKeyValue;
    Value<?> endClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startClusteringKeyValue = clusteringKeyValues.get(0);
      endClusteringKeyValue = clusteringKeyValues.get(1);
    } else {
      startClusteringKeyValue = clusteringKeyValues.get(4);
      endClusteringKeyValue = clusteringKeyValues.get(14);
    }

    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues,
            startClusteringKeyValue,
            startInclusive,
            endClusteringKeyValue,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            startClusteringKeyValue,
            startInclusive,
            endClusteringKeyValue,
            endInclusive,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType,
            clusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithClusteringKeyRangeWithSameValues_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean startInclusive : Arrays.asList(true, false)) {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
                    clusteringKeyValues,
                    clusteringKeyType,
                    clusteringOrder,
                    startInclusive,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> startAndEndClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startAndEndClusteringKeyValue = clusteringKeyValues.get(0);
    } else {
      startAndEndClusteringKeyValue = clusteringKeyValues.get(9);
    }

    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues,
            startAndEndClusteringKeyValue,
            startInclusive,
            startAndEndClusteringKeyValue,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            startAndEndClusteringKeyValue,
            startInclusive,
            startAndEndClusteringKeyValue,
            endInclusive,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType,
            clusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean startInclusive : Arrays.asList(true, false)) {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
                    clusteringKeyValues,
                    clusteringKeyType,
                    clusteringOrder,
                    startInclusive,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> startClusteringKeyValue = getMinValue(CLUSTERING_KEY, clusteringKeyType);
    Value<?> endClusteringKeyValue = getMaxValue(CLUSTERING_KEY, clusteringKeyType);
    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues,
            startClusteringKeyValue,
            startInclusive,
            endClusteringKeyValue,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            startClusteringKeyValue,
            startInclusive,
            endClusteringKeyValue,
            endInclusive,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType,
            clusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithClusteringKeyStartRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean startInclusive : Arrays.asList(true, false)) {
          for (OrderingType orderingType : OrderingType.values()) {
            for (boolean withLimit : Arrays.asList(false, true)) {
              scan_WithClusteringKeyStartRange_ShouldReturnProperResult(
                  clusteringKeyValues,
                  clusteringKeyType,
                  clusteringOrder,
                  startInclusive,
                  orderingType,
                  withLimit);
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyStartRange_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> startClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startClusteringKeyValue = clusteringKeyValues.get(0);
    } else {
      startClusteringKeyValue = clusteringKeyValues.get(4);
    }

    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues, startClusteringKeyValue, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            startClusteringKeyValue,
            startInclusive,
            null,
            null,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType, clusteringOrder, startInclusive, null, orderingType, withLimit));
  }

  @Test
  public void scan_WithClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean startInclusive : Arrays.asList(true, false)) {
          for (OrderingType orderingType : OrderingType.values()) {
            for (boolean withLimit : Arrays.asList(false, true)) {
              scan_WithClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
                  clusteringKeyValues,
                  clusteringKeyType,
                  clusteringOrder,
                  startInclusive,
                  orderingType,
                  withLimit);
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> startClusteringKeyValue = getMinValue(CLUSTERING_KEY, clusteringKeyType);
    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues, startClusteringKeyValue, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            startClusteringKeyValue,
            startInclusive,
            null,
            null,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType, clusteringOrder, startInclusive, null, orderingType, withLimit));
  }

  @Test
  public void scan_WithClusteringKeyEndRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean endInclusive : Arrays.asList(true, false)) {
          for (OrderingType orderingType : OrderingType.values()) {
            for (boolean withLimit : Arrays.asList(false, true)) {
              scan_WithClusteringKeyEndRange_ShouldReturnProperResult(
                  clusteringKeyValues,
                  clusteringKeyType,
                  clusteringOrder,
                  endInclusive,
                  orderingType,
                  withLimit);
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyEndRange_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> endClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      endClusteringKeyValue = clusteringKeyValues.get(1);
    } else {
      endClusteringKeyValue = clusteringKeyValues.get(14);
    }

    List<Value<?>> expected =
        getExpected(
            clusteringKeyValues, null, null, endClusteringKeyValue, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            null,
            null,
            endClusteringKeyValue,
            endInclusive,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType, clusteringOrder, null, endInclusive, orderingType, withLimit));
  }

  @Test
  public void scan_WithClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        truncateTable(clusteringKeyType, clusteringOrder);
        List<Value<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
        for (boolean endInclusive : Arrays.asList(true, false)) {
          for (OrderingType orderingType : OrderingType.values()) {
            for (boolean withLimit : Arrays.asList(false, true)) {
              scan_WithClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
                  clusteringKeyValues,
                  clusteringKeyType,
                  clusteringOrder,
                  endInclusive,
                  orderingType,
                  withLimit);
            }
          }
        }
      }
    }
  }

  private void scan_WithClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
      List<Value<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> endClusteringKey = getMaxValue(CLUSTERING_KEY, clusteringKeyType);
    List<Value<?>> expected =
        getExpected(clusteringKeyValues, null, null, endClusteringKey, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            clusteringKeyType,
            clusteringOrder,
            null,
            null,
            endClusteringKey,
            endInclusive,
            orderingType,
            limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            clusteringKeyType, clusteringOrder, null, endInclusive, orderingType, withLimit));
  }

  private List<Value<?>> prepareRecords(DataType clusteringKeyType, Order clusteringOrder)
      throws ExecutionException {
    RANDOM.setSeed(seed);

    List<Value<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (clusteringKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(CLUSTERING_KEY)
          .forEach(
              clusteringKeyValue -> {
                ret.add(clusteringKeyValue);
                puts.add(preparePut(clusteringKeyType, clusteringOrder, clusteringKeyValue));
              });
    } else {
      Set<Value<?>> valueSet = new HashSet<>();

      // Add min and max clustering key values
      Arrays.asList(
              getMinValue(CLUSTERING_KEY, clusteringKeyType),
              getMaxValue(CLUSTERING_KEY, clusteringKeyType))
          .forEach(
              clusteringKeyValue -> {
                valueSet.add(clusteringKeyValue);
                ret.add(clusteringKeyValue);
                puts.add(preparePut(clusteringKeyType, clusteringOrder, clusteringKeyValue));
              });

      IntStream.range(0, CLUSTERING_KEY_NUM - 2)
          .forEach(
              i -> {
                Value<?> clusteringKeyValue;
                while (true) {
                  clusteringKeyValue = getRandomValue(RANDOM, CLUSTERING_KEY, clusteringKeyType);
                  // reject duplication
                  if (!valueSet.contains(clusteringKeyValue)) {
                    valueSet.add(clusteringKeyValue);
                    break;
                  }
                }

                ret.add(clusteringKeyValue);
                puts.add(preparePut(clusteringKeyType, clusteringOrder, clusteringKeyValue));
              });
    }
    try {
      List<Put> buffer = new ArrayList<>();
      for (Put put : puts) {
        buffer.add(put);
        if (buffer.size() == 20) {
          storage.mutate(buffer);
          buffer.clear();
        }
      }
      if (!buffer.isEmpty()) {
        storage.mutate(buffer);
      }
    } catch (ExecutionException e) {
      throw new ExecutionException("put data to database failed", e);
    }

    ret.sort(
        clusteringOrder == Order.ASC
            ? com.google.common.collect.Ordering.natural()
            : com.google.common.collect.Ordering.natural().reverse());
    return ret;
  }

  private Put preparePut(
      DataType clusteringKeyType, Order clusteringOrder, Value<?> clusteringKeyValue) {
    return new Put(getPartitionKey(), new Key(clusteringKeyValue))
        .withValue(COL_NAME, 1)
        .forNamespace(namespace)
        .forTable(getTableName(clusteringKeyType, clusteringOrder));
  }

  private Key getPartitionKey() {
    return new Key(PARTITION_KEY, 1);
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

  private String description(
      DataType clusteringKeyType,
      Order clusteringOrder,
      @Nullable Boolean startInclusive,
      @Nullable Boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append(
        String.format(
            "failed with clusteringKeyType: %s, clusteringOrder: %s",
            clusteringKeyType, clusteringOrder));
    if (startInclusive != null) {
      builder.append(String.format(", startInclusive: %s", startInclusive));
    }
    if (endInclusive != null) {
      builder.append(String.format(", endInclusive: %s", endInclusive));
    }
    builder.append(String.format(", orderingType: %s, withLimit: %b", orderingType, withLimit));
    return builder.toString();
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  private List<Value<?>> getExpected(
      List<Value<?>> clusteringKeyValues,
      @Nullable Value<?> startClusteringKeyValue,
      @Nullable Boolean startInclusive,
      @Nullable Value<?> endClusteringKeyValue,
      @Nullable Boolean endInclusive,
      OrderingType orderingType) {
    List<Value<?>> ret = new ArrayList<>();
    for (Value<?> clusteringKeyValue : clusteringKeyValues) {
      if (startClusteringKeyValue != null && startInclusive != null) {
        int compare =
            Objects.compare(
                clusteringKeyValue,
                startClusteringKeyValue,
                com.google.common.collect.Ordering.natural());
        if (!(startInclusive ? compare >= 0 : compare > 0)) {
          continue;
        }
      }
      if (endClusteringKeyValue != null && endInclusive != null) {
        int compare =
            Objects.compare(
                clusteringKeyValue,
                endClusteringKeyValue,
                com.google.common.collect.Ordering.natural());
        if (!(endInclusive ? compare <= 0 : compare < 0)) {
          continue;
        }
      }
      ret.add(clusteringKeyValue);
    }
    if (orderingType == OrderingType.REVERSED) {
      Collections.reverse(ret);
    }
    return ret;
  }

  private int getLimit(boolean withLimit, List<Value<?>> expected) {
    int limit = 0;
    if (withLimit && !expected.isEmpty()) {
      if (expected.size() == 1) {
        limit = 1;
      } else {
        limit = RANDOM.nextInt(expected.size() - 1) + 1;
      }
    }
    return limit;
  }

  private Scan getScan(
      DataType clusteringKeyType,
      Order clusteringOrder,
      @Nullable Value<?> startClusteringKeyValue,
      @Nullable Boolean startInclusive,
      @Nullable Value<?> endClusteringKeyValue,
      @Nullable Boolean endInclusive,
      OrderingType orderingType,
      int limit) {
    Scan scan =
        new Scan(getPartitionKey())
            .forNamespace(namespace)
            .forTable(getTableName(clusteringKeyType, clusteringOrder));
    if (startClusteringKeyValue != null && startInclusive != null) {
      scan.withStart(new Key(startClusteringKeyValue), startInclusive);
    }
    if (endClusteringKeyValue != null && endInclusive != null) {
      scan.withEnd(new Key(endClusteringKeyValue), endInclusive);
    }
    switch (orderingType) {
      case SPECIFIED:
        scan.withOrdering(new Ordering(CLUSTERING_KEY, clusteringOrder));
        break;
      case REVERSED:
        scan.withOrdering(new Ordering(CLUSTERING_KEY, TestUtils.reverseOrder(clusteringOrder)));
        break;
      case NOTHING:
        break;
      default:
        throw new AssertionError();
    }
    if (limit > 0) {
      scan.withLimit(limit);
    }
    return scan;
  }

  private void assertScanResult(
      List<Result> actualResults, List<Value<?>> expected, String description) {
    List<Value<?>> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      assertThat(actualResult.getValue(CLUSTERING_KEY).isPresent()).isTrue();
      actual.add(actualResult.getValue(CLUSTERING_KEY).get());
    }
    assertThat(actual).describedAs(description).isEqualTo(expected);
  }
}
