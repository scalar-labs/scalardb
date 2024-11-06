package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public abstract class DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageSingleClusteringKeyScanIntegrationTestBase.class);

  private enum OrderingType {
    SPECIFIED,
    REVERSED,
    NOTHING,
  }

  private static final String TEST_NAME = "storage_single_ckey";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String PARTITION_KEY = "pkey";
  private static final String CLUSTERING_KEY = "ckey";
  private static final String COL_NAME = "col";

  private static final int CLUSTERING_KEY_NUM = 20;

  private static final Random random = new Random();

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespace;
  private Set<DataType> clusteringKeyTypes;

  private long seed;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    clusteringKeyTypes = getClusteringKeyTypes();
    createTables();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println(
        "The seed used in the single clustering key scan integration test is " + seed);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Set<DataType> getClusteringKeyTypes() {
    return new HashSet<>(Arrays.asList(DataType.valuesWithoutTimesRelatedTypes()));
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        createTable(clusteringKeyType, clusteringOrder, options);
      }
    }
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
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

  private String getTableName(DataType clusteringKeyType, Order clusteringOrder) {
    return clusteringKeyType + "_" + clusteringOrder;
  }

  @Test
  public void scan_WithoutClusteringKeyRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    for (DataType clusteringKeyType : clusteringKeyTypes) {
      for (Order clusteringOrder : Order.values()) {
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> startClusteringKeyValue;
    Column<?> endClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startClusteringKeyValue = clusteringKeyValues.get(0);
      endClusteringKeyValue = clusteringKeyValues.get(1);
    } else {
      startClusteringKeyValue = clusteringKeyValues.get(4);
      endClusteringKeyValue = clusteringKeyValues.get(14);
    }

    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> startAndEndClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startAndEndClusteringKeyValue = clusteringKeyValues.get(0);
    } else {
      startAndEndClusteringKeyValue = clusteringKeyValues.get(9);
    }

    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> startClusteringKeyValue = getColumnWithMinValue(CLUSTERING_KEY, clusteringKeyType);
    Column<?> endClusteringKeyValue = getColumnWithMaxValue(CLUSTERING_KEY, clusteringKeyType);
    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> startClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      startClusteringKeyValue = clusteringKeyValues.get(0);
    } else {
      startClusteringKeyValue = clusteringKeyValues.get(4);
    }

    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> startClusteringKeyValue = getColumnWithMinValue(CLUSTERING_KEY, clusteringKeyType);
    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> endClusteringKeyValue;
    if (clusteringKeyType == DataType.BOOLEAN) {
      endClusteringKeyValue = clusteringKeyValues.get(1);
    } else {
      endClusteringKeyValue = clusteringKeyValues.get(14);
    }

    List<Column<?>> expected =
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
        random.setSeed(seed);

        truncateTable(clusteringKeyType, clusteringOrder);
        List<Column<?>> clusteringKeyValues = prepareRecords(clusteringKeyType, clusteringOrder);
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
      List<Column<?>> clusteringKeyValues,
      DataType clusteringKeyType,
      Order clusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Column<?> endClusteringKey = getColumnWithMaxValue(CLUSTERING_KEY, clusteringKeyType);
    List<Column<?>> expected =
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

  private List<Column<?>> prepareRecords(DataType clusteringKeyType, Order clusteringOrder)
      throws ExecutionException {
    List<Column<?>> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (clusteringKeyType == DataType.BOOLEAN) {
      TestUtils.booleanColumns(CLUSTERING_KEY)
          .forEach(
              clusteringKeyValue -> {
                ret.add(clusteringKeyValue);
                puts.add(preparePut(clusteringKeyType, clusteringOrder, clusteringKeyValue));
              });
    } else {
      Set<Column<?>> valueSet = new HashSet<>();

      // Add min and max clustering key values
      Arrays.asList(
              getColumnWithMinValue(CLUSTERING_KEY, clusteringKeyType),
              getColumnWithMaxValue(CLUSTERING_KEY, clusteringKeyType))
          .forEach(
              clusteringKeyValue -> {
                valueSet.add(clusteringKeyValue);
                ret.add(clusteringKeyValue);
                puts.add(preparePut(clusteringKeyType, clusteringOrder, clusteringKeyValue));
              });

      IntStream.range(0, CLUSTERING_KEY_NUM - 2)
          .forEach(
              i -> {
                Column<?> clusteringKeyValue;
                while (true) {
                  clusteringKeyValue =
                      getColumnWithRandomValue(random, CLUSTERING_KEY, clusteringKeyType);
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
      throw new ExecutionException("Put data to database failed", e);
    }

    ret.sort(
        clusteringOrder == Order.ASC
            ? com.google.common.collect.Ordering.natural()
            : com.google.common.collect.Ordering.natural().reverse());
    return ret;
  }

  private Put preparePut(
      DataType clusteringKeyType, Order clusteringOrder, Column<?> clusteringKeyValue) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(getTableName(clusteringKeyType, clusteringOrder))
        .partitionKey(getPartitionKey())
        .clusteringKey(Key.newBuilder().add(clusteringKeyValue).build())
        .intValue(COL_NAME, 1)
        .build();
  }

  private Key getPartitionKey() {
    return Key.ofInt(PARTITION_KEY, 1);
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

  private List<Column<?>> getExpected(
      List<Column<?>> clusteringKeyValues,
      @Nullable Column<?> startClusteringKeyValue,
      @Nullable Boolean startInclusive,
      @Nullable Column<?> endClusteringKeyValue,
      @Nullable Boolean endInclusive,
      OrderingType orderingType) {
    List<Column<?>> ret = new ArrayList<>();
    for (Column<?> clusteringKeyValue : clusteringKeyValues) {
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

  private int getLimit(boolean withLimit, List<Column<?>> expected) {
    int limit = 0;
    if (withLimit && !expected.isEmpty()) {
      if (expected.size() == 1) {
        limit = 1;
      } else {
        limit = random.nextInt(expected.size() - 1) + 1;
      }
    }
    return limit;
  }

  private Scan getScan(
      DataType clusteringKeyType,
      Order clusteringOrder,
      @Nullable Column<?> startClusteringKeyValue,
      @Nullable Boolean startInclusive,
      @Nullable Column<?> endClusteringKeyValue,
      @Nullable Boolean endInclusive,
      OrderingType orderingType,
      int limit) {
    ScanBuilder.BuildableScanWithPartitionKey scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName(clusteringKeyType, clusteringOrder))
            .partitionKey(getPartitionKey());
    if (startClusteringKeyValue != null && startInclusive != null) {
      scan.start(Key.newBuilder().add(startClusteringKeyValue).build(), startInclusive);
    }
    if (endClusteringKeyValue != null && endInclusive != null) {
      scan.end(Key.newBuilder().add(endClusteringKeyValue).build(), endInclusive);
    }
    switch (orderingType) {
      case SPECIFIED:
        scan.ordering(TestUtils.getOrdering(CLUSTERING_KEY, clusteringOrder));
        break;
      case REVERSED:
        scan.ordering(
            TestUtils.getOrdering(CLUSTERING_KEY, TestUtils.reverseOrder(clusteringOrder)));
        break;
      case NOTHING:
        break;
      default:
        throw new AssertionError();
    }
    if (limit > 0) {
      scan.limit(limit);
    }
    return scan.build();
  }

  private void assertScanResult(
      List<Result> actualResults, List<Column<?>> expected, String description) {
    List<Column<?>> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      assertThat(actualResult.contains(CLUSTERING_KEY)).isTrue();
      actual.add(actualResult.getColumns().get(CLUSTERING_KEY));
    }
    assertThat(actual).describedAs(description).isEqualTo(expected);
  }
}
