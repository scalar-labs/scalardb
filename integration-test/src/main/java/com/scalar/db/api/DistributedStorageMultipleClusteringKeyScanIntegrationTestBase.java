package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ListMultimap;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageMultipleClusteringKeyScanIntegrationTestBase.class);

  private enum OrderingType {
    BOTH_SPECIFIED,
    ONLY_FIRST_SPECIFIED,
    BOTH_SPECIFIED_AND_REVERSED,
    ONLY_FIRST_SPECIFIED_AND_REVERSED,
    NOTHING
  }

  private static final String TEST_NAME = "storage_mul_ckey";
  private static final String NAMESPACE_BASE_NAME = "int_test_" + TEST_NAME + "_";
  private static final String PARTITION_KEY = "pkey";
  private static final String FIRST_CLUSTERING_KEY = "ckey1";
  private static final String SECOND_CLUSTERING_KEY = "ckey2";
  private static final String COL_NAME = "col";

  private static final int FIRST_CLUSTERING_KEY_NUM = 5;
  private static final int SECOND_CLUSTERING_KEY_NUM = 20;

  private static final int THREAD_NUM = 10;

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespaceBaseName;

  // Key: firstClusteringKeyType, Value: secondClusteringKeyType
  private ListMultimap<DataType, DataType> clusteringKeyTypes;

  private long seed;
  private ThreadLocal<Random> random;

  private ExecutorService executorService;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespaceBaseName = getNamespaceBaseName();
    clusteringKeyTypes = getClusteringKeyTypes();
    executorService = Executors.newFixedThreadPool(getThreadNum());
    createTables();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println(
        "The seed used in the multiple clustering key scan integration test is " + seed);
    random = ThreadLocal.withInitial(Random::new);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType firstClusteringKeyType : DataType.values()) {
      for (DataType secondClusteringKeyType : DataType.values()) {
        clusteringKeyTypes.put(firstClusteringKeyType, secondClusteringKeyType);
      }
    }
    return clusteringKeyTypes;
  }

  protected int getThreadNum() {
    return THREAD_NUM;
  }

  protected boolean isParallelDdlSupported() {
    return true;
  }

  private void createTables()
      throws java.util.concurrent.ExecutionException, InterruptedException, ExecutionException {
    List<Callable<Void>> testCallables = new ArrayList<>();

    Map<String, String> options = getCreationOptions();
    admin.createNamespace(getNamespaceName(), true, options);
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            for (DataType secondClusteringKeyType :
                clusteringKeyTypes.get(firstClusteringKeyType)) {
              for (Order firstClusteringOrder : Order.values()) {
                for (Order secondClusteringOrder : Order.values()) {
                  createTable(
                      firstClusteringKeyType,
                      firstClusteringOrder,
                      secondClusteringKeyType,
                      secondClusteringOrder,
                      options);
                }
              }
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
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(),
        getTableName(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder),
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, DataType.INT)
            .addColumn(FIRST_CLUSTERING_KEY, firstClusteringKeyType)
            .addColumn(SECOND_CLUSTERING_KEY, secondClusteringKeyType)
            .addColumn(COL_NAME, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
            .addClusteringKey(FIRST_CLUSTERING_KEY, firstClusteringOrder)
            .addClusteringKey(SECOND_CLUSTERING_KEY, secondClusteringOrder)
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

    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } catch (Exception e) {
      logger.warn("Failed to shutdown executor service", e);
    }
  }

  private void dropTables()
      throws java.util.concurrent.ExecutionException, InterruptedException, ExecutionException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            for (DataType secondClusteringKeyType :
                clusteringKeyTypes.get(firstClusteringKeyType)) {
              for (Order firstClusteringOrder : Order.values()) {
                for (Order secondClusteringOrder : Order.values()) {
                  admin.dropTable(
                      getNamespaceName(),
                      getTableName(
                          firstClusteringKeyType,
                          firstClusteringOrder,
                          secondClusteringKeyType,
                          secondClusteringOrder));
                }
              }
            }
            return null;
          };
      testCallables.add(testCallable);
    }

    // We firstly execute the callables without the last one. And then we execute the last one. This
    // is because the last table deletion deletes the metadata table, and this process can't be
    // handled in multiple threads/processes at the same time.
    executeDdls(testCallables.subList(0, testCallables.size() - 1));
    executeDdls(testCallables.subList(testCallables.size() - 1, testCallables.size()));
    admin.dropNamespace(getNamespaceName());
  }

  private void truncateTable(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(),
        getTableName(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder));
  }

  private String getTableName(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder) {
    return String.join(
        "_",
        firstClusteringKeyType.toString(),
        firstClusteringOrder.toString(),
        secondClusteringKeyType.toString(),
        secondClusteringOrder.toString());
  }

  private String getNamespaceName() {
    return namespaceBaseName + "all";
  }

  @Test
  public void scan_WithoutClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (OrderingType orderingType : OrderingType.values()) {
            for (boolean withLimit : Arrays.asList(false, true)) {
              scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  secondClusteringKeyType,
                  secondClusteringKeyOrder,
                  orderingType,
                  withLimit);
            }
          }
        });
  }

  private void scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            null,
            null,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            null,
            null,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (boolean endInclusive : Arrays.asList(true, false)) {
              for (OrderingType orderingType : OrderingType.values()) {
                for (boolean withLimit : Arrays.asList(false, true)) {
                  scan_WithFirstClusteringKeyRange_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      orderingType,
                      withLimit);
                }
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey;
    ClusteringKey endClusteringKey;
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(0, DataType.INT)).first);
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(1, DataType.INT)).first);
    } else {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(1, DataType.INT)).first);
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(3, DataType.INT)).first);
    }

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
            startClusteringKey,
            startInclusive,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (boolean endInclusive : Arrays.asList(true, false)) {
              for (OrderingType orderingType : OrderingType.values()) {
                for (boolean withLimit : Arrays.asList(false, true)) {
                  scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      orderingType,
                      withLimit);
                }
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startAndEndClusteringKey;
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      startAndEndClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(0, DataType.INT)).first);
    } else {
      startAndEndClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(2, DataType.INT)).first);
    }

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (boolean endInclusive : Arrays.asList(true, false)) {
              for (OrderingType orderingType : OrderingType.values()) {
                for (boolean withLimit : Arrays.asList(false, true)) {
                  scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      orderingType,
                      withLimit);
                }
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey =
        new ClusteringKey(getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));
    ClusteringKey endClusteringKey =
        new ClusteringKey(getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));
    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
            startClusteringKey,
            startInclusive,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    startInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey;
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(0, DataType.INT)).first);
    } else {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(1, DataType.INT)).first);
    }

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
            startClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            startInclusive,
            null,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    startInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey =
        new ClusteringKey(getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));
    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
            startClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            startInclusive,
            null,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey endClusteringKey;
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(1, DataType.INT)).first);
    } else {
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(getFirstClusteringKeyIndex(3, DataType.INT)).first);
    }

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            null,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey endClusteringKey =
        new ClusteringKey(getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));
    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            DataType.INT,
            Order.ASC,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            null,
            null,
            null,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (boolean endInclusive : Arrays.asList(true, false)) {
              for (OrderingType orderingType : OrderingType.values()) {
                for (boolean withLimit : Arrays.asList(false, true)) {
                  scan_WithSecondClusteringKeyRange_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      secondClusteringKeyType,
                      secondClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      orderingType,
                      withLimit);
                }
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      Value<?> firstClusteringKeyValue = clusteringKeys.get(0).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    } else {
      Value<?> firstClusteringKeyValue =
          clusteringKeys.get(getFirstClusteringKeyIndex(2, secondClusteringKeyType)).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    }

    ClusteringKey startClusteringKey;
    ClusteringKey endClusteringKey;
    if (secondClusteringKeyType == DataType.BOOLEAN) {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(0).first, clusteringKeys.get(0).second);
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(1).first, clusteringKeys.get(1).second);
    } else {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(4).first, clusteringKeys.get(4).second);
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(14).first, clusteringKeys.get(14).second);
    }

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startClusteringKey,
            startInclusive,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (boolean endInclusive : Arrays.asList(true, false)) {
              for (OrderingType orderingType : OrderingType.values()) {
                for (boolean withLimit : Arrays.asList(false, true)) {
                  scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      secondClusteringKeyType,
                      secondClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      orderingType,
                      withLimit);
                }
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      Value<?> firstClusteringKeyValue = clusteringKeys.get(0).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    } else {
      Value<?> firstClusteringKeyValue =
          clusteringKeys.get(getFirstClusteringKeyIndex(2, secondClusteringKeyType)).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    }

    ClusteringKey startAndEndClusteringKey;
    if (secondClusteringKeyType == DataType.BOOLEAN) {
      startAndEndClusteringKey =
          new ClusteringKey(clusteringKeys.get(0).first, clusteringKeys.get(0).second);
    } else {
      startAndEndClusteringKey =
          new ClusteringKey(clusteringKeys.get(9).first, clusteringKeys.get(9).second);
    }

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean useMinValueForFirstClusteringKeyValue : Arrays.asList(true, false)) {
            for (boolean startInclusive : Arrays.asList(true, false)) {
              for (boolean endInclusive : Arrays.asList(true, false)) {
                for (OrderingType orderingType : OrderingType.values()) {
                  for (boolean withLimit : Arrays.asList(false, true)) {
                    scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult(
                        clusteringKeys,
                        firstClusteringKeyType,
                        firstClusteringKeyOrder,
                        useMinValueForFirstClusteringKeyValue,
                        secondClusteringKeyType,
                        secondClusteringKeyOrder,
                        startInclusive,
                        endInclusive,
                        orderingType,
                        withLimit);
                  }
                }
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean useMinValueForFirstClusteringKeyValue,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> firstClusteringKeyValue =
        useMinValueForFirstClusteringKeyValue
            ? getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType)
            : getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType);
    clusteringKeys =
        clusteringKeys.stream()
            .filter(c -> c.first.equals(firstClusteringKeyValue))
            .collect(Collectors.toList());
    ClusteringKey startClusteringKey =
        new ClusteringKey(
            firstClusteringKeyValue, getMinValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType));
    ClusteringKey endClusteringKey =
        new ClusteringKey(
            firstClusteringKeyValue, getMaxValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType));
    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startClusteringKey,
            startInclusive,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startInclusive,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    startInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      Value<?> firstClusteringKeyValue = clusteringKeys.get(0).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    } else {
      Value<?> firstClusteringKeyValue =
          clusteringKeys.get(getFirstClusteringKeyIndex(2, secondClusteringKeyType)).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    }

    ClusteringKey startClusteringKey;
    if (secondClusteringKeyType == DataType.BOOLEAN) {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(0).first, clusteringKeys.get(0).second);
    } else {
      startClusteringKey =
          new ClusteringKey(clusteringKeys.get(4).first, clusteringKeys.get(4).second);
    }

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startInclusive,
            null,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean startInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    startInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> firstClusteringKeyValue = getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType);
    clusteringKeys =
        clusteringKeys.stream()
            .filter(c -> c.first.equals(firstClusteringKeyValue))
            .collect(Collectors.toList());
    ClusteringKey startClusteringKey =
        new ClusteringKey(
            firstClusteringKeyValue, getMinValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType));
    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startClusteringKey,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            startInclusive,
            null,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    if (firstClusteringKeyType == DataType.BOOLEAN) {
      Value<?> firstClusteringKeyValue = clusteringKeys.get(0).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    } else {
      Value<?> firstClusteringKeyValue =
          clusteringKeys.get(getFirstClusteringKeyIndex(2, secondClusteringKeyType)).first;
      clusteringKeys =
          clusteringKeys.stream()
              .filter(c -> c.first.equals(firstClusteringKeyValue))
              .collect(Collectors.toList());
    }

    ClusteringKey endClusteringKey;
    if (secondClusteringKeyType == DataType.BOOLEAN) {
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(1).first, clusteringKeys.get(1).second);
    } else {
      endClusteringKey =
          new ClusteringKey(clusteringKeys.get(14).first, clusteringKeys.get(14).second);
    }

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            null,
            endInclusive,
            orderingType,
            withLimit));
  }

  @Test
  public void scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (boolean endInclusive : Arrays.asList(true, false)) {
            for (OrderingType orderingType : OrderingType.values()) {
              for (boolean withLimit : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    endInclusive,
                    orderingType,
                    withLimit);
              }
            }
          }
        });
  }

  private void scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit)
      throws ExecutionException, IOException {
    // Arrange
    Value<?> firstClusteringKeyValue = getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType);
    clusteringKeys =
        clusteringKeys.stream()
            .filter(c -> c.first.equals(firstClusteringKeyValue))
            .collect(Collectors.toList());
    ClusteringKey endClusteringKey =
        new ClusteringKey(
            firstClusteringKeyValue, getMaxValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, orderingType);

    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }

    Scan scan =
        getScan(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
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
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder,
            null,
            endInclusive,
            orderingType,
            withLimit));
  }

  private List<ClusteringKey> prepareRecords(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder)
      throws ExecutionException {
    List<ClusteringKey> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (firstClusteringKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(FIRST_CLUSTERING_KEY)
          .forEach(
              firstClusteringKeyValue ->
                  prepareRecords(
                      firstClusteringKeyType,
                      firstClusteringOrder,
                      firstClusteringKeyValue,
                      secondClusteringKeyType,
                      secondClusteringOrder,
                      puts,
                      ret));
    } else {
      Set<Value<?>> valueSet = new HashSet<>();

      // Add min and max first clustering key values
      Arrays.asList(
              getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType),
              getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType))
          .forEach(
              firstClusteringKeyValue -> {
                valueSet.add(firstClusteringKeyValue);
                prepareRecords(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    firstClusteringKeyValue,
                    secondClusteringKeyType,
                    secondClusteringOrder,
                    puts,
                    ret);
              });

      IntStream.range(0, FIRST_CLUSTERING_KEY_NUM - 2)
          .forEach(
              i -> {
                Value<?> firstClusteringKeyValue;
                while (true) {
                  firstClusteringKeyValue = getFirstClusteringKeyValue(firstClusteringKeyType);
                  // reject duplication
                  if (!valueSet.contains(firstClusteringKeyValue)) {
                    valueSet.add(firstClusteringKeyValue);
                    break;
                  }
                }

                prepareRecords(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    firstClusteringKeyValue,
                    secondClusteringKeyType,
                    secondClusteringOrder,
                    puts,
                    ret);
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
    ret.sort(getClusteringKeyComparator(firstClusteringOrder, secondClusteringOrder));
    return ret;
  }

  private void prepareRecords(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      Value<?> firstClusteringKeyValue,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      List<Put> puts,
      List<ClusteringKey> ret) {
    if (secondClusteringKeyType == DataType.BOOLEAN) {
      TestUtils.booleanValues(SECOND_CLUSTERING_KEY)
          .forEach(
              secondClusteringKeyValue -> {
                ret.add(new ClusteringKey(firstClusteringKeyValue, secondClusteringKeyValue));
                puts.add(
                    preparePut(
                        firstClusteringKeyType,
                        firstClusteringOrder,
                        firstClusteringKeyValue,
                        secondClusteringKeyType,
                        secondClusteringOrder,
                        secondClusteringKeyValue));
              });
    } else {
      Set<Value<?>> valueSet = new HashSet<>();

      // min and max second clustering key values
      Arrays.asList(
              getMinValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType),
              getMaxValue(SECOND_CLUSTERING_KEY, secondClusteringKeyType))
          .forEach(
              secondClusteringKeyValue -> {
                ret.add(new ClusteringKey(firstClusteringKeyValue, secondClusteringKeyValue));
                puts.add(
                    preparePut(
                        firstClusteringKeyType,
                        firstClusteringOrder,
                        firstClusteringKeyValue,
                        secondClusteringKeyType,
                        secondClusteringOrder,
                        secondClusteringKeyValue));
                valueSet.add(secondClusteringKeyValue);
              });

      for (int i = 0; i < SECOND_CLUSTERING_KEY_NUM - 2; i++) {
        Value<?> secondClusteringKeyValue;
        while (true) {
          secondClusteringKeyValue =
              getRandomValue(random.get(), SECOND_CLUSTERING_KEY, secondClusteringKeyType);
          // reject duplication
          if (!valueSet.contains(secondClusteringKeyValue)) {
            valueSet.add(secondClusteringKeyValue);
            break;
          }
        }

        ret.add(new ClusteringKey(firstClusteringKeyValue, secondClusteringKeyValue));
        puts.add(
            preparePut(
                firstClusteringKeyType,
                firstClusteringOrder,
                firstClusteringKeyValue,
                secondClusteringKeyType,
                secondClusteringOrder,
                secondClusteringKeyValue));
      }
    }
  }

  private Put preparePut(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      Value<?> firstClusteringKeyValue,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      Value<?> secondClusteringKeyValue) {
    return new Put(getPartitionKey(), new Key(firstClusteringKeyValue, secondClusteringKeyValue))
        .withValue(COL_NAME, 1)
        .forNamespace(getNamespaceName())
        .forTable(
            getTableName(
                firstClusteringKeyType,
                firstClusteringOrder,
                secondClusteringKeyType,
                secondClusteringOrder));
  }

  private int getFirstClusteringKeyIndex(int i, DataType secondClusteringKeyType) {
    return i * (secondClusteringKeyType == DataType.BOOLEAN ? 2 : SECOND_CLUSTERING_KEY_NUM);
  }

  private String description(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      @Nullable DataType secondClusteringKeyType,
      @Nullable Order secondClusteringOrder,
      @Nullable Boolean startInclusive,
      @Nullable Boolean endInclusive,
      OrderingType orderingType,
      boolean withLimit) {
    StringBuilder builder = new StringBuilder();
    builder.append(
        String.format(
            "failed with firstClusteringKeyType: %s, firstClusteringOrder: %s",
            firstClusteringKeyType, firstClusteringOrder));
    if (secondClusteringKeyType != null) {
      builder.append(String.format(", secondClusteringKeyType: %s", secondClusteringKeyType));
    }
    if (secondClusteringOrder != null) {
      builder.append(String.format(", secondClusteringOrder: %s", secondClusteringOrder));
    }
    if (startInclusive != null) {
      builder.append(String.format(", startInclusive: %s", startInclusive));
    }
    if (endInclusive != null) {
      builder.append(String.format(", endInclusive: %s", endInclusive));
    }
    builder.append(String.format(", orderingType: %s, withLimit: %b", orderingType, withLimit));
    return builder.toString();
  }

  private Key getPartitionKey() {
    return new Key(PARTITION_KEY, 1);
  }

  private Value<?> getFirstClusteringKeyValue(DataType dataType) {
    return getRandomValue(random.get(), FIRST_CLUSTERING_KEY, dataType);
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

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  private List<ClusteringKey> getExpected(
      List<ClusteringKey> clusteringKeys,
      @Nullable ClusteringKey startClusteringKey,
      @Nullable Boolean startInclusive,
      @Nullable ClusteringKey endClusteringKey,
      @Nullable Boolean endInclusive,
      OrderingType orderingType) {
    List<ClusteringKey> ret = new ArrayList<>();
    for (ClusteringKey clusteringKey : clusteringKeys) {
      if (startClusteringKey != null && startInclusive != null) {
        int compare = clusteringKey.compareTo(startClusteringKey);
        if (!(startInclusive ? compare >= 0 : compare > 0)) {
          continue;
        }
      }
      if (endClusteringKey != null && endInclusive != null) {
        int compare = clusteringKey.compareTo(endClusteringKey);
        if (!(endInclusive ? compare <= 0 : compare < 0)) {
          continue;
        }
      }
      ret.add(clusteringKey);
    }
    if (orderingType == OrderingType.BOTH_SPECIFIED_AND_REVERSED
        || orderingType == OrderingType.ONLY_FIRST_SPECIFIED_AND_REVERSED) {
      Collections.reverse(ret);
    }
    return ret;
  }

  private int getLimit(boolean withLimit, List<ClusteringKey> expected) {
    int limit = 0;
    if (withLimit && !expected.isEmpty()) {
      if (expected.size() == 1) {
        limit = 1;
      } else {
        limit = random.get().nextInt(expected.size() - 1) + 1;
      }
    }
    return limit;
  }

  private Scan getScan(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      @Nullable ClusteringKey startClusteringKey,
      @Nullable Boolean startInclusive,
      @Nullable ClusteringKey endClusteringKey,
      @Nullable Boolean endInclusive,
      OrderingType orderingType,
      int limit) {
    Scan scan =
        new Scan(getPartitionKey())
            .forNamespace(getNamespaceName())
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));
    if (startClusteringKey != null && startInclusive != null) {
      Key key;
      if (startClusteringKey.second != null) {
        key = new Key(startClusteringKey.first, startClusteringKey.second);
      } else {
        key = new Key(startClusteringKey.first);
      }
      scan.withStart(key, startInclusive);
    }
    if (endClusteringKey != null && endInclusive != null) {
      Key key;
      if (endClusteringKey.second != null) {
        key = new Key(endClusteringKey.first, endClusteringKey.second);
      } else {
        key = new Key(endClusteringKey.first);
      }
      scan.withEnd(key, endInclusive);
    }
    switch (orderingType) {
      case BOTH_SPECIFIED:
        scan.withOrdering(new Ordering(FIRST_CLUSTERING_KEY, firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, secondClusteringOrder));
        break;
      case ONLY_FIRST_SPECIFIED:
        scan.withOrdering(new Ordering(FIRST_CLUSTERING_KEY, firstClusteringOrder));
        break;
      case BOTH_SPECIFIED_AND_REVERSED:
        scan.withOrdering(
                new Ordering(FIRST_CLUSTERING_KEY, TestUtils.reverseOrder(firstClusteringOrder)))
            .withOrdering(
                new Ordering(SECOND_CLUSTERING_KEY, TestUtils.reverseOrder(secondClusteringOrder)));
        break;
      case ONLY_FIRST_SPECIFIED_AND_REVERSED:
        scan.withOrdering(
            new Ordering(FIRST_CLUSTERING_KEY, TestUtils.reverseOrder(firstClusteringOrder)));
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
      List<Result> actualResults, List<ClusteringKey> expected, String description) {
    List<ClusteringKey> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      assertThat(actualResult.getValue(FIRST_CLUSTERING_KEY).isPresent()).isTrue();
      assertThat(actualResult.getValue(SECOND_CLUSTERING_KEY).isPresent()).isTrue();
      actual.add(
          new ClusteringKey(
              actualResult.getValue(FIRST_CLUSTERING_KEY).get(),
              actualResult.getValue(SECOND_CLUSTERING_KEY).get()));
    }
    assertThat(actual).describedAs(description).isEqualTo(expected);
  }

  private Comparator<ClusteringKey> getClusteringKeyComparator(
      Order firstClusteringOrder, Order secondClusteringOrder) {
    return (l, r) -> {
      ComparisonChain chain =
          ComparisonChain.start()
              .compare(
                  l.first,
                  r.first,
                  firstClusteringOrder == Order.ASC
                      ? com.google.common.collect.Ordering.natural()
                      : com.google.common.collect.Ordering.natural().reverse());
      if (l.second != null && r.second != null) {
        chain =
            chain.compare(
                l.second,
                r.second,
                secondClusteringOrder == Order.ASC
                    ? com.google.common.collect.Ordering.natural()
                    : com.google.common.collect.Ordering.natural().reverse());
      }
      return chain.result();
    };
  }

  private void executeInParallel(TestForFirstClusteringKeyScan test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      for (Order firstClusteringOrder : Order.values()) {
        testCallables.add(
            () -> {
              random.get().setSeed(seed);

              truncateTable(firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC);

              List<ClusteringKey> clusteringKeys =
                  prepareRecords(
                      firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC);

              test.execute(clusteringKeys, firstClusteringKeyType, firstClusteringOrder);
              return null;
            });
      }
    }

    executeInParallel(testCallables);
  }

  private void executeInParallel(TestForSecondClusteringKeyScan test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      for (DataType secondClusteringKeyType : clusteringKeyTypes.get(firstClusteringKeyType)) {
        for (Order firstClusteringOrder : Order.values()) {
          for (Order secondClusteringOrder : Order.values()) {
            testCallables.add(
                () -> {
                  random.get().setSeed(seed);

                  truncateTable(
                      firstClusteringKeyType,
                      firstClusteringOrder,
                      secondClusteringKeyType,
                      secondClusteringOrder);

                  List<ClusteringKey> clusteringKeys =
                      prepareRecords(
                          firstClusteringKeyType,
                          firstClusteringOrder,
                          secondClusteringKeyType,
                          secondClusteringOrder);

                  test.execute(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringOrder,
                      secondClusteringKeyType,
                      secondClusteringOrder);
                  return null;
                });
          }
        }
      }
    }
    executeInParallel(testCallables);
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

  @FunctionalInterface
  private interface TestForFirstClusteringKeyScan {
    void execute(
        List<ClusteringKey> clusteringKeys,
        DataType firstClusteringKeyType,
        Order firstClusteringOrder)
        throws Exception;
  }

  @FunctionalInterface
  private interface TestForSecondClusteringKeyScan {
    void execute(
        List<ClusteringKey> clusteringKeys,
        DataType firstClusteringKeyType,
        Order firstClusteringOrder,
        DataType secondClusteringKeyType,
        Order secondClusteringOrder)
        throws Exception;
  }

  private static class ClusteringKey implements Comparable<ClusteringKey> {
    public final Value<?> first;
    @Nullable public final Value<?> second;

    public ClusteringKey(Value<?> first, @Nullable Value<?> second) {
      this.first = first;
      this.second = second;
    }

    public ClusteringKey(Value<?> first) {
      this(first, null);
    }

    @Override
    public int compareTo(ClusteringKey o) {
      ComparisonChain chain = ComparisonChain.start().compare(first, o.first);
      if (second != null && o.second != null) {
        chain = chain.compare(second, o.second);
      }
      return chain.result();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ClusteringKey)) {
        return false;
      }
      ClusteringKey that = (ClusteringKey) o;
      return first.equals(that.first) && Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
      return Objects.hash(first, second);
    }

    @Override
    public String toString() {
      return "ClusteringKey{" + "first=" + first + ", second=" + second + '}';
    }
  }
}
