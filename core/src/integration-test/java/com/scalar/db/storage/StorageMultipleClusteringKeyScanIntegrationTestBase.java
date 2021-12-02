package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ListMultimap;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings(value = {"MS_PKGPROTECT", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
public abstract class StorageMultipleClusteringKeyScanIntegrationTestBase {

  protected static final String TEST_NAME = "mul_ckey";
  protected static final String NAMESPACE_BASE_NAME = "integration_testing_" + TEST_NAME + "_";
  protected static final String PARTITION_KEY = "pkey";
  protected static final String FIRST_CLUSTERING_KEY = "ckey1";
  protected static final String SECOND_CLUSTERING_KEY = "ckey2";
  protected static final String COL_NAME = "col";

  private static final int FIRST_CLUSTERING_KEY_NUM = 5;
  private static final int SECOND_CLUSTERING_KEY_NUM = 20;

  private static final Random RANDOM = new Random();

  private static final int THREAD_NUM = 5;

  private static boolean initialized;
  protected static DistributedStorageAdmin admin;
  protected static DistributedStorage storage;
  private static String namespaceBaseName;

  // Key: firstClusteringKeyType, Value: secondClusteringKeyType
  protected static ListMultimap<DataType, DataType> clusteringKeyTypes;

  private static long seed;

  private static ExecutorService executorService;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespaceBaseName = getNamespaceBaseName();
      clusteringKeyTypes = getClusteringKeyTypes();
      executorService = Executors.newFixedThreadPool(THREAD_NUM);
      createTables();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println(
          "The seed used in the multiple clustering key scan integration test is " + seed);
      initialized = true;
    }
  }

  protected abstract DatabaseConfig getDatabaseConfig();

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

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTables() throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Callable<Void>> testCallables = new ArrayList<>();

    Map<String, String> options = getCreateOptions();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            admin.createNamespace(getNamespaceName(firstClusteringKeyType), true, options);
            for (DataType secondClusteringKeyType :
                clusteringKeyTypes.get(firstClusteringKeyType)) {
              createTable(
                  firstClusteringKeyType, Order.ASC, secondClusteringKeyType, Order.ASC, options);
            }
            return null;
          };
      testCallables.add(testCallable);
    }

    // We firstly execute the first one and then the rest. This is because the first table creation
    // creates the metadata table, and this process can't be handled in multiple threads/processes
    // at the same time.
    execute(testCallables.subList(0, 1));
    execute(testCallables.subList(1, testCallables.size()));
  }

  private void createTable(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(firstClusteringKeyType),
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    admin.close();
    storage.close();
    executorService.shutdown();
  }

  private static void deleteTables()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            for (DataType secondClusteringKeyType :
                clusteringKeyTypes.get(firstClusteringKeyType)) {
              admin.dropTable(
                  getNamespaceName(firstClusteringKeyType),
                  getTableName(
                      firstClusteringKeyType, Order.ASC, secondClusteringKeyType, Order.ASC));
            }
            admin.dropNamespace(getNamespaceName(firstClusteringKeyType));
            return null;
          };
      testCallables.add(testCallable);
    }

    // We firstly execute the callables without the last one. And then we execute the last one. This
    // is because the last table deletion deletes the metadata table, and this process can't be
    // handled in multiple threads/processes at the same time.
    execute(testCallables.subList(0, testCallables.size() - 1));
    execute(testCallables.subList(testCallables.size() - 1, testCallables.size()));
  }

  private void truncateTable(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(firstClusteringKeyType),
        getTableName(
            firstClusteringKeyType,
            firstClusteringOrder,
            secondClusteringKeyType,
            secondClusteringOrder));
  }

  private static String getTableName(
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

  private static String getNamespaceName(DataType firstClusteringKeyType) {
    return namespaceBaseName + firstClusteringKeyType;
  }

  private static void execute(List<Callable<Void>> testCallables)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Void>> futures = executorService.invokeAll(testCallables);
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @Test
  public void scan_WithoutClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean reverse : Arrays.asList(false, true)) {
            scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
                clusteringKeys,
                firstClusteringKeyType,
                firstClusteringKeyOrder,
                secondClusteringKeyType,
                secondClusteringKeyOrder,
                reverse);
          }
        });
  }

  protected void scan_WithoutClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean reverse)
      throws ExecutionException, IOException {
    // Arrange
    Scan scan =
        new Scan(getPartitionKey())
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected = getExpected(clusteringKeys, null, null, null, null, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean endInclusive : Arrays.asList(true, false)) {
              for (Boolean reverse : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    startInclusive,
                    endInclusive,
                    reverse);
              }
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first), startInclusive)
            .withEnd(new Key(endClusteringKey.first), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean endInclusive : Arrays.asList(true, false)) {
              for (Boolean reverse : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    startInclusive,
                    endInclusive,
                    reverse);
              }
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startAndEndClusteringKey.first), startInclusive)
            .withEnd(new Key(startAndEndClusteringKey.first), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean endInclusive : Arrays.asList(true, false)) {
              for (Boolean reverse : Arrays.asList(false, true)) {
                scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    startInclusive,
                    endInclusive,
                    reverse);
              }
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey =
        new ClusteringKey(getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));
    ClusteringKey endClusteringKey =
        new ClusteringKey(getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first), startInclusive)
            .withEnd(new Key(endClusteringKey.first), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  startInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first), startInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  startInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean startInclusive,
      boolean reverse)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey startClusteringKey =
        new ClusteringKey(getMinValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first), startInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean endInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  endInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withEnd(new Key(endClusteringKey.first), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, reverse);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            firstClusteringKeyType, firstClusteringOrder, null, null, null, endInclusive, reverse));
  }

  @Test
  public void scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys, firstClusteringKeyType, firstClusteringKeyOrder) -> {
          for (Boolean endInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  endInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean endInclusive,
      boolean reverse)
      throws ExecutionException, IOException {
    // Arrange
    ClusteringKey endClusteringKey =
        new ClusteringKey(getMaxValue(FIRST_CLUSTERING_KEY, firstClusteringKeyType));

    Scan scan =
        new Scan(getPartitionKey())
            .withEnd(new Key(endClusteringKey.first), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(new Ordering(SECOND_CLUSTERING_KEY, reverse ? Order.DESC : Order.ASC))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType, firstClusteringOrder, DataType.INT, Order.ASC));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, reverse);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        expected,
        description(
            firstClusteringKeyType, firstClusteringOrder, null, null, null, endInclusive, reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean endInclusive : Arrays.asList(true, false)) {
              for (Boolean reverse : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyRange_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    startInclusive,
                    endInclusive,
                    reverse);
              }
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first, startClusteringKey.second), startInclusive)
            .withEnd(new Key(endClusteringKey.first, endClusteringKey.second), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean endInclusive : Arrays.asList(true, false)) {
              for (Boolean reverse : Arrays.asList(false, true)) {
                scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
                    clusteringKeys,
                    firstClusteringKeyType,
                    firstClusteringKeyOrder,
                    secondClusteringKeyType,
                    secondClusteringKeyOrder,
                    startInclusive,
                    endInclusive,
                    reverse);
              }
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(
                new Key(startAndEndClusteringKey.first, startAndEndClusteringKey.second),
                startInclusive)
            .withEnd(
                new Key(startAndEndClusteringKey.first, startAndEndClusteringKey.second),
                endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startAndEndClusteringKey,
            startInclusive,
            startAndEndClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean useMinValueForFirstClusteringKeyValue : Arrays.asList(true, false)) {
            for (Boolean startInclusive : Arrays.asList(true, false)) {
              for (Boolean endInclusive : Arrays.asList(true, false)) {
                for (Boolean reverse : Arrays.asList(false, true)) {
                  scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult(
                      clusteringKeys,
                      firstClusteringKeyType,
                      firstClusteringKeyOrder,
                      useMinValueForFirstClusteringKeyValue,
                      secondClusteringKeyType,
                      secondClusteringKeyOrder,
                      startInclusive,
                      endInclusive,
                      reverse);
                }
              }
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      boolean useMinValueForFirstClusteringKeyValue,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first, startClusteringKey.second), startInclusive)
            .withEnd(new Key(endClusteringKey.first, endClusteringKey.second), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(
            clusteringKeys,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  secondClusteringKeyType,
                  secondClusteringKeyOrder,
                  startInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first, startClusteringKey.second), startInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean startInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  secondClusteringKeyType,
                  secondClusteringKeyOrder,
                  startInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean startInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withStart(new Key(startClusteringKey.first, startClusteringKey.second), startInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, startClusteringKey, startInclusive, null, null, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean endInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  secondClusteringKeyType,
                  secondClusteringKeyOrder,
                  endInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withEnd(new Key(endClusteringKey.first, endClusteringKey.second), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, reverse);

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
            reverse));
  }

  @Test
  public void scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    execute(
        (clusteringKeys,
            firstClusteringKeyType,
            firstClusteringKeyOrder,
            secondClusteringKeyType,
            secondClusteringKeyOrder) -> {
          for (Boolean endInclusive : Arrays.asList(true, false)) {
            for (Boolean reverse : Arrays.asList(false, true)) {
              scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
                  clusteringKeys,
                  firstClusteringKeyType,
                  firstClusteringKeyOrder,
                  secondClusteringKeyType,
                  secondClusteringKeyOrder,
                  endInclusive,
                  reverse);
            }
          }
        });
  }

  protected void scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult(
      List<ClusteringKey> clusteringKeys,
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder,
      boolean endInclusive,
      boolean reverse)
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

    Scan scan =
        new Scan(getPartitionKey())
            .withEnd(new Key(endClusteringKey.first, endClusteringKey.second), endInclusive)
            .withOrdering(
                new Ordering(
                    FIRST_CLUSTERING_KEY,
                    reverse ? reverseOrder(firstClusteringOrder) : firstClusteringOrder))
            .withOrdering(
                new Ordering(
                    SECOND_CLUSTERING_KEY,
                    reverse ? reverseOrder(secondClusteringOrder) : secondClusteringOrder))
            .forNamespace(getNamespaceName(firstClusteringKeyType))
            .forTable(
                getTableName(
                    firstClusteringKeyType,
                    firstClusteringOrder,
                    secondClusteringKeyType,
                    secondClusteringOrder));

    List<ClusteringKey> expected =
        getExpected(clusteringKeys, null, null, endClusteringKey, endInclusive, reverse);

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
            reverse));
  }

  private List<ClusteringKey> prepareRecords(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder)
      throws ExecutionException {
    RANDOM.setSeed(seed);

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
      throw new ExecutionException("put data to database failed", e);
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
      List<Value<?>> secondClusteringKeyValues =
          getSecondClusteringKeyValues(secondClusteringKeyType);
      for (Value<?> secondClusteringKeyValue : secondClusteringKeyValues) {
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
        .forNamespace(getNamespaceName(firstClusteringKeyType))
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
      boolean reverse) {
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
    builder.append(String.format(", reverse: %s", reverse));
    return builder.toString();
  }

  private Order reverseOrder(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError();
    }
  }

  private Key getPartitionKey() {
    return new Key(PARTITION_KEY, 1);
  }

  private Value<?> getFirstClusteringKeyValue(DataType dataType) {
    return getRandomValue(RANDOM, FIRST_CLUSTERING_KEY, dataType);
  }

  private List<Value<?>> getSecondClusteringKeyValues(DataType dataType) {
    Set<Value<?>> valueSet = new HashSet<>();
    List<Value<?>> ret = new ArrayList<>();

    // min and max second clustering key values
    Arrays.asList(
            getMinValue(SECOND_CLUSTERING_KEY, dataType),
            getMaxValue(SECOND_CLUSTERING_KEY, dataType))
        .forEach(
            value -> {
              ret.add(value);
              valueSet.add(value);
            });

    for (int i = 0; i < SECOND_CLUSTERING_KEY_NUM - 2; i++) {
      while (true) {
        Value<?> value = getRandomValue(RANDOM, SECOND_CLUSTERING_KEY, dataType);
        // reject duplication
        if (!valueSet.contains(value)) {
          ret.add(value);
          valueSet.add(value);
          break;
        }
      }
    }

    return ret;
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
      boolean reverse) {
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

    if (reverse) {
      Collections.reverse(ret);
    }
    return ret;
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

  private void execute(TestForFirstClusteringKeyScan test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      testCallables.add(
          () -> {
            truncateTable(firstClusteringKeyType, Order.ASC, DataType.INT, Order.ASC);

            List<ClusteringKey> clusteringKeys =
                prepareRecords(firstClusteringKeyType, Order.ASC, DataType.INT, Order.ASC);

            test.execute(clusteringKeys, firstClusteringKeyType, Order.ASC);
            return null;
          });
    }

    execute(testCallables);
  }

  private void execute(TestForSecondClusteringKeyScan test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstClusteringKeyType : clusteringKeyTypes.keySet()) {
      for (DataType secondClusteringKeyType : clusteringKeyTypes.get(firstClusteringKeyType)) {
        testCallables.add(
            () -> {
              truncateTable(firstClusteringKeyType, Order.ASC, secondClusteringKeyType, Order.ASC);

              List<ClusteringKey> clusteringKeys =
                  prepareRecords(
                      firstClusteringKeyType, Order.ASC, secondClusteringKeyType, Order.ASC);

              test.execute(
                  clusteringKeys,
                  firstClusteringKeyType,
                  Order.ASC,
                  secondClusteringKeyType,
                  Order.ASC);
              return null;
            });
      }
    }
    execute(testCallables);
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
