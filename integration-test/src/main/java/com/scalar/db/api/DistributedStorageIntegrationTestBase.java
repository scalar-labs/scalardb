package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.TestUtils;
import com.scalar.db.util.TestUtils.ExpectedResult;
import com.scalar.db.util.TestUtils.ExpectedResult.ExpectedResultBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageIntegrationTestBase.class);

  private static final String TEST_NAME = "storage";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";

  protected DistributedStorage storage;
  protected DistributedStorageAdmin admin;
  protected String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(getTestName());
    StorageFactory factory = StorageFactory.create(getProperties(getTestName()));
    admin = factory.getStorageAdmin();
    namespace = getNamespace();
    createTable();
    storage = factory.getStorage();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getTestName() {
    return TEST_NAME;
  }

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected String getTableName() {
    return TABLE;
  }

  protected String getColumnName1() {
    return COL_NAME1;
  }

  protected String getColumnName2() {
    return COL_NAME2;
  }

  protected String getColumnName3() {
    return COL_NAME3;
  }

  protected String getColumnName4() {
    return COL_NAME4;
  }

  protected String getColumnName5() {
    return COL_NAME5;
  }

  protected String getColumnName6() {
    return COL_NAME6;
  }

  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(getColumnName1(), DataType.INT)
        .addColumn(getColumnName2(), DataType.TEXT)
        .addColumn(getColumnName3(), DataType.INT)
        .addColumn(getColumnName4(), DataType.INT)
        .addColumn(getColumnName5(), DataType.BOOLEAN)
        .addColumn(getColumnName6(), DataType.BLOB)
        .addPartitionKey(getColumnName1())
        .addClusteringKey(getColumnName4())
        .addSecondaryIndex(getColumnName3())
        .build();
  }

  protected DistributedStorage getStorage() {
    return storage;
  }

  protected DistributedStorageAdmin getAdmin() {
    return admin;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, getTableName(), getTableMetadata(), true, options);
  }

  protected int getLargeDataSizeInBytes() {
    return 5000;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace, getTableName());
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop table", e);
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

  private void dropTable() throws ExecutionException {
    admin.dropTable(namespace, getTableName());
    admin.dropNamespace(namespace);
  }

  @Test
  public void operation_NoTargetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 0);
    Key clusteringKey = Key.ofInt(getColumnName4(), 0);
    Get get =
        Get.newBuilder()
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_WrongNamespaceGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 0);
    Key clusteringKey = Key.ofInt(getColumnName4(), 0);
    Get get =
        Get.newBuilder()
            .namespace("wrong_" + namespace) // a wrong namespace
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();
    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_WrongTableGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 0);
    Key clusteringKey = Key.ofInt(getColumnName4(), 0);
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table("wrong_" + getTableName()) // a wrong table
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_DefaultNamespaceGiven_ShouldWorkProperly() {
    Properties properties = getProperties(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace);
    final DistributedStorage storageWithDefaultNamespace =
        StorageFactory.create(properties).getStorage();
    try {
      // Arrange
      populateRecords();
      Get get =
          Get.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 0))
              .clusteringKey(Key.ofInt(getColumnName4(), 0))
              .build();
      Scan scan =
          Scan.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 0))
              .build();
      Put put =
          Put.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 1))
              .clusteringKey(Key.ofInt(getColumnName4(), 0))
              .textValue(getColumnName2(), "foo")
              .build();
      Delete delete =
          Delete.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 2))
              .clusteringKey(Key.ofInt(getColumnName4(), 0))
              .build();
      Mutation putAsMutation1 =
          Put.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 3))
              .clusteringKey(Key.ofInt(getColumnName4(), 0))
              .textValue(getColumnName2(), "foo")
              .build();
      Mutation deleteAsMutation2 =
          Delete.newBuilder()
              .table(getTableName())
              .partitionKey(Key.ofInt(getColumnName1(), 3))
              .clusteringKey(Key.ofInt(getColumnName4(), 1))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                storageWithDefaultNamespace.get(get);
                storageWithDefaultNamespace.scan(scan).close();
                storageWithDefaultNamespace.put(put);
                storageWithDefaultNamespace.delete(delete);
                storageWithDefaultNamespace.mutate(
                    ImmutableList.of(putAsMutation1, deleteAsMutation2));
              })
          .doesNotThrowAnyException();
    } finally {
      if (storageWithDefaultNamespace != null) {
        storageWithDefaultNamespace.close();
      }
    }
  }

  @Test
  public void get_GetWithPartitionKeyAndClusteringKeyGiven_ShouldRetrieveSingleResult()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Get get = prepareGet(pKey, 0);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName1())).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get().contains(getColumnName4())).isTrue();
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(0);
  }

  @Test
  public void get_GetWithoutPartitionKeyGiven_ShouldThrowInvalidUsageException() {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .build();
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Get get =
        Get.newBuilder(prepareGet(pKey, cKey))
            .projections(getColumnName1(), getColumnName2(), getColumnName3(), getColumnName6())
            .build();
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual).isNotEmpty();
    assertThat(actual.get().getContainedColumnNames())
        .containsOnly(getColumnName1(), getColumnName2(), getColumnName3(), getColumnName6());
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get().getText(getColumnName2())).isEqualTo("0");
    assertThat(actual.get().getInt(getColumnName3())).isEqualTo(0);
    assertThat(actual.get().isNull(getColumnName6())).isTrue();
  }

  @Test
  public void get_GetWithMatchedConjunctionsGiven_ShouldRetrieveSingleResult()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 1;
    int cKey = 2;

    // Act
    Get get =
        Get.newBuilder(prepareGet(pKey, cKey))
            .where(ConditionBuilder.column(getColumnName2()).isEqualToText("3"))
            .and(ConditionBuilder.column(getColumnName3()).isEqualToInt(3))
            .build();
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(actual.get().getText(getColumnName2())).isEqualTo("3");
    assertThat(actual.get().getInt(getColumnName3())).isEqualTo(3);
  }

  @Test
  public void get_GetWithUnmatchedConjunctionsGiven_ShouldReturnEmpty() throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 1;
    int cKey = 2;

    // Act
    Get get =
        Get.newBuilder(prepareGet(pKey, cKey))
            .where(ConditionBuilder.column(getColumnName2()).isEqualToText("a"))
            .and(ConditionBuilder.column(getColumnName3()).isEqualToInt(3))
            .build();
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .projection(getColumnName1())
            .projection(getColumnName2())
            .projection(getColumnName3())
            .projection(getColumnName6())
            .build();
    List<Result> actual = scanAll(scan);

    // Assert
    actual.forEach(
        a -> {
          assertThat(a.getContainedColumnNames())
              .containsOnly(getColumnName1(), getColumnName2(), getColumnName3(), getColumnName6());
          assertThat(a.getInt(getColumnName1())).isEqualTo(0);
          assertThat(a.isNull(getColumnName6())).isTrue();
        });
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getText(getColumnName2())).isEqualTo("0");
    assertThat(actual.get(1).getText(getColumnName2())).isEqualTo("1");
    assertThat(actual.get(2).getText(getColumnName2())).isEqualTo("2");
    assertThat(actual.get(0).getInt(getColumnName3())).isEqualTo(0);
    assertThat(actual.get(1).getInt(getColumnName3())).isEqualTo(1);
    assertThat(actual.get(2).getInt(getColumnName3())).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    Scanner scanner = storage.scan(scan);

    // Assert
    List<Result> results = new ArrayList<>();

    Optional<Result> result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isFalse();

    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(results.get(2).contains(getColumnName1())).isTrue();
    assertThat(results.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(2).contains(getColumnName4())).isTrue();
    assertThat(results.get(2).getInt(getColumnName4())).isEqualTo(2);

    scanner.close();
  }

  @Test
  public void scan_ScanWithPartitionGivenThreeTimes_ShouldRetrieveResultsProperlyEveryTime()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    double t1 = System.currentTimeMillis();
    List<Result> actual = scanAll(scan);
    double t2 = System.currentTimeMillis();
    Scanner scanner = storage.scan(scan);
    scanner.close();
    double t3 = System.currentTimeMillis();
    scanner = storage.scan(scan);
    scanner.close();
    double t4 = System.currentTimeMillis();

    // Assert
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    System.err.println("first: " + (t2 - t1) + " (ms)");
    System.err.println("second: " + (t3 - t2) + " (ms)");
    System.err.println("third: " + (t4 - t3) + " (ms)");
  }

  @Test
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .start(Key.ofInt(getColumnName4(), 0))
            .end(Key.ofInt(getColumnName4(), 2), false)
            .build();
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(1);
  }

  @Test
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .start(Key.ofInt(getColumnName4(), 0), false)
            .end(Key.ofInt(getColumnName4(), 2))
            .build();
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .start(Key.ofInt(getColumnName4(), 0))
            .end(Key.ofInt(getColumnName4(), 2))
            .ordering(Scan.Ordering.asc(getColumnName4()))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(2).contains(getColumnName4())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithOrderDescGiven_ShouldReturnDescendingOrderedResults()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .start(Key.ofInt(getColumnName4(), 0))
            .end(Key.ofInt(getColumnName4(), 2))
            .ordering(Scan.Ordering.desc(getColumnName4()))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(2);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(2).contains(getColumnName4())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName4())).isEqualTo(0);
  }

  @Test
  public void scan_ScanWithLimitGiven_ShouldReturnGivenNumberOfResults()
      throws IOException, ExecutionException {
    // setup
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));

    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .ordering(Scan.Ordering.desc(getColumnName4()))
            .limit(1)
            .build();

    // exercise
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void
      scan_ScanWithClusteringKeyRangeAndConjunctionsGiven_ShouldRetrieveResultsOfBothConditions()
          throws IOException, ExecutionException {
    // Arrange
    populateRecords();

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .start(Key.ofInt(getColumnName4(), 0))
            .end(Key.ofInt(getColumnName4(), 2))
            .where(ConditionBuilder.column(getColumnName5()).isEqualToBoolean(true))
            .build();
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void scannerIterator_ScanWithPartitionKeyGiven_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(2).contains(getColumnName1())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(2).contains(getColumnName4())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void scannerIterator_OneAndIteratorCalled_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    Optional<Result> result = scanner.one();
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().contains(getColumnName1())).isTrue();
    assertThat(result.get().getInt(getColumnName1())).isEqualTo(0);
    assertThat(result.get().contains(getColumnName4())).isTrue();
    assertThat(result.get().getInt(getColumnName4())).isEqualTo(0);

    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void scannerIterator_AllAndIteratorCalled_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    List<Result> all = scanner.all();
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(all.size()).isEqualTo(3);
    assertThat(all.get(0).contains(getColumnName1())).isTrue();
    assertThat(all.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(all.get(0).contains(getColumnName4())).isTrue();
    assertThat(all.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(all.get(1).contains(getColumnName1())).isTrue();
    assertThat(all.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(all.get(1).contains(getColumnName4())).isTrue();
    assertThat(all.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(all.get(2).contains(getColumnName1())).isTrue();
    assertThat(all.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(all.get(2).contains(getColumnName4())).isTrue();
    assertThat(all.get(2).getInt(getColumnName4())).isEqualTo(2);

    assertThat(actual).isEmpty();
  }

  @Test
  public void scannerIterator_IteratorCalledMultipleTimes_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    actual.add(scanner.iterator().next());
    actual.add(scanner.iterator().next());
    actual.add(scanner.iterator().next());
    scanner.close();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(1);
    assertThat(actual.get(2).contains(getColumnName1())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(2).contains(getColumnName4())).isTrue();
    assertThat(actual.get(2).getInt(getColumnName4())).isEqualTo(2);
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();

    // Act
    storage.put(puts.get(pKey * 2 + cKey));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName1())).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get().contains(getColumnName2())).isTrue();
    assertThat(actual.get().getText(getColumnName2())).isEqualTo(Integer.toString(pKey + cKey));
    assertThat(actual.get().contains(getColumnName3())).isTrue();
    assertThat(actual.get().getInt(getColumnName3())).isEqualTo(pKey + cKey);
    assertThat(actual.get().contains(getColumnName4())).isTrue();
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(actual.get().contains(getColumnName5())).isTrue();
    assertThat(actual.get().getBoolean(getColumnName5())).isEqualTo(cKey % 2 == 0);
  }

  @Test
  public void put_SinglePutWithIfNotExistsGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.set(0, Put.newBuilder(puts.get(0)).condition(ConditionBuilder.putIfNotExists()).build());
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();

    // Act
    storage.put(puts.get(0));
    assertThatThrownBy(
            () ->
                storage.put(
                    Put.newBuilder(puts.get(0))
                        .intValue(getColumnName3(), Integer.MAX_VALUE)
                        .build()))
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName1())).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get().contains(getColumnName2())).isTrue();
    assertThat(actual.get().getText(getColumnName2())).isEqualTo(Integer.toString(pKey + cKey));
    assertThat(actual.get().contains(getColumnName3())).isTrue();
    assertThat(actual.get().getInt(getColumnName3())).isEqualTo(pKey + cKey);
    assertThat(actual.get().contains(getColumnName4())).isTrue();
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(actual.get().contains(getColumnName5())).isTrue();
    assertThat(actual.get().getBoolean(getColumnName5())).isEqualTo(cKey % 2 == 0);
  }

  @Test
  public void put_MultiplePutGiven_ShouldStoreProperly() throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(pKey + cKey);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).contains(getColumnName1())).isTrue();
    assertThat(results.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(2).contains(getColumnName4())).isTrue();
    assertThat(results.get(2).getInt(getColumnName4())).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.set(0, Put.newBuilder(puts.get(0)).condition(ConditionBuilder.putIfNotExists()).build());
    puts.set(1, Put.newBuilder(puts.get(1)).condition(ConditionBuilder.putIfNotExists()).build());
    puts.set(2, Put.newBuilder(puts.get(2)).condition(ConditionBuilder.putIfNotExists()).build());
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(pKey + cKey);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).contains(getColumnName1())).isTrue();
    assertThat(results.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(2).contains(getColumnName4())).isTrue();
    assertThat(results.get(2).getInt(getColumnName4())).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void put_PutWithoutValuesGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 0);
    Key clusteringKey = Key.ofInt(getColumnName4(), 0);

    // Act
    assertThatCode(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(getTableName())
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .build()))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .build());
    assertThat(result).isPresent();
  }

  @Test
  public void put_PutWithoutValuesGivenTwice_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 0);
    Key clusteringKey = Key.ofInt(getColumnName4(), 0);

    // Act
    assertThatCode(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(getTableName())
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .build()))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(getTableName())
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .build()))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .build());
    assertThat(result).isPresent();
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGivenWhenOneExists_ShouldThrowNoMutationException()
      throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    assertThatCode(() -> storage.put(puts.get(0))).doesNotThrowAnyException();
    puts.set(0, Put.newBuilder(puts.get(0)).condition(ConditionBuilder.putIfNotExists()).build());
    puts.set(1, Put.newBuilder(puts.get(1)).condition(ConditionBuilder.putIfNotExists()).build());
    puts.set(2, Put.newBuilder(puts.get(2)).condition(ConditionBuilder.putIfNotExists()).build());
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();

    // Act
    assertThatThrownBy(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .isInstanceOf(NoMutationException.class);

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(pKey + cKey);
  }

  @Test
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.put(puts.get(1));
    puts.set(0, Put.newBuilder(puts.get(0)).condition(ConditionBuilder.putIfNotExists()).build());
    puts.set(
        1,
        Put.newBuilder(puts.get(1))
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(getColumnName2()).isEqualToText("1"))
                    .build())
            .build());

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results =
        scanAll(
            Scan.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(Key.ofInt(getColumnName1(), 0))
                .build());
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(1);
  }

  @Test
  public void put_PutWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.set(0, Put.newBuilder(puts.get(0)).condition(ConditionBuilder.putIfExists()).build());
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    assertThatThrownBy(() -> storage.put(puts.get(0))).isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void put_PutWithIfExistsGivenWhenSuchRecordExists_ShouldUpdateRecord()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    assertThatCode(
            () ->
                storage.put(
                    Put.newBuilder(puts.get(0))
                        .intValue(getColumnName3(), Integer.MAX_VALUE)
                        .condition(ConditionBuilder.putIfExists())
                        .build()))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.contains(getColumnName1())).isTrue();
    assertThat(result.getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(result.contains(getColumnName4())).isTrue();
    assertThat(result.getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(result.contains(getColumnName3())).isTrue();
    assertThat(result.getInt(getColumnName3())).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void put_PutWithIfGivenWhenSuchRecordExists_ShouldUpdateRecord()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    assertThatCode(
            () ->
                storage.put(
                    Put.newBuilder(puts.get(0))
                        .intValue(getColumnName3(), Integer.MAX_VALUE)
                        .condition(
                            ConditionBuilder.putIf(
                                    ConditionBuilder.column(getColumnName3())
                                        .isEqualToInt(pKey + cKey))
                                .build())
                        .build()))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.contains(getColumnName1())).isTrue();
    assertThat(result.getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(result.contains(getColumnName4())).isTrue();
    assertThat(result.getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(result.contains(getColumnName3())).isTrue();
    assertThat(result.getInt(getColumnName3())).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void put_PutWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    assertThatThrownBy(
            () ->
                storage.put(
                    Put.newBuilder(puts.get(0))
                        .intValue(getColumnName3(), Integer.MAX_VALUE)
                        .condition(
                            ConditionBuilder.putIf(
                                    ConditionBuilder.column(getColumnName3())
                                        .isEqualToInt(pKey + cKey + 1))
                                .build())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.contains(getColumnName1())).isTrue();
    assertThat(result.getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(result.contains(getColumnName4())).isTrue();
    assertThat(result.getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(result.contains(getColumnName3())).isTrue();
    assertThat(result.getInt(getColumnName3())).isEqualTo(pKey + cKey);
  }

  @Test
  public void put_PutWithNullValue_ShouldPutProperly() throws ExecutionException {
    // Arrange
    Put put = preparePuts().get(0);
    storage.put(put);

    put =
        Put.newBuilder(put)
            .textValue(getColumnName2(), null)
            .booleanValue(getColumnName5(), null)
            .build();

    // Act
    storage.put(put);

    // Assert
    Optional<Result> actual = storage.get(prepareGet(0, 0));
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.contains(getColumnName1())).isTrue();
    assertThat(result.getInt(getColumnName1())).isEqualTo(0);
    assertThat(result.contains(getColumnName4())).isTrue();
    assertThat(result.getInt(getColumnName4())).isEqualTo(0);
    assertThat(result.contains(getColumnName2())).isTrue();
    assertThat(result.isNull(getColumnName2())).isTrue();
    assertThat(result.contains(getColumnName3())).isTrue();
    assertThat(result.getInt(getColumnName3())).isEqualTo(0);
    assertThat(result.contains(getColumnName5())).isTrue();
    assertThat(result.isNull(getColumnName5())).isTrue();

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    getColumnName1(),
                    getColumnName2(),
                    getColumnName3(),
                    getColumnName4(),
                    getColumnName5(),
                    getColumnName6())));

    assertThat(result.contains(getColumnName1())).isTrue();
    assertThat(result.isNull(getColumnName1())).isFalse();
    assertThat(result.getInt(getColumnName1())).isEqualTo(0);
    assertThat(result.getAsObject(getColumnName1())).isEqualTo(0);

    assertThat(result.contains(getColumnName4())).isTrue();
    assertThat(result.isNull(getColumnName4())).isFalse();
    assertThat(result.getInt(getColumnName4())).isEqualTo(0);
    assertThat(result.getAsObject(getColumnName4())).isEqualTo(0);

    assertThat(result.contains(getColumnName2())).isTrue();
    assertThat(result.isNull(getColumnName2())).isTrue();
    assertThat(result.getText(getColumnName2())).isNull();
    assertThat(result.getAsObject(getColumnName2())).isNull();

    assertThat(result.contains(getColumnName3())).isTrue();
    assertThat(result.isNull(getColumnName3())).isFalse();
    assertThat(result.getInt(getColumnName3())).isEqualTo(0);
    assertThat(result.getAsObject(getColumnName3())).isEqualTo(0);

    assertThat(result.contains(getColumnName5())).isTrue();
    assertThat(result.isNull(getColumnName5())).isTrue();
    assertThat(result.getBoolean(getColumnName5())).isFalse();
    assertThat(result.getAsObject(getColumnName5())).isNull();

    assertThat(result.contains(getColumnName6())).isTrue();
    assertThat(result.isNull(getColumnName6())).isTrue();
    assertThat(result.getBlob(getColumnName6())).isNull();
    assertThat(result.getAsObject(getColumnName6())).isNull();
  }

  @Test
  public void put_withPutIfIsNullWhenRecordDoesNotExist_shouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    Put putIf =
        Put.newBuilder(preparePuts().get(0))
            .intValue(getColumnName3(), 100)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(getColumnName3()).isNullInt())
                    .build())
            .enableImplicitPreRead()
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(putIf)).isInstanceOf(NoMutationException.class);

    Optional<Result> result = storage.get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    List<Result> results =
        scanAll(
            Scan.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .build());
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(cKey + 1);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(cKey + 2);
  }

  @Test
  public void put_PutWithLargeTextValue_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int payloadSizeBytes = 100 * 1024 * 1024; // 100 MiB
    char[] chars = new char[payloadSizeBytes];
    Arrays.fill(chars, 'a');
    String largeText = new String(chars);
    Put put = preparePuts().get(0);
    put = Put.newBuilder(put).textValue(getColumnName2(), largeText).build();

    // Act
    storage.put(put);

    // Assert
    Get get = prepareGet(0, 0);
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName2())).isTrue();
    assertThat(actual.get().getText(getColumnName2())).isEqualTo(largeText);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException() {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Delete delete = prepareDelete(pKey, Integer.MAX_VALUE);
    Delete deleteWithCondition =
        Delete.newBuilder(delete).condition(ConditionBuilder.deleteIfExists()).build();
    assertThatThrownBy(() -> storage.delete(deleteWithCondition))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    Delete deleteWithCondition =
        Delete.newBuilder(delete).condition(ConditionBuilder.deleteIfExists()).build();
    assertThatCode(() -> storage.delete(deleteWithCondition)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .build());
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    Delete deleteWithCondition =
        Delete.newBuilder(delete)
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(getColumnName2())
                            .isEqualToText(Integer.toString(Integer.MAX_VALUE)))
                    .build())
            .build();
    assertThatThrownBy(() -> storage.delete(deleteWithCondition))
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .build());
    assertThat(actual.isPresent()).isTrue();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    Delete deleteWithCondition =
        Delete.newBuilder(delete)
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(getColumnName2())
                            .isEqualToText(Integer.toString(pKey)))
                    .build())
            .build();
    assertThatCode(() -> storage.delete(deleteWithCondition)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .build());
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_MultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    List<Delete> deletes = prepareDeletes();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    deletes.set(
        0, Delete.newBuilder(deletes.get(0)).condition(ConditionBuilder.deleteIfExists()).build());
    deletes.set(
        1,
        Delete.newBuilder(deletes.get(1))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(getColumnName2()).isEqualToText("1"))
                    .build())
            .build());

    // Act
    assertThatCode(
            () -> storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results =
        scanAll(
            Scan.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(Key.ofInt(getColumnName1(), 0))
                .build());
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void delete_ForNonExistingRecord_ShouldDoNothing() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThatCode(() -> storage.delete(prepareDeletes().get(0))).doesNotThrowAnyException();

    Optional<Result> result = storage.get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void mutate_MultiplePutGiven_ShouldStoreProperly() throws ExecutionException, IOException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();

    // Act
    assertThatCode(() -> storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName4())).isTrue();
    assertThat(results.get(0).getInt(getColumnName4())).isEqualTo(pKey + cKey);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName4())).isTrue();
    assertThat(results.get(1).getInt(getColumnName4())).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).contains(getColumnName1())).isTrue();
    assertThat(results.get(2).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(2).contains(getColumnName4())).isTrue();
    assertThat(results.get(2).getInt(getColumnName4())).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldUpdateAndDeleteRecordsProperly()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    List<Put> puts = preparePuts();

    int pKey = 0;
    int cKey = 0;
    Delete delete = prepareDelete(pKey, cKey);

    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), pKey))
            .build();

    // Act
    assertThatCode(
            () ->
                storage.mutate(
                    Arrays.asList(
                        delete,
                        Put.newBuilder(puts.get(1))
                            .intValue(getColumnName3(), Integer.MAX_VALUE)
                            .build(),
                        Put.newBuilder(puts.get(2))
                            .intValue(getColumnName3(), Integer.MIN_VALUE)
                            .build())))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).contains(getColumnName1())).isTrue();
    assertThat(results.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(0).contains(getColumnName3())).isTrue();
    assertThat(results.get(0).getInt(getColumnName3())).isEqualTo(Integer.MAX_VALUE);
    assertThat(results.get(1).contains(getColumnName1())).isTrue();
    assertThat(results.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(results.get(1).contains(getColumnName3())).isTrue();
    assertThat(results.get(1).getInt(getColumnName3())).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();

    // Act
    storage.mutate(Collections.singletonList(puts.get(pKey * 2 + cKey)));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName1())).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(pKey);
    assertThat(actual.get().contains(getColumnName2())).isTrue();
    assertThat(actual.get().getText(getColumnName2())).isEqualTo(Integer.toString(pKey + cKey));
    assertThat(actual.get().contains(getColumnName3())).isTrue();
    assertThat(actual.get().getInt(getColumnName3())).isEqualTo(pKey + cKey);
    assertThat(actual.get().contains(getColumnName4())).isTrue();
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(cKey);
    assertThat(actual.get().contains(getColumnName5())).isTrue();
    assertThat(actual.get().getBoolean(getColumnName5())).isEqualTo(cKey % 2 == 0);
  }

  @Test
  public void
      mutate_SingleDeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
          throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    Delete delete =
        Delete.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();
    assertThatCode(() -> storage.mutate(Collections.singletonList(delete)))
        .doesNotThrowAnyException();

    // Assert
    List<Result> actual =
        scanAll(
            Scan.newBuilder()
                .namespace(namespace)
                .table(getTableName())
                .partitionKey(partitionKey)
                .build());
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(getColumnName1())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(0).contains(getColumnName4())).isTrue();
    assertThat(actual.get(0).getInt(getColumnName4())).isEqualTo(cKey + 1);
    assertThat(actual.get(1).contains(getColumnName1())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get(1).contains(getColumnName4())).isTrue();
    assertThat(actual.get(1).getInt(getColumnName4())).isEqualTo(cKey + 2);
  }

  @Test
  public void mutate_EmptyListGiven_ShouldThrowIllegalArgumentException() {
    // Act
    assertThatCode(() -> storage.mutate(new ArrayList<>()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_PutWithoutClusteringKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .build();

    // Act Assert
    assertThatCode(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_IncorrectPutGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .intValue(getColumnName4(), cKey)
            .build();

    // Act Assert
    assertThatCode(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_PutGivenForIndexedColumnWithNullValue_ShouldPut() throws ExecutionException {
    // Arrange
    storage.put(
        Put.newBuilder(preparePuts().get(0))
            .value(IntColumn.ofNull(getColumnName3()))
            .build()); // (0,0)
    Get get = prepareGet(0, 0);

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getColumns().get(getColumnName1()))
        .isEqualTo(IntColumn.of(getColumnName1(), 0));
    assertThat(actual.get().getColumns().get(getColumnName4()))
        .isEqualTo(IntColumn.of(getColumnName4(), 0));
    assertThat(actual.get().getColumns().get(getColumnName3()))
        .isEqualTo(IntColumn.ofNull(getColumnName3()));
  }

  @Test
  public void get_GetGivenForIndexedColumn_ShouldGet() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofInt(getColumnName3(), c3))
            .build();

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName1())).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get().contains(getColumnName4())).isTrue();
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForIndexedColumnWithMatchedConjunctions_ShouldGet()
      throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofInt(getColumnName3(), c3))
            .where(ConditionBuilder.column(getColumnName2()).isEqualToText("0"))
            .and(ConditionBuilder.column(getColumnName5()).isEqualToBoolean(true))
            .build();

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(getColumnName1())).isEqualTo(0);
    assertThat(actual.get().getInt(getColumnName4())).isEqualTo(0);
    assertThat(actual.get().getText(getColumnName2())).isEqualTo("0");
    assertThat(actual.get().getBoolean(getColumnName5())).isEqualTo(true);
  }

  @Test
  public void get_GetGivenForIndexedColumnWithUnmatchedConjunctions_ShouldReturnEmpty()
      throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofInt(getColumnName3(), c3))
            .where(ConditionBuilder.column(getColumnName2()).isEqualToText("a"))
            .and(ConditionBuilder.column(getColumnName5()).isEqualToBoolean(true))
            .build();

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    int c3 = 3;
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofInt(getColumnName3(), c3))
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofInt(getColumnName3(), c3))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    List<List<Integer>> expectedValues =
        new ArrayList<>(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 1), Arrays.asList(3, 0)));
    for (Result result : actual) {
      assertThat(result.contains(getColumnName1())).isTrue();
      assertThat(result.contains(getColumnName4())).isTrue();

      int col1Val = result.getInt(getColumnName1());
      int col4Val = result.getInt(getColumnName4());
      List<Integer> col1AndCol4 = Arrays.asList(col1Val, col4Val);
      assertThat(expectedValues).contains(col1AndCol4);
      expectedValues.remove(col1AndCol4);
    }
    assertThat(expectedValues).isEmpty();
  }

  @Test
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    String c2 = "test";
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .indexKey(Key.ofText(getColumnName2(), c2))
            .build();

    // Act Assert
    assertThatThrownBy(() -> scanAll(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    int recordCount = 345;

    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 1);
    for (int i = 0; i < recordCount; i++) {
      Key clusteringKey = Key.ofInt(getColumnName4(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }

    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .build();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(recordCount);
    for (int i = 0; i < recordCount; i++) {
      assertThat(results.get(i).getInt(getColumnName1())).isEqualTo(1);
      assertThat(results.get(i).getInt(getColumnName4())).isEqualTo(i);
    }
  }

  @Test
  public void scan_ScanLargeDataWithOrdering_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    int recordCount = 345;
    int fetchCount = 234;

    // Arrange
    Key partitionKey = Key.ofInt(getColumnName1(), 1);
    for (int i = 0; i < recordCount; i++) {
      Key clusteringKey = Key.ofInt(getColumnName4(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }

    Scan scanAsc =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .ordering(Scan.Ordering.asc(getColumnName4()))
            .build();
    Scan scanDesc =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .ordering(Scan.Ordering.desc(getColumnName4()))
            .build();

    // Act
    List<Result> resultsAsc = new ArrayList<>();
    try (Scanner scanner = storage.scan(scanAsc)) {
      Iterator<Result> iterator = scanner.iterator();
      for (int i = 0; i < fetchCount; i++) {
        resultsAsc.add(iterator.next());
      }
    }

    List<Result> resultsDesc = new ArrayList<>();
    try (Scanner scanner = storage.scan(scanDesc)) {
      Iterator<Result> iterator = scanner.iterator();
      for (int i = 0; i < fetchCount; i++) {
        resultsDesc.add(iterator.next());
      }
    }

    // Assert
    assertThat(resultsAsc.size()).isEqualTo(fetchCount);
    for (int i = 0; i < fetchCount; i++) {
      assertThat(resultsAsc.get(i).getInt(getColumnName1())).isEqualTo(1);
      assertThat(resultsAsc.get(i).getInt(getColumnName4())).isEqualTo(i);
    }

    assertThat(resultsDesc.size()).isEqualTo(fetchCount);
    for (int i = 0; i < fetchCount; i++) {
      assertThat(resultsDesc.get(i).getInt(getColumnName1())).isEqualTo(1);
      assertThat(resultsDesc.get(i).getInt(getColumnName4())).isEqualTo(recordCount - i - 1);
    }
  }

  @Test
  public void scan_ScanLargeDataWithLimit_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    int recordCount = 345;
    int limit = 234;

    Key partitionKey = Key.ofInt(getColumnName1(), 1);
    for (int i = 0; i < recordCount; i++) {
      Key clusteringKey = Key.ofInt(getColumnName4(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(partitionKey)
            .limit(limit)
            .build();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(limit);
    for (int i = 0; i < limit; i++) {
      assertThat(results.get(i).getInt(getColumnName1())).isEqualTo(1);
      assertThat(results.get(i).getInt(getColumnName4())).isEqualTo(i);
    }
  }

  @Test
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    Scan scanAll = Scan.newBuilder().namespace(namespace).table(getTableName()).all().build();

    // Act
    List<Result> results = scanAll(scanAll);

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(getColumnName1(), i))
                                    .column(IntColumn.of(getColumnName4(), j))
                                    .column(
                                        TextColumn.of(getColumnName2(), Integer.toString(i + j)))
                                    .column(IntColumn.of(getColumnName3(), i + j))
                                    .column(BooleanColumn.of(getColumnName5(), j % 2 == 0))
                                    .column(BlobColumn.ofNull(getColumnName6()))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords()
      throws ExecutionException, IOException {
    // Arrange
    Put p1 =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 1))
            .clusteringKey(Key.ofInt(getColumnName4(), 1))
            .build();
    Put p2 =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 1))
            .clusteringKey(Key.ofInt(getColumnName4(), 2))
            .build();
    Put p3 =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 2))
            .clusteringKey(Key.ofInt(getColumnName4(), 1))
            .build();
    Put p4 =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 3))
            .clusteringKey(Key.ofInt(getColumnName4(), 0))
            .build();
    storage.put(ImmutableList.of(p1, p2));
    storage.put(p3);
    storage.put(p4);
    Scan scanAll =
        Scan.newBuilder().namespace(namespace).table(getTableName()).all().limit(2).build();

    // Act
    List<Result> results = scanAll(scanAll);

    // Assert
    TestUtils.assertResultsAreASubsetOf(
        results,
        ImmutableList.of(
            new ExpectedResultBuilder()
                .column(IntColumn.of(getColumnName1(), 1))
                .column(IntColumn.of(getColumnName4(), 1))
                .column(TextColumn.ofNull(getColumnName2()))
                .column(IntColumn.ofNull(getColumnName3()))
                .column(BooleanColumn.ofNull(getColumnName5()))
                .column(BlobColumn.ofNull(getColumnName6()))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(getColumnName1(), 1))
                .column(IntColumn.of(getColumnName4(), 2))
                .column(TextColumn.ofNull(getColumnName2()))
                .column(IntColumn.ofNull(getColumnName3()))
                .column(BooleanColumn.ofNull(getColumnName5()))
                .column(BlobColumn.ofNull(getColumnName6()))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(getColumnName1(), 2))
                .column(IntColumn.of(getColumnName4(), 1))
                .column(TextColumn.ofNull(getColumnName2()))
                .column(IntColumn.ofNull(getColumnName3()))
                .column(BooleanColumn.ofNull(getColumnName5()))
                .column(BlobColumn.ofNull(getColumnName6()))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(getColumnName1(), 3))
                .column(IntColumn.of(getColumnName4(), 0))
                .column(TextColumn.ofNull(getColumnName2()))
                .column(IntColumn.ofNull(getColumnName3()))
                .column(BooleanColumn.ofNull(getColumnName5()))
                .column(BlobColumn.ofNull(getColumnName6()))
                .build()));
    assertThat(results).hasSize(2);
  }

  @Test
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();

    // Act
    Scan scanAll =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .all()
            .projections(getColumnName1(), getColumnName2(), getColumnName3(), getColumnName6())
            .build();
    List<Result> actualResults = scanAll(scanAll);

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(getColumnName1(), i))
                                    .column(
                                        TextColumn.of(getColumnName2(), Integer.toString(i + j)))
                                    .column(IntColumn.of(getColumnName3(), i + j))
                                    .column(BlobColumn.ofNull(getColumnName6()))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(actualResults, expectedResults);
    actualResults.forEach(
        actualResult ->
            assertThat(actualResult.getContainedColumnNames())
                .containsOnly(
                    getColumnName1(), getColumnName2(), getColumnName3(), getColumnName6()));
  }

  @Test
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    for (int i = 0; i < 345; i++) {
      Key partitionKey = Key.ofInt(getColumnName1(), i % 4);
      Key clusteringKey = Key.ofInt(getColumnName4(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }
    Scan scan = Scan.newBuilder().namespace(namespace).table(getTableName()).all().build();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    for (int i = 0; i < 345; i++) {
      expectedResults.add(
          new ExpectedResultBuilder()
              .column(IntColumn.of(getColumnName1(), i % 4))
              .column(IntColumn.of(getColumnName4(), i))
              .column(TextColumn.ofNull(getColumnName2()))
              .column(IntColumn.ofNull(getColumnName3()))
              .column(BooleanColumn.ofNull(getColumnName5()))
              .column(BlobColumn.of(getColumnName6(), new byte[getLargeDataSizeInBytes()]))
              .build());
    }
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void get_GetWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .clusteringKey(Key.ofInt(getColumnName4(), 0))
            .textValue(getColumnName2(), "foo")
            .intValue(getColumnName3(), 0)
            .booleanValue(getColumnName5(), true)
            .build();
    storage.put(put);

    // Act
    Get get =
        Get.newBuilder(prepareGet(0, 0))
            .projection(getColumnName3())
            .projection(getColumnName5())
            .build();
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getContainedColumnNames()).containsOnly(getColumnName3(), getColumnName5());
    assertThat(result.getInt(getColumnName3())).isEqualTo(0);
    assertThat(result.getBoolean(getColumnName5())).isTrue();
  }

  @Test
  public void scan_ScanWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException, IOException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .clusteringKey(Key.ofInt(getColumnName4(), 0))
            .textValue(getColumnName2(), "foo")
            .intValue(getColumnName3(), 0)
            .booleanValue(getColumnName5(), true)
            .build();
    storage.put(put);

    // Act
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .projection(getColumnName3())
            .projection(getColumnName5())
            .build();
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getContainedColumnNames())
        .containsOnly(getColumnName3(), getColumnName5());
    assertThat(results.get(0).getInt(getColumnName3())).isEqualTo(0);
    assertThat(results.get(0).getBoolean(getColumnName5())).isTrue();
  }

  @Test
  public void scan_ScanAllWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException, IOException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .partitionKey(Key.ofInt(getColumnName1(), 0))
            .clusteringKey(Key.ofInt(getColumnName4(), 0))
            .textValue(getColumnName2(), "foo")
            .intValue(getColumnName3(), 0)
            .booleanValue(getColumnName5(), true)
            .build();
    storage.put(put);

    // Act
    Scan scanAll =
        Scan.newBuilder()
            .namespace(namespace)
            .table(getTableName())
            .all()
            .projection(getColumnName3())
            .projection(getColumnName5())
            .build();
    List<Result> results = scanAll(scanAll);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getContainedColumnNames())
        .containsOnly(getColumnName3(), getColumnName5());
    assertThat(results.get(0).getInt(getColumnName3())).isEqualTo(0);
    assertThat(results.get(0).getBoolean(getColumnName5())).isTrue();
  }

  @Test
  public void scan_ScanAllLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    int recordCount = 345;

    // Arrange
    Key clusteringKey = Key.ofInt(getColumnName4(), 1);
    for (int i = 0; i < recordCount; i++) {
      Key partitionKey = Key.ofInt(getColumnName1(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }

    Scan scanAll = Scan.newBuilder().namespace(namespace).table(getTableName()).all().build();

    // Act
    List<Result> results = scanAll(scanAll);

    // Assert
    assertThat(results.size()).isEqualTo(recordCount);

    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < recordCount; i++) {
      partitionKeys.add(results.get(i).getInt(getColumnName1()));
      assertThat(results.get(i).getInt(getColumnName4())).isEqualTo(1);
    }
    assertThat(partitionKeys.size()).isEqualTo(recordCount);
    assertThat(partitionKeys).allMatch(i -> i >= 0 && i < recordCount);
  }

  @Test
  public void scan_ScanAllLargeDataWithLimit_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    int recordCount = 345;
    int limit = 234;

    Key clusteringKey = Key.ofInt(getColumnName4(), 1);
    for (int i = 0; i < recordCount; i++) {
      Key partitionKey = Key.ofInt(getColumnName1(), i);
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(getTableName())
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .blobValue(getColumnName6(), new byte[getLargeDataSizeInBytes()])
              .build());
    }

    Scan scan =
        Scan.newBuilder().namespace(namespace).table(getTableName()).all().limit(limit).build();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(limit);

    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < limit; i++) {
      partitionKeys.add(results.get(i).getInt(getColumnName1()));
      assertThat(results.get(i).getInt(getColumnName4())).isEqualTo(1);
    }
    assertThat(partitionKeys.size()).isEqualTo(limit);
    assertThat(partitionKeys).allMatch(i -> i >= 0 && i < recordCount);
  }

  private void populateRecords() {
    List<Put> puts = preparePuts();
    puts.forEach(p -> assertThatCode(() -> storage.put(p)).doesNotThrowAnyException());
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    return Get.newBuilder()
        .namespace(namespace)
        .table(getTableName())
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = Key.ofInt(getColumnName1(), i);
                          Key clusteringKey = Key.ofInt(getColumnName4(), j);
                          Put put =
                              Put.newBuilder()
                                  .namespace(namespace)
                                  .table(getTableName())
                                  .partitionKey(partitionKey)
                                  .clusteringKey(clusteringKey)
                                  .textValue(getColumnName2(), Integer.toString(i + j))
                                  .intValue(getColumnName3(), i + j)
                                  .booleanValue(getColumnName5(), j % 2 == 0)
                                  .build();
                          puts.add(put);
                        }));

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = Key.ofInt(getColumnName1(), pKey);
    Key clusteringKey = Key.ofInt(getColumnName4(), cKey);
    return Delete.newBuilder()
        .namespace(namespace)
        .table(getTableName())
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = Key.ofInt(getColumnName1(), i);
                          Key clusteringKey = Key.ofInt(getColumnName4(), j);
                          Delete delete =
                              Delete.newBuilder()
                                  .namespace(namespace)
                                  .table(getTableName())
                                  .partitionKey(partitionKey)
                                  .clusteringKey(clusteringKey)
                                  .build();
                          deletes.add(delete);
                        }));

    return deletes;
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }
}
