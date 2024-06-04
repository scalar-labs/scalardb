package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
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

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    createTable();
    storage = factory.getStorage();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addColumn(COL_NAME6, DataType.BLOB)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addSecondaryIndex(COL_NAME3)
            .build(),
        true,
        options);
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
    storage.with(namespace, TABLE);
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace, TABLE);
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
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  @Test
  public void operation_NoTargetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(null, TABLE);
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_WrongNamespaceGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with("wrong_" + namespace, TABLE); // a wrong namespace
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_WrongTableGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(namespace, "wrong_" + TABLE); // a wrong table
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_DefaultNamespaceGiven_ShouldWorkProperly() {
    Properties properties = getProperties(TEST_NAME);
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, getNamespace());
    final DistributedStorage storageWithDefaultNamespace =
        StorageFactory.create(properties).getStorage();
    try {
      // Arrange
      populateRecords();
      Get get =
          Get.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 0))
              .build();
      Scan scan = Scan.newBuilder().table(TABLE).partitionKey(Key.ofInt(COL_NAME1, 0)).build();
      Put put =
          Put.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .clusteringKey(Key.ofInt(COL_NAME4, 0))
              .textValue(COL_NAME2, "foo")
              .build();
      Delete delete =
          Delete.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, 2))
              .clusteringKey(Key.ofInt(COL_NAME4, 0))
              .build();
      Mutation putAsMutation1 =
          Put.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, 3))
              .clusteringKey(Key.ofInt(COL_NAME4, 0))
              .textValue(COL_NAME2, "foo")
              .build();
      Mutation deleteAsMutation2 =
          Delete.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(COL_NAME1, 3))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
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
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void get_GetWithoutPartitionKeyGiven_ShouldThrowInvalidUsageException() {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Key partitionKey = new Key(COL_NAME1, pKey);
    Get get = new Get(partitionKey);
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
    Get get = prepareGet(pKey, cKey);
    get.withProjections(Arrays.asList(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME6));
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual).isNotEmpty();
    assertThat(actual.get().getContainedColumnNames())
        .containsOnly(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME6);
    assertThat(actual.get().getInt(COL_NAME1)).isEqualTo(0);
    assertThat(actual.get().getText(COL_NAME2)).isEqualTo("0");
    assertThat(actual.get().getInt(COL_NAME3)).isEqualTo(0);
    assertThat(actual.get().isNull(COL_NAME6)).isTrue();
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
            .where(ConditionBuilder.column(COL_NAME2).isEqualToText("3"))
            .and(ConditionBuilder.column(COL_NAME3).isEqualToInt(3))
            .build();
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(COL_NAME1)).isEqualTo(pKey);
    assertThat(actual.get().getInt(COL_NAME4)).isEqualTo(cKey);
    assertThat(actual.get().getText(COL_NAME2)).isEqualTo("3");
    assertThat(actual.get().getInt(COL_NAME3)).isEqualTo(3);
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
            .where(ConditionBuilder.column(COL_NAME2).isEqualToText("a"))
            .and(ConditionBuilder.column(COL_NAME3).isEqualToInt(3))
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
        new Scan(Key.ofInt(COL_NAME1, pKey))
            .withProjection(COL_NAME1)
            .withProjection(COL_NAME2)
            .withProjection(COL_NAME3)
            .withProjection(COL_NAME6);
    List<Result> actual = scanAll(scan);

    // Assert
    actual.forEach(
        a -> {
          assertThat(a.getContainedColumnNames())
              .containsOnly(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME6);
          assertThat(a.getInt(COL_NAME1)).isEqualTo(0);
          assertThat(a.isNull(COL_NAME6)).isTrue();
        });
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getText(COL_NAME2)).isEqualTo("0");
    assertThat(actual.get(1).getText(COL_NAME2)).isEqualTo("1");
    assertThat(actual.get(2).getText(COL_NAME2)).isEqualTo("2");
    assertThat(actual.get(0).getInt(COL_NAME3)).isEqualTo(0);
    assertThat(actual.get(1).getInt(COL_NAME3)).isEqualTo(1);
    assertThat(actual.get(2).getInt(COL_NAME3)).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
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
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    scanner.close();
  }

  @Test
  public void scan_ScanWithPartitionGivenThreeTimes_ShouldRetrieveResultsProperlyEveryTime()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
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
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
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
        new Scan(new Key(COL_NAME1, pKey))
            .withStart(new Key(COL_NAME4, 0), true)
            .withEnd(new Key(COL_NAME4, 2), false);
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
  }

  @Test
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(COL_NAME1, pKey))
            .withStart(new Key(COL_NAME4, 0), false)
            .withEnd(new Key(COL_NAME4, 2), true);
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(COL_NAME1, 0))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.ASC));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithOrderDescGiven_ShouldReturnDescendingOrderedResults()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(COL_NAME1, 0))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void scan_ScanWithLimitGiven_ShouldReturnGivenNumberOfResults()
      throws IOException, ExecutionException {
    // setup
    List<Put> puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));

    Scan scan =
        new Scan(new Key(COL_NAME1, 0))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC))
            .withLimit(1);

    // exercise
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
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
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .start(Key.ofInt(COL_NAME4, 0))
            .end(Key.ofInt(COL_NAME4, 2))
            .where(ConditionBuilder.column(COL_NAME5).isEqualToBoolean(true))
            .build();
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).contains(COL_NAME1)).isTrue();
    assertThat(actual.get(0).getInt(COL_NAME1)).isEqualTo(0);
    assertThat(actual.get(0).contains(COL_NAME4)).isTrue();
    assertThat(actual.get(0).getInt(COL_NAME4)).isEqualTo(0);
    assertThat(actual.get(1).contains(COL_NAME1)).isTrue();
    assertThat(actual.get(1).getInt(COL_NAME1)).isEqualTo(0);
    assertThat(actual.get(1).contains(COL_NAME4)).isTrue();
    assertThat(actual.get(1).getInt(COL_NAME4)).isEqualTo(2);
  }

  @Test
  public void scannerIterator_ScanWithPartitionKeyGiven_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(actual.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void scannerIterator_OneAndIteratorCalled_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    Optional<Result> result = scanner.one();
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(result.get().getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);

    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void scannerIterator_AllAndIteratorCalled_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    List<Result> all = scanner.all();
    scanner.forEach(actual::add);
    scanner.close();

    // Assert
    assertThat(all.size()).isEqualTo(3);
    assertThat(all.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(all.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(all.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(all.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(all.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(all.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(all.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(all.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(all.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(all.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(all.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(all.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    assertThat(actual).isEmpty();
  }

  @Test
  public void scannerIterator_IteratorCalledMultipleTimes_ShouldRetrieveCorrectResults()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
    List<Result> actual = new ArrayList<>();
    Scanner scanner = storage.scan(scan);
    actual.add(scanner.iterator().next());
    actual.add(scanner.iterator().next());
    actual.add(scanner.iterator().next());
    scanner.close();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(actual.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(pKey * 2 + cKey));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
  }

  @Test
  public void put_SinglePutWithIfNotExistsGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(0));
    puts.get(0).withValue(COL_NAME3, Integer.MAX_VALUE);
    assertThatThrownBy(() -> storage.put(puts.get(0))).isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
  }

  @Test
  public void put_MultiplePutGiven_ShouldStoreProperly() throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void put_PutWithoutValuesGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);

    // Act
    assertThatCode(() -> storage.put(new Put(partitionKey, clusteringKey)))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> result = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(result).isPresent();
  }

  @Test
  public void put_PutWithoutValuesGivenTwice_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);

    // Act
    assertThatCode(() -> storage.put(new Put(partitionKey, clusteringKey)))
        .doesNotThrowAnyException();
    assertThatCode(() -> storage.put(new Put(partitionKey, clusteringKey)))
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> result = storage.get(new Get(partitionKey, clusteringKey));
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
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatThrownBy(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .isInstanceOf(NoMutationException.class);

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
  }

  @Test
  public void
      put_MultiplePutWithDifferentPartitionsWithIfNotExistsGiven_ShouldThrowIllegalArgumentException()
          throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(3).withCondition(new PutIfNotExists());
    puts.get(6).withCondition(new PutIfNotExists());

    // Act
    assertThatThrownBy(() -> storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6))))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 3)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 6)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentPartitionsGiven_ShouldThrowIllegalArgumentException()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();

    // Act
    assertThatThrownBy(() -> storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6))))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 3)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 6)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.put(puts.get(1));
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
  }

  @Test
  public void put_PutWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    puts.get(0).withCondition(new PutIfExists());
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
    puts.get(0).withCondition(new PutIfExists());
    puts.get(0).withValue(COL_NAME3, Integer.MAX_VALUE);
    assertThatCode(() -> storage.put(puts.get(0))).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
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
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME3, new IntValue(pKey + cKey), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(COL_NAME3, Integer.MAX_VALUE);
    assertThatCode(() -> storage.put(puts.get(0))).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
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
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME3, new IntValue(pKey + cKey + 1), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(COL_NAME3, Integer.MAX_VALUE);
    assertThatThrownBy(() -> storage.put(puts.get(0))).isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
  }

  @Test
  public void put_PutWithNullValue_ShouldPutProperly() throws ExecutionException {
    // Arrange
    Put put = preparePuts().get(0);
    storage.put(put);

    put.withTextValue(COL_NAME2, null);
    put.withBooleanValue(COL_NAME5, null);

    // Act
    storage.put(put);

    // Assert
    Optional<Result> actual = storage.get(prepareGet(0, 0));
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(result.getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, (String) null)));
    assertThat(result.getValue(COL_NAME3)).isEqualTo(Optional.of(new IntValue(COL_NAME3, 0)));
    assertThat(result.getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, false)));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME4, COL_NAME5, COL_NAME6)));

    assertThat(result.contains(COL_NAME1)).isTrue();
    assertThat(result.isNull(COL_NAME1)).isFalse();
    assertThat(result.getInt(COL_NAME1)).isEqualTo(0);
    assertThat(result.getAsObject(COL_NAME1)).isEqualTo(0);

    assertThat(result.contains(COL_NAME4)).isTrue();
    assertThat(result.isNull(COL_NAME4)).isFalse();
    assertThat(result.getInt(COL_NAME4)).isEqualTo(0);
    assertThat(result.getAsObject(COL_NAME4)).isEqualTo(0);

    assertThat(result.contains(COL_NAME2)).isTrue();
    assertThat(result.isNull(COL_NAME2)).isTrue();
    assertThat(result.getText(COL_NAME2)).isNull();
    assertThat(result.getAsObject(COL_NAME2)).isNull();

    assertThat(result.contains(COL_NAME3)).isTrue();
    assertThat(result.isNull(COL_NAME3)).isFalse();
    assertThat(result.getInt(COL_NAME3)).isEqualTo(0);
    assertThat(result.getAsObject(COL_NAME3)).isEqualTo(0);

    assertThat(result.contains(COL_NAME5)).isTrue();
    assertThat(result.isNull(COL_NAME5)).isTrue();
    assertThat(result.getBoolean(COL_NAME5)).isFalse();
    assertThat(result.getAsObject(COL_NAME5)).isNull();

    assertThat(result.contains(COL_NAME6)).isTrue();
    assertThat(result.isNull(COL_NAME6)).isTrue();
    assertThat(result.getBlob(COL_NAME6)).isNull();
    assertThat(result.getAsObject(COL_NAME6)).isNull();
  }

  @Test
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(partitionKey));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(cKey + 1);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(cKey + 2);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException() {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Delete delete = prepareDelete(pKey, Integer.MAX_VALUE);
    delete.withCondition(new DeleteIfExists());
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(new DeleteIfExists());
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2,
                new TextValue(Integer.toString(Integer.MAX_VALUE)),
                ConditionalExpression.Operator.EQ)));
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isTrue();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2,
                new TextValue(Integer.toString(pKey)),
                ConditionalExpression.Operator.EQ)));
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_MultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    List<Delete> deletes = prepareDeletes();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    deletes.get(0).withCondition(new DeleteIfExists());
    deletes
        .get(1)
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(
            () -> storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_MultiplePutGiven_ShouldStoreProperly() throws ExecutionException, IOException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void mutate_MultiplePutWithDifferentPartitionsGiven_ShouldThrowIllegalArgumentException()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();

    // Act
    assertThatCode(() -> storage.mutate(Arrays.asList(puts.get(0), puts.get(3), puts.get(6))))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 3)));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(COL_NAME1, 6)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldUpdateAndDeleteRecordsProperly()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    List<Put> puts = preparePuts();
    puts.get(1).withValue(COL_NAME3, Integer.MAX_VALUE);
    puts.get(2).withValue(COL_NAME3, Integer.MIN_VALUE);

    int pKey = 0;
    int cKey = 0;
    Delete delete = prepareDelete(pKey, cKey);

    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.mutate(Arrays.asList(delete, puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(COL_NAME3).isPresent()).isTrue();
    assertThat(results.get(0).getValue(COL_NAME3).get().getAsInt()).isEqualTo(Integer.MAX_VALUE);
    assertThat(results.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME3).isPresent()).isTrue();
    assertThat(results.get(1).getValue(COL_NAME3).get().getAsInt()).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.mutate(Collections.singletonList(puts.get(pKey * 2 + cKey)));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
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
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    Delete delete = new Delete(partitionKey, clusteringKey);
    assertThatCode(() -> storage.mutate(Collections.singletonList(delete)))
        .doesNotThrowAnyException();

    // Assert
    List<Result> actual = scanAll(new Scan(partitionKey));
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(0).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(cKey + 1);
    assertThat(actual.get(1).getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(0);
    assertThat(actual.get(1).getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(cKey + 2);
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
    Key partitionKey = new Key(COL_NAME1, pKey);
    Put put = new Put(partitionKey);

    // Act Assert
    assertThatCode(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_IncorrectPutGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);
    Put put = new Put(partitionKey).withValue(COL_NAME4, cKey);

    // Act Assert
    assertThatCode(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_PutGivenForIndexedColumnWithNullValue_ShouldPut() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0).withValue(IntColumn.ofNull(COL_NAME3))); // (0,0)
    Get get = new Get(prepareGet(0, 0));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getColumns().get(COL_NAME1)).isEqualTo(IntColumn.of(COL_NAME1, 0));
    assertThat(actual.get().getColumns().get(COL_NAME4)).isEqualTo(IntColumn.of(COL_NAME4, 0));
    assertThat(actual.get().getColumns().get(COL_NAME3)).isEqualTo(IntColumn.ofNull(COL_NAME3));
  }

  @Test
  public void get_GetGivenForIndexedColumn_ShouldGet() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get getBuiltByConstructor = new Get(new Key(COL_NAME3, c3));
    Get getBuiltByBuilder =
        Get.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .indexKey(Key.ofInt(COL_NAME3, c3))
            .build();

    // Act
    Optional<Result> actual1 = storage.get(getBuiltByConstructor);
    Optional<Result> actual2 = storage.get(getBuiltByBuilder);

    // Assert
    assertThat(actual1.isPresent()).isTrue();
    assertThat(actual1.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(actual1.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));

    assertThat(actual2).isEqualTo(actual1);
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
            .table(TABLE)
            .indexKey(Key.ofInt(COL_NAME3, c3))
            .where(ConditionBuilder.column(COL_NAME2).isEqualToText("0"))
            .and(ConditionBuilder.column(COL_NAME5).isEqualToBoolean(true))
            .build();

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(COL_NAME1)).isEqualTo(0);
    assertThat(actual.get().getInt(COL_NAME4)).isEqualTo(0);
    assertThat(actual.get().getText(COL_NAME2)).isEqualTo("0");
    assertThat(actual.get().getBoolean(COL_NAME5)).isEqualTo(true);
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
            .table(TABLE)
            .indexKey(Key.ofInt(COL_NAME3, c3))
            .where(ConditionBuilder.column(COL_NAME2).isEqualToText("a"))
            .and(ConditionBuilder.column(COL_NAME5).isEqualToBoolean(true))
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
    Get get = new Get(new Key(COL_NAME3, c3));

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scanBuiltByConstructor = new Scan(new Key(COL_NAME3, c3));
    Scan scanBuiltByBuilder =
        Scan.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .indexKey(Key.ofInt(COL_NAME3, c3))
            .build();

    // Act
    List<Result> actual1 = scanAll(scanBuiltByConstructor);
    List<Result> actual2 = scanAll(scanBuiltByBuilder);

    // Assert
    assertThat(actual1.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    List<List<Integer>> expectedValues =
        new ArrayList<>(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 1), Arrays.asList(3, 0)));
    for (Result result : actual1) {
      assertThat(result.getValue(COL_NAME1).isPresent()).isTrue();
      assertThat(result.getValue(COL_NAME4).isPresent()).isTrue();

      int col1Val = result.getValue(COL_NAME1).get().getAsInt();
      int col4Val = result.getValue(COL_NAME4).get().getAsInt();
      List<Integer> col1AndCol4 = Arrays.asList(col1Val, col4Val);
      assertThat(expectedValues).contains(col1AndCol4);
      expectedValues.remove(col1AndCol4);
    }
    assertThat(expectedValues).isEmpty();

    assertThat(actual2).isEqualTo(actual1);
  }

  @Test
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    String c2 = "test";
    Scan scan = new Scan(new Key(COL_NAME2, c2));

    // Act Assert
    assertThatThrownBy(() -> scanAll(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 1);
    for (int i = 0; i < 345; i++) {
      Key clusteringKey = new Key(COL_NAME4, i);
      storage.put(
          new Put(partitionKey, clusteringKey)
              .withBlobValue(COL_NAME6, new byte[getLargeDataSizeInBytes()]));
    }
    Scan scan = new Scan(partitionKey);

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(345);
    for (int i = 0; i < 345; i++) {
      assertThat(results.get(i).getValue(COL_NAME1).isPresent()).isTrue();
      assertThat(results.get(i).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
      assertThat(results.get(i).getValue(COL_NAME4).isPresent()).isTrue();
      assertThat(results.get(i).getValue(COL_NAME4).get().getAsInt()).isEqualTo(i);
    }
  }

  @Test
  public void scan_ScanLargeDataWithOrdering_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = new Key(COL_NAME1, 1);
    for (int i = 0; i < 345; i++) {
      Key clusteringKey = new Key(COL_NAME4, i);
      storage.put(
          new Put(partitionKey, clusteringKey)
              .withBlobValue(COL_NAME6, new byte[getLargeDataSizeInBytes()]));
    }
    Scan scan = new Scan(partitionKey).withOrdering(new Ordering(COL_NAME4, Order.ASC));

    // Act
    List<Result> results = new ArrayList<>();
    try (Scanner scanner = storage.scan(scan)) {
      Iterator<Result> iterator = scanner.iterator();
      for (int i = 0; i < 234; i++) {
        results.add(iterator.next());
      }
    }

    // Assert
    assertThat(results.size()).isEqualTo(234);
    for (int i = 0; i < 234; i++) {
      assertThat(results.get(i).getInt(COL_NAME1)).isEqualTo(1);
      assertThat(results.get(i).getInt(COL_NAME4)).isEqualTo(i);
    }
  }

  @Test
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    ScanAll scanAll = new ScanAll();

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
                                    .column(IntColumn.of(COL_NAME1, i))
                                    .column(IntColumn.of(COL_NAME4, j))
                                    .column(TextColumn.of(COL_NAME2, Integer.toString(i + j)))
                                    .column(IntColumn.of(COL_NAME3, i + j))
                                    .column(BooleanColumn.of(COL_NAME5, j % 2 == 0))
                                    .column(BlobColumn.ofNull(COL_NAME6))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords()
      throws ExecutionException, IOException {
    // Arrange
    Put p1 = new Put(Key.ofInt(COL_NAME1, 1), Key.ofInt(COL_NAME4, 1));
    Put p2 = new Put(Key.ofInt(COL_NAME1, 1), Key.ofInt(COL_NAME4, 2));
    Put p3 = new Put(Key.ofInt(COL_NAME1, 2), Key.ofInt(COL_NAME4, 1));
    Put p4 = new Put(Key.ofInt(COL_NAME1, 3), Key.ofInt(COL_NAME4, 0));
    storage.put(ImmutableList.of(p1, p2));
    storage.put(p3);
    storage.put(p4);
    ScanAll scanAll = new ScanAll().withLimit(2);

    // Act
    List<Result> results = scanAll(scanAll);

    // Assert
    TestUtils.assertResultsAreASubsetOf(
        results,
        ImmutableList.of(
            new ExpectedResultBuilder()
                .column(IntColumn.of(COL_NAME1, 1))
                .column(IntColumn.of(COL_NAME4, 1))
                .column(TextColumn.ofNull(COL_NAME2))
                .column(IntColumn.ofNull(COL_NAME3))
                .column(BooleanColumn.ofNull(COL_NAME5))
                .column(BlobColumn.ofNull(COL_NAME6))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(COL_NAME1, 1))
                .column(IntColumn.of(COL_NAME4, 2))
                .column(TextColumn.ofNull(COL_NAME2))
                .column(IntColumn.ofNull(COL_NAME3))
                .column(BooleanColumn.ofNull(COL_NAME5))
                .column(BlobColumn.ofNull(COL_NAME6))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(COL_NAME1, 2))
                .column(IntColumn.of(COL_NAME4, 1))
                .column(TextColumn.ofNull(COL_NAME2))
                .column(IntColumn.ofNull(COL_NAME3))
                .column(BooleanColumn.ofNull(COL_NAME5))
                .column(BlobColumn.ofNull(COL_NAME6))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(COL_NAME1, 3))
                .column(IntColumn.of(COL_NAME4, 0))
                .column(TextColumn.ofNull(COL_NAME2))
                .column(IntColumn.ofNull(COL_NAME3))
                .column(BooleanColumn.ofNull(COL_NAME5))
                .column(BlobColumn.ofNull(COL_NAME6))
                .build()));
    assertThat(results).hasSize(2);
  }

  @Test
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();

    // Act
    ScanAll scanAll =
        new ScanAll().withProjections(Arrays.asList(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME6));
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
                                    .column(IntColumn.of(COL_NAME1, i))
                                    .column(TextColumn.of(COL_NAME2, Integer.toString(i + j)))
                                    .column(IntColumn.of(COL_NAME3, i + j))
                                    .column(BlobColumn.ofNull(COL_NAME6))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(actualResults, expectedResults);
    actualResults.forEach(
        actualResult ->
            assertThat(actualResult.getContainedColumnNames())
                .containsOnly(COL_NAME1, COL_NAME2, COL_NAME3, COL_NAME6));
  }

  @Test
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues()
      throws ExecutionException, IOException {
    // Arrange
    for (int i = 0; i < 345; i++) {
      Key partitionKey = new Key(COL_NAME1, i % 4);
      Key clusteringKey = new Key(COL_NAME4, i);
      storage.put(
          new Put(partitionKey, clusteringKey)
              .withBlobValue(COL_NAME6, new byte[getLargeDataSizeInBytes()]));
    }
    Scan scan = new ScanAll();

    // Act
    List<Result> results = scanAll(scan);

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    for (int i = 0; i < 345; i++) {
      expectedResults.add(
          new ExpectedResultBuilder()
              .column(IntColumn.of(COL_NAME1, i % 4))
              .column(IntColumn.of(COL_NAME4, i))
              .column(TextColumn.ofNull(COL_NAME2))
              .column(IntColumn.ofNull(COL_NAME3))
              .column(BooleanColumn.ofNull(COL_NAME5))
              .column(BlobColumn.of(COL_NAME6, new byte[getLargeDataSizeInBytes()]))
              .build());
    }
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void get_GetWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException {
    // Arrange
    Put put =
        new Put(Key.ofInt(COL_NAME1, 0), Key.ofInt(COL_NAME4, 0))
            .withTextValue(COL_NAME2, "foo")
            .withIntValue(COL_NAME3, 0)
            .withBooleanValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(TABLE);
    storage.put(put);

    // Act
    Get get = prepareGet(0, 0).withProjection(COL_NAME3).withProjection(COL_NAME5);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getContainedColumnNames()).containsOnly(COL_NAME3, COL_NAME5);
    assertThat(result.getInt(COL_NAME3)).isEqualTo(0);
    assertThat(result.getBoolean(COL_NAME5)).isTrue();
  }

  @Test
  public void scan_ScanWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException, IOException {
    // Arrange
    Put put =
        new Put(Key.ofInt(COL_NAME1, 0), Key.ofInt(COL_NAME4, 0))
            .withTextValue(COL_NAME2, "foo")
            .withIntValue(COL_NAME3, 0)
            .withBooleanValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(TABLE);
    storage.put(put);

    // Act
    Scan scan =
        new Scan(Key.ofInt(COL_NAME1, 0)).withProjection(COL_NAME3).withProjection(COL_NAME5);
    List<Result> results = scanAll(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getContainedColumnNames()).containsOnly(COL_NAME3, COL_NAME5);
    assertThat(results.get(0).getInt(COL_NAME3)).isEqualTo(0);
    assertThat(results.get(0).getBoolean(COL_NAME5)).isTrue();
  }

  @Test
  public void scan_ScanAllWithProjectionsGivenOnNonPrimaryKey_ShouldRetrieveOnlyProjectedColumns()
      throws ExecutionException, IOException {
    // Arrange
    Put put =
        new Put(Key.ofInt(COL_NAME1, 0), Key.ofInt(COL_NAME4, 0))
            .withTextValue(COL_NAME2, "foo")
            .withIntValue(COL_NAME3, 0)
            .withBooleanValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(TABLE);
    storage.put(put);

    // Act
    ScanAll scanAll = new ScanAll().withProjection(COL_NAME3).withProjection(COL_NAME5);
    List<Result> results = scanAll(scanAll);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getContainedColumnNames()).containsOnly(COL_NAME3, COL_NAME5);
    assertThat(results.get(0).getInt(COL_NAME3)).isEqualTo(0);
    assertThat(results.get(0).getBoolean(COL_NAME5)).isTrue();
  }

  private void populateRecords() {
    List<Put> puts = preparePuts();
    puts.forEach(p -> assertThatCode(() -> storage.put(p)).doesNotThrowAnyException());
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    return new Get(partitionKey, clusteringKey);
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(COL_NAME1, i);
                          Key clusteringKey = new Key(COL_NAME4, j);
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .withValue(COL_NAME2, Integer.toString(i + j))
                                  .withValue(COL_NAME3, i + j)
                                  .withValue(COL_NAME5, j % 2 == 0);
                          puts.add(put);
                        }));

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
    return new Delete(partitionKey, clusteringKey);
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(COL_NAME1, i);
                          Key clusteringKey = new Key(COL_NAME4, j);
                          Delete delete = new Delete(partitionKey, clusteringKey);
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
