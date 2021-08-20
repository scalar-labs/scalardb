package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Test;

public abstract class IntegrationTestBase {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String TABLE = "test_table";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private List<Put> puts;
  private List<Delete> deletes;

  protected void setUp(DistributedStorage storage) {
    this.storage = storage;
  }

 protected static void createTable(Map<String, String> options) throws ExecutionException {
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addSecondaryIndex(COL_NAME3)
            .build(),
        options);
  }

  protected void deleteData() throws ExecutionException {
    admin.truncateTable(NAMESPACE, TABLE);
  }

  static protected void deleteTable() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE);
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
    storage.with("wrong_" + NAMESPACE, TABLE); // a wrong namespace
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void operation_WrongTableGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(NAMESPACE, "wrong_" + TABLE); // a wrong table
    Key partitionKey = new Key(COL_NAME1, 0);
    Key clusteringKey = new Key(COL_NAME4, 0);
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
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
    get.withProjection(COL_NAME1).withProjection(COL_NAME2).withProjection(COL_NAME3);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue(); // since it's clustering key
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isFalse();
  }

  @Test
  public void scan_ScanWithProjectionsGiven_ShouldRetrieveSpecifiedValues() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(COL_NAME1, pKey))
            .withProjection(COL_NAME1)
            .withProjection(COL_NAME2)
            .withProjection(COL_NAME3);
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(0, 1, 2));
    actual.forEach(
        a -> {
          assertThat(a.getValue(COL_NAME1).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME2).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME3).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME4).isPresent()).isTrue(); // since it's clustering key
          assertThat(a.getValue(COL_NAME5).isPresent()).isFalse();
        });
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws Exception {
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

    assertScanResultWithoutOrdering(results, pKey, COL_NAME4, Arrays.asList(0, 1, 2));

    scanner.close();
  }

  @Test
  public void scan_ScanWithPartitionGivenThreeTimes_ShouldRetrieveResultsProperlyEveryTime()
      throws Exception {
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
      throws Exception {
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
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(0, 1));
  }

  @Test
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws Exception {
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
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(1, 2));
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults() throws Exception {
    // Arrange
    puts = preparePuts();
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
  public void scan_ScanWithOrderDescGiven_ShouldReturnDescendingOrderedResults() throws Exception {
    // Arrange
    puts = preparePuts();
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
  public void scan_ScanWithLimitGiven_ShouldReturnGivenNumberOfResults() throws Exception {
    // setup
    puts = preparePuts();
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
  public void scannerIterator_ScanWithPartitionKeyGiven_ShouldRetrieveCorrectResults()
      throws Exception {
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
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(0, 1, 2));
  }

  @Test
  public void scannerIterator_OneAndIteratorCalled_ShouldRetrieveCorrectResults() throws Exception {
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

    List<Integer> expected = new ArrayList<>(Arrays.asList(0, 1, 2));
    expected.remove(Integer.valueOf(result.get().getValue(COL_NAME4).get().getAsInt()));
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, expected);
  }

  @Test
  public void scannerIterator_AllAndIteratorCalled_ShouldRetrieveCorrectResults() throws Exception {
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
    assertScanResultWithoutOrdering(all, pKey, COL_NAME4, Arrays.asList(0, 1, 2));
    assertThat(actual).isEmpty();
  }

  @Test
  public void scannerIterator_IteratorCalledMultipleTimes_ShouldRetrieveCorrectResults()
      throws Exception {
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
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(0, 1, 2));
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
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
    puts = preparePuts();
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
  public void put_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertScanResultWithoutOrdering(
        results, pKey, COL_NAME4, Arrays.asList(pKey + cKey, pKey + cKey + 1, pKey + cKey + 2));
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertScanResultWithoutOrdering(
        results, pKey, COL_NAME4, Arrays.asList(pKey + cKey, pKey + cKey + 1, pKey + cKey + 2));
  }

  @Test
  public void put_PutWithoutValuesGiven_ShouldStoreProperly() throws Exception {
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
  public void put_PutWithoutValuesGivenTwice_ShouldStoreProperly() throws Exception {
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
      throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
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
          throws Exception {
    // Arrange
    puts = preparePuts();
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
      throws Exception {
    // Arrange
    puts = preparePuts();

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
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    puts = preparePuts();
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
    assertScanResultWithoutOrdering(results, 0, COL_NAME4, Arrays.asList(0, 1));
  }

  @Test
  public void put_PutWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
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
    puts = preparePuts();
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
    puts = preparePuts();
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
    puts = preparePuts();
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
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws Exception {
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
    assertScanResultWithoutOrdering(results, pKey, COL_NAME4, Arrays.asList(cKey + 1, cKey + 2));
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
      throws Exception {
    // Arrange
    puts = preparePuts();
    deletes = prepareDeletes();
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
  public void mutate_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertScanResultWithoutOrdering(
        results, pKey, COL_NAME4, Arrays.asList(pKey + cKey, pKey + cKey + 1, pKey + cKey + 2));
  }

  @Test
  public void mutate_MultiplePutWithDifferentPartitionsGiven_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Arrange
    puts = preparePuts();

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
  public void mutate_PutAndDeleteGiven_ShouldUpdateAndDeleteRecordsProperly() throws Exception {
    // Arrange
    populateRecords();
    puts = preparePuts();
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
    assertScanResultWithoutOrdering(
        results, pKey, COL_NAME3, Arrays.asList(Integer.MAX_VALUE, Integer.MIN_VALUE));
  }

  @Test
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
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
          throws Exception {
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
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(cKey + 1, cKey + 2));
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
  public void get_GetGivenForIndexedColumn_ShouldGet() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get = new Get(new Key(COL_NAME3, c3));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
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
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws Exception {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan = new Scan(new Key(COL_NAME3, c3));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    List<List<Integer>> expectedValues =
        new ArrayList<>(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 1), Arrays.asList(3, 0)));
    for (Result result : actual) {
      int col1Val = result.getValue(COL_NAME1).get().getAsInt();
      int col4Val = result.getValue(COL_NAME4).get().getAsInt();
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
    Scan scan = new Scan(new Key(COL_NAME2, c2));

    // Act Assert
    assertThatThrownBy(() -> scanAll(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  private void populateRecords() {
    puts = preparePuts();
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

  protected List<Result> scanAll(Scan scan) throws Exception {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  protected void assertScanResultWithoutOrdering(
      List<Result> actual,
      int expectedPartitionKeyValue,
      String checkedColumn,
      List<Integer> expectedValues) {
    Set<Integer> expectedValuesSet = new HashSet<>(expectedValues);
    assertThat(actual.size()).isEqualTo(expectedValues.size());

    for (Result result : actual) {
      assertThat(result.getValue(COL_NAME1))
          .isEqualTo(Optional.of(new IntValue(COL_NAME1, expectedPartitionKeyValue)));

      int actualClusteringKeyValue = result.getValue(checkedColumn).get().getAsInt();
      assertThat(expectedValuesSet).contains(actualClusteringKeyValue);
      expectedValuesSet.remove(actualClusteringKeyValue);
    }

    assertThat(expectedValuesSet).isEmpty();
  }
}
