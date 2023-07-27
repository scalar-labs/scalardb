package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.GetBuilder.BuildableGet;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.TestUtils;
import com.scalar.db.util.TestUtils.ExpectedResult;
import com.scalar.db.util.TestUtils.ExpectedResult.ExpectedResultBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedTransactionIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "int_test_";
  protected static final String TABLE = "test_table";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final String SOME_COLUMN = "some_column";
  protected static final int INITIAL_BALANCE = 1000;
  protected static final int NUM_ACCOUNTS = 4;
  protected static final int NUM_TYPES = 4;
  protected static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ACCOUNT_ID, DataType.INT)
          .addColumn(ACCOUNT_TYPE, DataType.INT)
          .addColumn(BALANCE, DataType.INT)
          .addColumn(SOME_COLUMN, DataType.INT)
          .addPartitionKey(ACCOUNT_ID)
          .addClusteringKey(ACCOUNT_TYPE)
          .addSecondaryIndex(SOME_COLUMN)
          .build();
  protected DistributedTransactionAdmin admin;
  protected DistributedTransactionManager manager;
  protected String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);
    Properties properties = getProperties(testName);
    TransactionFactory factory = TransactionFactory.create(properties);
    admin = factory.getTransactionAdmin();
    namespace = getNamespaceBaseName() + testName;
    createTables();
    manager = factory.getTransactionManager();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract String getTestName();

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, TABLE_METADATA, true, options);
    admin.createCoordinatorTables(true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    admin.truncateTable(namespace, TABLE);
    admin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTables();
    admin.close();
    manager.close();
  }

  private void dropTables() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
    admin.dropCoordinatorTables();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Get get = prepareGet(0, 0);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().getInt(SOME_COLUMN)).isEqualTo(0);
  }

  @Test
  public void get_GetWithProjectionGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Get get =
        prepareGet(0, 0)
            .withProjection(ACCOUNT_ID)
            .withProjection(ACCOUNT_TYPE)
            .withProjection(BALANCE);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().getContainedColumnNames())
        .containsOnly(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecords() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareScan(1, 0, 2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(0);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(1);

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(2))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(2).getInt(SOME_COLUMN)).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithProjectionsGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        prepareScan(1, 0, 2)
            .withProjection(ACCOUNT_ID)
            .withProjection(ACCOUNT_TYPE)
            .withProjection(BALANCE);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    results.forEach(
        result -> {
          assertThat(result.getContainedColumnNames())
              .containsOnly(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE);
          assertThat(getBalance(result)).isEqualTo(INITIAL_BALANCE);
        });
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
  }

  @Test
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareScan(1, 0, 2).withOrdering(Ordering.desc(ACCOUNT_TYPE));

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(1);

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(2))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(2).getInt(SOME_COLUMN)).isEqualTo(0);
  }

  @Test
  public void scan_ScanWithLimitGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareScan(1, 0, 2).withLimit(2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(0);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Get get = prepareGet(0, 4);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareScan(0, 4, 6);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.start();
    transaction.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .intValue(SOME_COLUMN, 2)
            .build());
    transaction.commit();

    transaction = manager.start();
    Get getBuiltByConstructor =
        new Get(Key.ofInt(SOME_COLUMN, 2))
            .forNamespace(namespace)
            .forTable(TABLE)
            .withConsistency(Consistency.LINEARIZABLE);

    Get getBuiltByBuilder =
        Get.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .indexKey(Key.ofInt(SOME_COLUMN, 2))
            .build();

    // Act
    Optional<Result> result1 = transaction.get(getBuiltByConstructor);
    Optional<Result> result2 = transaction.get(getBuiltByBuilder);
    transaction.get(getBuiltByBuilder);
    transaction.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(result1.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result1.get().getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(result2).isEqualTo(result1);
  }

  @Test
  public void scan_ScanGivenForIndexColumn_ShouldReturnRecords() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scanBuiltByConstructor =
        new Scan(Key.ofInt(SOME_COLUMN, 2))
            .forNamespace(namespace)
            .forTable(TABLE)
            .withConsistency(Consistency.LINEARIZABLE);

    Scan scanBuiltByBuilder =
        Scan.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .indexKey(Key.ofInt(SOME_COLUMN, 2))
            .build();

    List<ExpectedResult> expectedResults = new ArrayList<>();
    expectedResults.add(
        new ExpectedResultBuilder()
            .column(IntColumn.of(ACCOUNT_ID, 1))
            .column(IntColumn.of(ACCOUNT_TYPE, 2))
            .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .column(IntColumn.of(SOME_COLUMN, 2))
            .build());
    expectedResults.add(
        new ExpectedResultBuilder()
            .column(IntColumn.of(ACCOUNT_ID, 2))
            .column(IntColumn.of(ACCOUNT_TYPE, 1))
            .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .column(IntColumn.of(SOME_COLUMN, 2))
            .build());

    // Act
    List<Result> results1 = transaction.scan(scanBuiltByConstructor);
    List<Result> results2 = transaction.scan(scanBuiltByBuilder);
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(results1, expectedResults);
    TestUtils.assertResultsContainsExactlyInAnyOrder(results2, expectedResults);
  }

  @Test
  public void scan_ScanAllGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    ScanAll scanAll = prepareScanAll();

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(ACCOUNT_ID, i))
                                    .column(IntColumn.of(ACCOUNT_TYPE, j))
                                    .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
                                    .column(IntColumn.of(SOME_COLUMN, i * j))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scan_ScanAllGivenWithLimit_ShouldReturnLimitedAmountOfRecords()
      throws TransactionException {
    // Arrange
    DistributedTransaction putTransaction = manager.start();
    putTransaction.put(
        Arrays.asList(
            new Put(Key.ofInt(ACCOUNT_ID, 1), Key.ofInt(ACCOUNT_TYPE, 1))
                .forNamespace(namespace)
                .forTable(TABLE),
            new Put(Key.ofInt(ACCOUNT_ID, 1), Key.ofInt(ACCOUNT_TYPE, 2))
                .forNamespace(namespace)
                .forTable(TABLE),
            new Put(Key.ofInt(ACCOUNT_ID, 2), Key.ofInt(ACCOUNT_TYPE, 1))
                .forNamespace(namespace)
                .forTable(TABLE),
            new Put(Key.ofInt(ACCOUNT_ID, 3), Key.ofInt(ACCOUNT_TYPE, 0))
                .forNamespace(namespace)
                .forTable(TABLE)));
    putTransaction.commit();

    DistributedTransaction scanAllTransaction = manager.start();
    ScanAll scanAll = prepareScanAll().withLimit(2);

    // Act
    List<Result> results = scanAllTransaction.scan(scanAll);
    scanAllTransaction.commit();

    // Assert
    TestUtils.assertResultsAreASubsetOf(
        results,
        ImmutableList.of(
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 1))
                .column(IntColumn.of(ACCOUNT_TYPE, 1))
                .column(IntColumn.ofNull(BALANCE))
                .column(IntColumn.ofNull(SOME_COLUMN))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 1))
                .column(IntColumn.of(ACCOUNT_TYPE, 2))
                .column(IntColumn.ofNull(BALANCE))
                .column(IntColumn.ofNull(SOME_COLUMN))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 2))
                .column(IntColumn.of(ACCOUNT_TYPE, 1))
                .column(IntColumn.ofNull(BALANCE))
                .column(IntColumn.ofNull(SOME_COLUMN))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 3))
                .column(IntColumn.of(ACCOUNT_TYPE, 0))
                .column(IntColumn.ofNull(BALANCE))
                .column(IntColumn.ofNull(SOME_COLUMN))
                .build()));
    assertThat(results).hasSize(2);
  }

  @Test
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    ScanAll scanAll = prepareScanAll().withProjection(ACCOUNT_TYPE).withProjection(BALANCE);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(ACCOUNT_TYPE, j))
                                    .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scanAll_ScanAllGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.start();
    ScanAll scanAll = prepareScanAll();

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put = preparePut(0, 0).withValue(BALANCE, expected);
    DistributedTransaction transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0);
    DistributedTransaction another = manager.start();
    Optional<Result> result = another.get(get);
    another.commit();
    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0);
    DistributedTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result.isPresent()).isTrue();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0).withValue(BALANCE, expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.start();
    Optional<Result> actual = another.get(get);
    another.commit();

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void putWithNullValueAndCommit_ShouldCreateRecordProperly() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0).withIntValue(BALANCE, null);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0);
    DistributedTransaction another = manager.begin();
    Optional<Result> result = another.get(get);
    another.commit();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().isNull(BALANCE)).isTrue();
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populateRecords();
    List<Get> gets = prepareGets();
    int amount = 100;
    int fromId = 0;
    int toId = NUM_TYPES;

    // Act
    DistributedTransaction transaction = manager.begin();

    Optional<Result> fromResult = transaction.get(gets.get(fromId));
    assertThat(fromResult.isPresent()).isTrue();
    IntValue fromBalance = new IntValue(BALANCE, getBalance(fromResult.get()) - amount);

    Optional<Result> toResult = transaction.get(gets.get(toId));
    assertThat(toResult.isPresent()).isTrue();
    IntValue toBalance = new IntValue(BALANCE, getBalance(toResult.get()) + amount);

    List<Put> puts = preparePuts();
    puts.get(fromId).withValue(fromBalance);
    puts.get(toId).withValue(toBalance);
    transaction.put(puts.get(fromId));
    transaction.put(puts.get(toId));

    transaction.commit();

    // Assert
    DistributedTransaction another = manager.begin();
    fromResult = another.get(gets.get(fromId));
    assertThat(fromResult.isPresent()).isTrue();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE - amount);

    toResult = another.get(gets.get(toId));
    assertThat(toResult.isPresent()).isTrue();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE + amount);
    another.commit();
  }

  @Test
  public void putAndAbort_ShouldNotCreateRecord() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0).withValue(BALANCE, INITIAL_BALANCE);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.abort();

    // Assert
    Get get = prepareGet(0, 0);
    DistributedTransaction another = manager.begin();
    Optional<Result> result = another.get(get);
    another.commit();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void putAndRollback_ShouldNotCreateRecord() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0).withValue(BALANCE, INITIAL_BALANCE);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.rollback();

    // Assert
    Get get = prepareGet(0, 0);
    DistributedTransaction another = manager.begin();
    Optional<Result> result = another.get(get);
    another.commit();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void deleteAndCommit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0);
    Delete delete = prepareDelete(0, 0);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    DistributedTransaction another = manager.begin();
    Optional<Result> result1 = another.get(get);
    another.commit();
    assertThat(result1.isPresent()).isFalse();
  }

  @Test
  public void deleteAndAbort_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0);
    Delete delete = prepareDelete(0, 0);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.abort();

    // Assert
    assertThat(result).isPresent();
    DistributedTransaction another = manager.begin();
    Optional<Result> result1 = another.get(get);
    another.commit();
    assertThat(result1).isPresent();
  }

  @Test
  public void deleteAndRollback_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0);
    Delete delete = prepareDelete(0, 0);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.rollback();

    // Assert
    assertThat(result).isPresent();
    DistributedTransaction another = manager.begin();
    Optional<Result> result1 = another.get(get);
    another.commit();
    assertThat(result1).isPresent();
  }

  @Test
  public void mutateAndCommit_ShouldMutateRecordsProperly() throws TransactionException {
    // Arrange
    populateRecords();
    Get get1 = prepareGet(0, 0);
    Get get2 = prepareGet(1, 0);
    Put put = preparePut(0, 0).withIntValue(BALANCE, INITIAL_BALANCE - 100);
    Delete delete = prepareDelete(1, 0);

    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.get(get1);
    transaction.get(get2);
    transaction.mutate(Arrays.asList(put, delete));
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> result1 = another.get(get1);
    Optional<Result> result2 = another.get(get2);
    another.commit();

    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE - 100);
    assertThat(result2.isPresent()).isFalse();
  }

  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.get(prepareGet(0, 0));
    transaction.put(preparePut(0, 0).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    TransactionState state = manager.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() throws TransactionException {
    // Arrange
    DistributedTransaction transaction1 = manager.begin();
    transaction1.get(prepareGet(0, 0));
    transaction1.put(preparePut(0, 0).withValue(BALANCE, 1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0));
    transaction2.put(preparePut(0, 0).withValue(BALANCE, 1));
    transaction2.commit();

    assertThatCode(transaction1::commit).isInstanceOf(CommitConflictException.class);

    // Act
    TransactionState state = manager.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.get(prepareGet(0, 0));
    transaction.put(preparePut(0, 0).withValue(BALANCE, 1));

    // Act
    manager.abort(transaction.getId());

    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    TransactionState state = manager.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.get(prepareGet(0, 0));
    transaction.put(preparePut(0, 0).withValue(BALANCE, 1));

    // Act
    manager.rollback(transaction.getId());

    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    TransactionState state = manager.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void
      get_GetWithProjectionOnNonPrimaryKeyColumnsForGivenForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    populateSingleRecord();
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0, 0).withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getContainedColumnNames()).containsOnly(BALANCE, SOME_COLUMN);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void
      scan_ScanWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    populateSingleRecord();
    Scan scan = prepareScan(0, 0, 0).withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    results.forEach(
        result -> {
          assertThat(result.getContainedColumnNames()).containsOnly(BALANCE, SOME_COLUMN);
          assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
          assertThat(result.isNull(SOME_COLUMN)).isTrue();
        });
  }

  @Test
  public void
      scan_ScanAllWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    populateSingleRecord();
    DistributedTransaction transaction = manager.begin();
    ScanAll scanAll = prepareScanAll().withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    ExpectedResult expectedResult =
        new ExpectedResultBuilder()
            .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .column(IntColumn.ofNull(SOME_COLUMN))
            .build();
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, Collections.singletonList(expectedResult));
  }

  @Test
  public void resume_WithBeginningTransaction_ShouldReturnBegunTransaction()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();

    // Act
    DistributedTransaction resumed = manager.resume(transaction.getId());

    // Assert
    assertThat(resumed.getId()).isEqualTo(transaction.getId());

    transaction.commit();
  }

  @Test
  public void resume_WithoutBeginningTransaction_ShouldThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.resume("txId"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_WithBeginningAndCommittingTransaction_ShouldThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      resume_WithBeginningAndRollingBackTransaction_ShouldThrowTransactionNotFoundException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void operation_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace);
    final DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager();
    try {
      // Arrange
      populateRecords();
      Get get =
          Get.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();
      Scan scan = Scan.newBuilder().table(TABLE).all().build();
      Put put =
          Put.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();
      Delete delete =
          Delete.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();
      Mutation putAsMutation1 =
          Put.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();
      Mutation deleteAsMutation2 =
          Delete.newBuilder()
              .table(TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.get(get);
                tx.scan(scan);
                tx.put(put);
                tx.delete(delete);
                tx.mutate(ImmutableList.of(putAsMutation1, deleteAsMutation2));
                tx.commit();
              })
          .doesNotThrowAnyException();
    } finally {
      if (managerWithDefaultNamespace != null) {
        managerWithDefaultNamespace.close();
      }
    }
  }

  @Test
  public void put_withPutIfWithVerifiedCondition_shouldPutProperly() throws TransactionException {
    // Arrange
    int someColumnValue = 10;
    Put initialData =
        Put.newBuilder(preparePut(0, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .intValue(SOME_COLUMN, someColumnValue)
            .build();
    put(initialData);

    int updatedBalance = 2;
    Put putIf =
        Put.newBuilder(initialData)
            .intValue(BALANCE, updatedBalance)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act
    getThenPut(putIf);

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(updatedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(someColumnValue);
  }

  @Test
  public void put_withPutIfExistsWhenRecordExists_shouldPutProperly() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0);
    put(put);
    Put putIfExists =
        Put.newBuilder(put)
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act Assert
    getThenPut(putIfExists);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordDoesNotExist_shouldPutProperly()
      throws TransactionException {
    // Arrange
    Put putIfNotExists =
        Put.newBuilder(preparePut(0, 0)).condition(ConditionBuilder.putIfNotExists()).build();

    // Act Assert
    getThenPut(putIfNotExists);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void delete_withDeleteIfWithVerifiedCondition_shouldDeleteProperly()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).intValue(BALANCE, INITIAL_BALANCE).build();
    put(initialData);

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNullInt())
                    .build())
            .build();

    // Act
    getThenDelete(deleteIf);

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isFalse();
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordsExists_shouldDeleteProperly()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).build();
    put(initialData);

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0)).condition(ConditionBuilder.deleteIfExists()).build();

    // Act Assert
    getThenDelete(deleteIf);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isFalse();
  }

  @Test
  public void put_withPutIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put put =
        Put.newBuilder(preparePut(0, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIf(ConditionBuilder.column(BALANCE).isNullInt()).build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(put)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> result = get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put put =
        Put.newBuilder(preparePut(0, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(put)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> result = get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordExists_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0);
    put(put);
    Put putIfNotExists = Put.newBuilder(put).condition(ConditionBuilder.putIfNotExists()).build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(putIfNotExists))
        .isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void
      delete_withDeleteIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {
    // Arrange
    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0)).condition(ConditionBuilder.deleteIfExists()).build();

    // Act Assert
    assertThatThrownBy(() -> getThenDelete(deleteIf))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void delete_withDeleteIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).build();
    put(initialData);

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenDelete(deleteIf))
        .isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void put_withPutIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).intValue(BALANCE, INITIAL_BALANCE).build();
    put(initialData);

    Put putIf =
        Put.newBuilder(initialData)
            .intValue(BALANCE, 2)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(putIf)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  protected Optional<Result> get(Get get) throws TransactionException {
    DistributedTransaction tx = manager.start();
    try {
      Optional<Result> result = tx.get(get);
      tx.commit();
      return result;
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  protected void put(Put put) throws TransactionException {
    DistributedTransaction tx = manager.start();
    try {
      tx.put(put);
      tx.commit();
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  protected void getThenPut(Put put) throws TransactionException {
    getThenMutate(put);
  }

  protected void getThenDelete(Delete delete) throws TransactionException {
    getThenMutate(delete);
  }

  protected void getThenMutate(Mutation mutation) throws TransactionException {
    assert mutation.forNamespace().isPresent();
    assert mutation.forTable().isPresent();
    BuildableGet get =
        Get.newBuilder()
            .namespace(mutation.forNamespace().get())
            .table(mutation.forTable().get())
            .partitionKey(mutation.getPartitionKey());
    mutation.getClusteringKey().ifPresent(get::clusteringKey);
    DistributedTransaction tx = manager.start();
    try {
      tx.get(get.build());
      tx.mutate(Collections.singletonList(mutation));
      tx.commit();
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  protected void populateRecords() throws TransactionException {
    DistributedTransaction transaction = manager.start();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(ACCOUNT_ID, i);
                          Key clusteringKey = new Key(ACCOUNT_TYPE, j);
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .forNamespace(namespace)
                                  .forTable(TABLE)
                                  .withIntValue(BALANCE, INITIAL_BALANCE)
                                  .withIntValue(SOME_COLUMN, i * j);
                          try {
                            transaction.put(put);
                          } catch (CrudException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    transaction.commit();
  }

  protected void populateSingleRecord() throws TransactionException {
    Put put =
        new Put(Key.ofInt(ACCOUNT_ID, 0), Key.ofInt(ACCOUNT_TYPE, 0))
            .forNamespace(namespace)
            .forTable(TABLE)
            .withIntValue(BALANCE, INITIAL_BALANCE);
    DistributedTransaction transaction = manager.start();
    transaction.put(put);
    transaction.commit();
  }

  protected Get prepareGet(int id, int type) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected List<Get> prepareGets() {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(i -> IntStream.range(0, NUM_TYPES).forEach(j -> gets.add(prepareGet(i, j))));
    return gets;
  }

  protected Scan prepareScan(int id, int fromType, int toType) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  protected ScanAll prepareScanAll() {
    return new ScanAll()
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected Put preparePut(int id, int type) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(i -> IntStream.range(0, NUM_TYPES).forEach(j -> puts.add(preparePut(i, j))));

    return puts;
  }

  protected Delete prepareDelete(int id, int type) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }
}
