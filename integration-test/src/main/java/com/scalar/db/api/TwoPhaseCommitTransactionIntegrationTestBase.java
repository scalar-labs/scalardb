package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.TransactionException;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TwoPhaseCommitTransactionIntegrationTestBase {

  private static final String NAMESPACE_BASE_NAME = "int_test_";
  protected static final String TABLE_1 = "test_table1";
  protected static final String TABLE_2 = "test_table2";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  private static final String SOME_COLUMN = "some_column";
  protected static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
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
  private DistributedTransactionAdmin admin1;
  private DistributedTransactionAdmin admin2;
  private TwoPhaseCommitTransactionManager manager1;
  private TwoPhaseCommitTransactionManager manager2;

  protected String namespace1;
  protected String namespace2;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);
    TransactionFactory factory1 = TransactionFactory.create(getProperties1(testName));
    admin1 = factory1.getTransactionAdmin();
    TransactionFactory factory2 = TransactionFactory.create(getProperties2(testName));
    admin2 = factory2.getTransactionAdmin();
    namespace1 = getNamespaceBaseName() + testName + "1";
    namespace2 = getNamespaceBaseName() + testName + "2";
    createTables();
    manager1 = factory1.getTwoPhaseCommitTransactionManager();
    manager2 = factory2.getTwoPhaseCommitTransactionManager();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract String getTestName();

  protected abstract Properties getProperties1(String testName);

  protected Properties getProperties2(String testName) {
    return getProperties1(testName);
  }

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin1.createNamespace(namespace1, true, options);
    admin1.createTable(namespace1, TABLE_1, TABLE_METADATA, true, options);
    admin1.createCoordinatorTables(true, options);
    admin2.createNamespace(namespace2, true, options);
    admin2.createTable(namespace2, TABLE_2, TABLE_METADATA, true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws ExecutionException {
    admin1.truncateTable(namespace1, TABLE_1);
    admin1.truncateCoordinatorTables();
    admin2.truncateTable(namespace2, TABLE_2);
  }

  @AfterAll
  public void afterAll() throws ExecutionException {
    dropTables();
    admin1.close();
    admin2.close();
    manager1.close();
    manager2.close();
  }

  private void dropTables() throws ExecutionException {
    admin1.dropTable(namespace1, TABLE_1);
    admin1.dropNamespace(namespace1);
    admin1.dropCoordinatorTables();
    admin2.dropTable(namespace2, TABLE_2);
    admin2.dropNamespace(namespace2);
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
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
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Get get =
        prepareGet(0, 0, namespace1, TABLE_1)
            .withProjection(ACCOUNT_ID)
            .withProjection(ACCOUNT_TYPE)
            .withProjection(BALANCE);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().contains(SOME_COLUMN)).isFalse();
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecords() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan =
        prepareScan(1, 0, 2, namespace1, TABLE_1)
            .withProjection(ACCOUNT_ID)
            .withProjection(ACCOUNT_TYPE)
            .withProjection(BALANCE);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).contains(SOME_COLUMN)).isFalse();

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).contains(SOME_COLUMN)).isFalse();

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(2))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(2).contains(SOME_COLUMN)).isFalse();
  }

  @Test
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1).withOrdering(Ordering.desc(ACCOUNT_TYPE));

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1).withLimit(2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Get get = prepareGet(0, 4, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareScan(0, 4, 4, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.start();
    transaction.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .intValue(SOME_COLUMN, 2)
            .build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    transaction = manager1.start();
    Get getBuiltByConstructor =
        new Get(Key.ofInt(SOME_COLUMN, 2))
            .forNamespace(namespace1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.LINEARIZABLE);

    Get getBuiltByBuilder =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .indexKey(Key.ofInt(SOME_COLUMN, 2))
            .build();

    // Act
    Optional<Result> result1 = transaction.get(getBuiltByConstructor);
    Optional<Result> result2 = transaction.get(getBuiltByBuilder);
    transaction.get(getBuiltByBuilder);
    transaction.prepare();
    transaction.validate();
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
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scanBuiltByConstructor =
        new Scan(Key.ofInt(SOME_COLUMN, 2))
            .forNamespace(namespace1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.LINEARIZABLE);

    Scan scanBuiltByBuilder =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .indexKey(Key.ofInt(SOME_COLUMN, 2))
            .build();

    List<ExpectedResult> expectedResults = new ArrayList<>();
    expectedResults.add(
        new ExpectedResultBuilder()
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .nonKeyColumns(
                ImmutableList.of(
                    IntColumn.of(BALANCE, INITIAL_BALANCE), IntColumn.of(SOME_COLUMN, 2)))
            .build());
    expectedResults.add(
        new ExpectedResultBuilder()
            .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
            .nonKeyColumns(
                ImmutableList.of(
                    IntColumn.of(BALANCE, INITIAL_BALANCE), IntColumn.of(SOME_COLUMN, 2)))
            .build());

    // Act
    List<Result> results1 = transaction.scan(scanBuiltByConstructor);
    List<Result> results2 = transaction.scan(scanBuiltByBuilder);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(results1, expectedResults);
    TestUtils.assertResultsContainsExactlyInAnyOrder(results2, expectedResults);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> result = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result.isPresent()).isTrue();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(get);
    another.prepare();
    another.validate();
    another.commit();

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void putWithNullValueAndCommit_ShouldCreateRecordProperly() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, null);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().isNull(BALANCE)).isTrue();
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    int amount = 100;
    int fromId = 0;
    int toId = NUM_TYPES;

    // Act
    TwoPhaseCommitTransaction transaction = manager1.begin();
    List<Get> gets = prepareGets(namespace1, TABLE_1);

    Optional<Result> fromResult = transaction.get(gets.get(fromId));
    assertThat(fromResult.isPresent()).isTrue();
    IntValue fromBalance = new IntValue(BALANCE, getBalance(fromResult.get()) - amount);

    Optional<Result> toResult = transaction.get(gets.get(toId));
    assertThat(toResult.isPresent()).isTrue();
    IntValue toBalance = new IntValue(BALANCE, getBalance(toResult.get()) + amount);

    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(fromId).withValue(fromBalance);
    puts.get(toId).withValue(toBalance);
    transaction.put(puts.get(fromId));
    transaction.put(puts.get(toId));

    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    fromResult = another.get(gets.get(fromId));
    assertThat(fromResult.isPresent()).isTrue();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE - amount);

    toResult = another.get(gets.get(toId));
    assertThat(toResult.isPresent()).isTrue();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE + amount);
    another.prepare();
    another.validate();
    another.commit();
  }

  @Test
  public void putAndRollback_ShouldNotCreateRecord() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, INITIAL_BALANCE);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.put(put);
    transaction.rollback();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void putAndAbort_ShouldNotCreateRecord() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, INITIAL_BALANCE);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.put(put);
    transaction.abort();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void deleteAndCommit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result1 = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result1.isPresent()).isFalse();
  }

  @Test
  public void deleteAndRollback_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.rollback();

    // Assert
    assertThat(result).isPresent();
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result1 = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result1).isPresent();
  }

  @Test
  public void deleteAndAbort_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.abort();

    // Assert
    assertThat(result).isPresent();
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result1 = another.get(get);
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result1).isPresent();
  }

  @Test
  public void mutateAndCommit_ShouldMutateRecordsProperly() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get1 = prepareGet(0, 0, namespace1, TABLE_1);
    Get get2 = prepareGet(1, 0, namespace1, TABLE_1);
    Put put = preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, INITIAL_BALANCE - 100);
    Delete delete = prepareDelete(1, 0, namespace1, TABLE_1);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.get(get1);
    transaction.get(get2);
    transaction.mutate(Arrays.asList(put, delete));
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result1 = another.get(get1);
    Optional<Result> result2 = another.get(get2);
    another.prepare();
    another.validate();
    another.commit();

    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE - 100);
    assertThat(result2.isPresent()).isFalse();
  }

  @Test
  public void mutateAndCommit_WithMultipleSubTransactions_ShouldMutateRecordsProperly()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecords(manager2, namespace2, TABLE_2);

    Get get1 = prepareGet(0, 0, namespace1, TABLE_1);
    Get get2 = prepareGet(1, 0, namespace2, TABLE_2);
    Put put = preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, INITIAL_BALANCE - 100);
    Delete delete = prepareDelete(1, 0, namespace2, TABLE_2);

    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    // Act
    transaction1.get(get1);
    transaction1.put(put);

    transaction2.get(get2);
    transaction2.delete(delete);

    // Prepare
    transaction1.prepare();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.prepare();

    // Validate
    transaction1.validate();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.validate();

    // Commit
    transaction1.commit();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.commit();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());
    Optional<Result> result1 = another1.get(get1);
    Optional<Result> result2 = another2.get(get2);
    another1.prepare();
    another2.prepare();
    another1.validate();
    another2.validate();
    another1.commit();
    another2.commit();

    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE - 100);
    assertThat(result2.isPresent()).isFalse();
  }

  @Test
  public void mutateAndRollback_WithMultipleSubTransactions_ShouldRollbackRecordsProperly()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecords(manager2, namespace2, TABLE_2);

    Get get1 = prepareGet(0, 0, namespace1, TABLE_1);
    Get get2 = prepareGet(1, 0, namespace2, TABLE_2);
    Put put = preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, INITIAL_BALANCE - 100);
    Delete delete = prepareDelete(1, 0, namespace2, TABLE_2);

    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    // Act
    transaction1.get(get1);
    transaction1.put(put);

    transaction2.get(get2);
    transaction2.delete(delete);

    // Prepare
    transaction1.prepare();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.prepare();

    // Validate
    transaction1.validate();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.validate();

    // Rollback
    transaction1.rollback();

    transaction2 = manager2.resume(transaction1.getId());
    transaction2.rollback();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());
    Optional<Result> result1 = another1.get(get1);
    Optional<Result> result2 = another2.get(get2);
    another1.prepare();
    another2.prepare();
    another1.validate();
    another2.validate();
    another1.commit();
    another2.commit();

    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Act
    TransactionState state = manager1.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.prepare();
    transaction2.validate();
    transaction2.commit();

    assertThatCode(transaction1::prepare).isInstanceOf(PreparationConflictException.class);
    transaction1.rollback();

    // Act
    TransactionState state = manager1.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager1.rollback(transaction.getId());

    transaction.prepare();
    transaction.validate();
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    TransactionState state = manager1.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager1.abort(transaction.getId());

    transaction.prepare();
    transaction.validate();
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    TransactionState state = manager1.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void scan_ScanAllGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j -> {
                          ExpectedResultBuilder erBuilder =
                              new ExpectedResultBuilder()
                                  .partitionKey(Key.ofInt(ACCOUNT_ID, i))
                                  .clusteringKey(Key.ofInt(ACCOUNT_TYPE, j))
                                  .nonKeyColumns(
                                      ImmutableList.of(
                                          IntColumn.of(BALANCE, INITIAL_BALANCE),
                                          IntColumn.of(SOME_COLUMN, i * j)));
                          expectedResults.add(erBuilder.build());
                        }));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scan_ScanAllGivenWithLimit_ShouldReturnLimitedAmountOfRecords()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction putTransaction = manager1.begin();
    putTransaction.put(
        Arrays.asList(
            new Put(Key.ofInt(ACCOUNT_ID, 1), Key.ofInt(ACCOUNT_TYPE, 1))
                .forNamespace(namespace1)
                .forTable(TABLE_1),
            new Put(Key.ofInt(ACCOUNT_ID, 1), Key.ofInt(ACCOUNT_TYPE, 2))
                .forNamespace(namespace1)
                .forTable(TABLE_1),
            new Put(Key.ofInt(ACCOUNT_ID, 2), Key.ofInt(ACCOUNT_TYPE, 1))
                .forNamespace(namespace1)
                .forTable(TABLE_1),
            new Put(Key.ofInt(ACCOUNT_ID, 3), Key.ofInt(ACCOUNT_TYPE, 0))
                .forNamespace(namespace1)
                .forTable(TABLE_1)));
    putTransaction.prepare();
    putTransaction.validate();
    putTransaction.commit();

    TwoPhaseCommitTransaction scanAllTransaction = manager1.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1).withLimit(2);

    // Act
    List<Result> results = scanAllTransaction.scan(scanAll);
    scanAllTransaction.prepare();
    scanAllTransaction.validate();
    scanAllTransaction.commit();

    // Assert
    TestUtils.assertResultsAreASubsetOf(
        results,
        ImmutableList.of(
            new ExpectedResultBuilder()
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .nonKeyColumns(
                    Arrays.asList(IntColumn.ofNull(BALANCE), IntColumn.ofNull(SOME_COLUMN)))
                .build(),
            new ExpectedResultBuilder()
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .nonKeyColumns(
                    Arrays.asList(IntColumn.ofNull(BALANCE), IntColumn.ofNull(SOME_COLUMN)))
                .build(),
            new ExpectedResultBuilder()
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .nonKeyColumns(
                    Arrays.asList(IntColumn.ofNull(BALANCE), IntColumn.ofNull(SOME_COLUMN)))
                .build(),
            new ExpectedResultBuilder()
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .nonKeyColumns(
                    Arrays.asList(IntColumn.ofNull(BALANCE), IntColumn.ofNull(SOME_COLUMN)))
                .build()));
    assertThat(results).hasSize(2);
  }

  @Test
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    ScanAll scanAll =
        prepareScanAll(namespace1, TABLE_1).withProjection(ACCOUNT_TYPE).withProjection(BALANCE);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j -> {
                          ExpectedResultBuilder erBuilder =
                              new ExpectedResultBuilder()
                                  .clusteringKey(Key.ofInt(ACCOUNT_TYPE, j))
                                  .nonKeyColumns(
                                      ImmutableList.of(IntColumn.of(BALANCE, INITIAL_BALANCE)));
                          expectedResults.add(erBuilder.build());
                        }));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
    results.forEach(
        result -> {
          assertThat(result.contains(ACCOUNT_ID)).isFalse();
          assertThat(result.contains(SOME_COLUMN)).isFalse();
        });
  }

  @Test
  public void scan_ScanAllGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void
      get_GetWithProjectionOnNonPrimaryKeyColumnsForGivenForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    populateSingleRecord(namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    Get get =
        prepareGet(0, 0, namespace1, TABLE_1).withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
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
    TwoPhaseCommitTransaction transaction = manager1.begin();
    populateSingleRecord(namespace1, TABLE_1);
    Scan scan =
        prepareScan(0, 0, 0, namespace1, TABLE_1)
            .withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
    populateSingleRecord(namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    ScanAll scanAll =
        prepareScanAll(namespace1, TABLE_1).withProjections(Arrays.asList(BALANCE, SOME_COLUMN));

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    ExpectedResult expectedResult =
        new ExpectedResultBuilder()
            .nonKeyColumns(
                ImmutableList.of(
                    IntColumn.of(BALANCE, INITIAL_BALANCE), IntColumn.ofNull(SOME_COLUMN)))
            .build();
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, Collections.singletonList(expectedResult));
  }

  @Test
  public void resume_WithBeginningTransaction_ShouldReturnBegunTransaction()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    TwoPhaseCommitTransaction resumed = manager1.resume(transaction.getId());

    // Assert
    assertThat(resumed).isEqualTo(transaction);

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void resume_WithoutBeginningTransaction_ShouldThrowTransactionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager1.resume("txId")).isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_WithBeginningAndCommittingTransaction_ShouldThrowTransactionException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.prepare();
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager1.resume(transaction.getId()))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_WithBeginningAndRollingBackTransaction_ShouldThrowTransactionException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager1.resume(transaction.getId()))
        .isInstanceOf(TransactionException.class);
  }

  private void populateRecords(
      TwoPhaseCommitTransactionManager manager, String namespaceName, String tableName)
      throws TransactionException {
    TwoPhaseCommitTransaction transaction = manager.begin();
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
                                  .forNamespace(namespaceName)
                                  .forTable(tableName)
                                  .withIntValue(BALANCE, INITIAL_BALANCE)
                                  .withIntValue(SOME_COLUMN, i * j);
                          try {
                            transaction.put(put);
                          } catch (CrudException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  private void populateSingleRecord(String namespaceName, String tableName)
      throws TransactionException {
    Put put =
        new Put(Key.ofInt(ACCOUNT_ID, 0), Key.ofInt(ACCOUNT_TYPE, 0))
            .forNamespace(namespaceName)
            .forTable(tableName)
            .withIntValue(BALANCE, INITIAL_BALANCE);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  protected Get prepareGet(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Get> prepareGets(String namespaceName, String tableName) {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> gets.add(prepareGet(i, j, namespaceName, tableName))));
    return gets;
  }

  protected Scan prepareScan(
      int id, int fromType, int toType, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  private ScanAll prepareScanAll(String namespaceName, String tableName) {
    return new ScanAll()
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Put preparePut(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Put> preparePuts(String namespaceName, String tableName) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> puts.add(preparePut(i, j, namespaceName, tableName))));

    return puts;
  }

  private Delete prepareDelete(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }
}
