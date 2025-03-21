package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.io.Value;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.TestUtils;
import com.scalar.db.util.TestUtils.ExpectedResult;
import com.scalar.db.util.TestUtils.ExpectedResult.ExpectedResultBuilder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TwoPhaseCommitTransactionIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(TwoPhaseCommitTransactionIntegrationTestBase.class);

  protected static final String NAMESPACE_BASE_NAME = "int_test_";
  protected static final String TABLE_1 = "test_table1";
  protected static final String TABLE_2 = "test_table2";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final String SOME_COLUMN = "some_column";
  protected static final String BOOLEAN_COL = "boolean_col";
  protected static final String BIGINT_COL = "bigint_col";
  protected static final String FLOAT_COL = "float_col";
  protected static final String DOUBLE_COL = "double_col";
  protected static final String TEXT_COL = "text_col";
  protected static final String BLOB_COL = "blob_col";
  protected static final String DATE_COL = "date_col";
  protected static final String TIME_COL = "time_col";
  protected static final String TIMESTAMP_COL = "timestamp_col";
  protected static final String TIMESTAMPTZ_COL = "timestamptz_col";
  protected static final int INITIAL_BALANCE = 1000;
  protected static final int NUM_ACCOUNTS = 4;
  protected static final int NUM_TYPES = 4;
  protected DistributedTransactionAdmin admin1;
  protected DistributedTransactionAdmin admin2;
  protected TwoPhaseCommitTransactionManager manager1;
  protected TwoPhaseCommitTransactionManager manager2;

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
    TableMetadata.Builder tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(SOME_COLUMN, DataType.INT)
            .addColumn(BOOLEAN_COL, DataType.BOOLEAN)
            .addColumn(BIGINT_COL, DataType.BIGINT)
            .addColumn(FLOAT_COL, DataType.FLOAT)
            .addColumn(DOUBLE_COL, DataType.DOUBLE)
            .addColumn(TEXT_COL, DataType.TEXT)
            .addColumn(BLOB_COL, DataType.BLOB)
            .addColumn(DATE_COL, DataType.DATE)
            .addColumn(TIME_COL, DataType.TIME)
            .addColumn(TIMESTAMPTZ_COL, DataType.TIMESTAMPTZ)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .addSecondaryIndex(SOME_COLUMN);
    if (isTimestampTypeSupported()) {
      tableMetadata.addColumn(TIMESTAMP_COL, DataType.TIMESTAMP);
    }

    Map<String, String> options = getCreationOptions();
    admin1.createCoordinatorTables(true, options);
    admin1.createNamespace(namespace1, true, options);
    admin1.createTable(namespace1, TABLE_1, tableMetadata.build(), true, options);
    admin2.createNamespace(namespace2, true, options);
    admin2.createTable(namespace2, TABLE_2, tableMetadata.build(), true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    admin1.truncateTable(namespace1, TABLE_1);
    admin1.truncateCoordinatorTables();
    admin2.truncateTable(namespace2, TABLE_2);
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTables();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin1 != null) {
        admin1.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin#1", e);
    }

    try {
      if (admin2 != null) {
        admin2.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin#2", e);
    }

    try {
      if (manager1 != null) {
        manager1.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close manager#1", e);
    }

    try {
      if (manager2 != null) {
        manager2.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close manager#2", e);
    }
  }

  private void dropTables() throws ExecutionException {
    admin2.dropTable(namespace2, TABLE_2);
    admin2.dropNamespace(namespace2);
    admin1.dropTable(namespace1, TABLE_1);
    admin1.dropNamespace(namespace1);
    admin1.dropCoordinatorTables();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Get get = prepareGet(2, 3, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertResult(2, 3, result);
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
  public void get_GetWithMatchedConjunctionsGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    GetBuilder.BuildableGetFromExistingWithOngoingWhereAnd get =
        Get.newBuilder(prepareGet(1, 2, namespace1, TABLE_1))
            .where(ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
            .and(ConditionBuilder.column(SOME_COLUMN).isEqualToInt(2))
            .and(ConditionBuilder.column(BOOLEAN_COL).isNotEqualToBoolean(true))
            .and(ConditionBuilder.column(BIGINT_COL).isLessThanBigInt(BigIntColumn.MAX_VALUE))
            .and(ConditionBuilder.column(FLOAT_COL).isEqualToFloat(0.12F))
            .and(ConditionBuilder.column(DOUBLE_COL).isGreaterThanDouble(-10))
            .and(ConditionBuilder.column(TEXT_COL).isNotEqualToText("foo"))
            .and(ConditionBuilder.column(DATE_COL).isLessThanDate(LocalDate.of(3000, 1, 1)))
            .and(ConditionBuilder.column(TIME_COL).isNotNullTime())
            .and(ConditionBuilder.column(TIMESTAMPTZ_COL).isNotEqualToTimestampTZ(Instant.EPOCH));
    if (isTimestampTypeSupported()) {
      get.and(
          ConditionBuilder.column(TIMESTAMP_COL)
              .isGreaterThanOrEqualToTimestamp(LocalDateTime.of(1970, 1, 1, 1, 2)));
    }

    // Act
    Optional<Result> result = transaction.get(get.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertResult(1, 2, result);
  }

  @Test
  public void get_GetWithUnmatchedConjunctionsGivenForCommittedRecord_ShouldReturnEmpty()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Get get =
        Get.newBuilder(prepareGet(1, 1, namespace1, TABLE_1))
            .where(ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
            .and(ConditionBuilder.column(SOME_COLUMN).isEqualToInt(0))
            .build();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
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
    assertResult(1, 0, results.get(0));
    assertResult(1, 1, results.get(1));
    assertResult(1, 2, results.get(2));
  }

  @Test
  public void scan_ScanWithConjunctionsGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan =
        Scan.newBuilder(prepareScan(1, 0, 2, namespace1, TABLE_1))
            .where(ConditionBuilder.column(SOME_COLUMN).isGreaterThanOrEqualToInt(1))
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(2);

    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(1);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(2);
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
            .column(IntColumn.of(ACCOUNT_ID, 1))
            .column(IntColumn.of(ACCOUNT_TYPE, 2))
            .columns(prepareNonKeyColumns(1, 2))
            .build());
    expectedResults.add(
        new ExpectedResultBuilder()
            .column(IntColumn.of(ACCOUNT_ID, 2))
            .column(IntColumn.of(ACCOUNT_TYPE, 1))
            .columns(prepareNonKeyColumns(2, 1))
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
    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(0, 0).forEach(put::value);
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    transaction.put(put.build());
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

    assertResult(0, 0, result);
  }

  @Test
  public void putAndCommit_PutGivenForExisting_ShouldUpdateRecord() throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(2, 2).forEach(insert::value);
    insert(insert.build());
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .enableImplicitPreRead();
    prepareNonKeyColumns(0, 0).forEach(put::value);
    transaction.put(put.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertResult(0, 0, actual);
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
  public void deleteAndCommit_DeleteGivenForExisting_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.delete(delete);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void deleteAndRollback_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.delete(delete);
    transaction.rollback();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result).isPresent();
  }

  @Test
  public void deleteAndAbort_ShouldNotDeleteRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.delete(delete);
    transaction.abort();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();
    assertThat(result).isPresent();
  }

  @Test
  public void mutateAndCommit_AfterRead_ShouldMutateRecordsProperly() throws TransactionException {
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
  public void mutateAndCommit_ShouldMutateRecordsProperly() throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insertRecord1 =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(1, 1).forEach(insertRecord1::value);
    Insert insertRecord2 = prepareInsert(1, 0, namespace1, TABLE_1);

    insert(insertRecord1.build(), insertRecord2);

    PutBuilder.Buildable putRecord1 =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).enableImplicitPreRead();
    prepareNonKeyColumns(0, 0).forEach(putRecord1::value);
    Delete deleteRecord2 = prepareDelete(1, 0, namespace1, TABLE_1);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.mutate(Arrays.asList(putRecord1.build(), deleteRecord2));
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result1 = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = another.get(prepareGet(1, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertResult(0, 0, result1);
    assertThat(result2.isPresent()).isFalse();
  }

  @Test
  public void mutateAndCommit_WithMultipleSubTransactions_ShouldMutateRecordsProperly()
      throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insertForTable1 =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(1, 1).forEach(insertForTable1::value);
    Insert insertForTable2 = prepareInsert(1, 0, namespace2, TABLE_2);

    insert(insertForTable1.build(), insertForTable2);

    PutBuilder.Buildable putForTable1 =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).enableImplicitPreRead();
    prepareNonKeyColumns(0, 0).forEach(putForTable1::value);
    Delete deleteForTable2 = prepareDelete(1, 0, namespace2, TABLE_2);

    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    // Act
    transaction1.put(putForTable1.build());
    transaction2.delete(deleteForTable2);

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
    Optional<Result> result1 = another1.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = another2.get(prepareGet(1, 0, namespace2, TABLE_2));
    another1.prepare();
    another2.prepare();
    another1.validate();
    another2.validate();
    another1.commit();
    another2.commit();

    assertResult(0, 0, result1);
    assertThat(result2.isPresent()).isFalse();
  }

  @Test
  public void mutateAndRollback_WithMultipleSubTransactions_ShouldRollbackRecordsProperly()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecords(manager2, namespace2, TABLE_2);

    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE - 100)
            .enableImplicitPreRead()
            .build();
    Delete delete = prepareDelete(1, 0, namespace2, TABLE_2);

    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    // Act
    transaction1.put(put);
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
    Optional<Result> result1 = another1.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = another2.get(prepareGet(1, 0, namespace2, TABLE_2));
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
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(ACCOUNT_ID, i))
                                    .column(IntColumn.of(ACCOUNT_TYPE, j))
                                    .columns(prepareNonKeyColumns(i, j))
                                    .build())));
    TestUtils.assertResultsContainsExactlyInAnyOrder(results, expectedResults);
  }

  @Test
  public void scan_ScanAllGivenWithLimit_ShouldReturnLimitedAmountOfRecords()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction putTransaction = manager1.begin();
    insert(
        prepareInsert(1, 1, namespace1, TABLE_1),
        prepareInsert(1, 2, namespace1, TABLE_1),
        prepareInsert(2, 1, namespace1, TABLE_1),
        prepareInsert(3, 0, namespace1, TABLE_1));
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
                .column(IntColumn.of(ACCOUNT_ID, 1))
                .column(IntColumn.of(ACCOUNT_TYPE, 1))
                .columns(prepareNonKeyColumns(1, 1))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 1))
                .column(IntColumn.of(ACCOUNT_TYPE, 2))
                .columns(prepareNonKeyColumns(1, 2))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 2))
                .column(IntColumn.of(ACCOUNT_TYPE, 1))
                .columns(prepareNonKeyColumns(2, 1))
                .build(),
            new ExpectedResultBuilder()
                .column(IntColumn.of(ACCOUNT_ID, 3))
                .column(IntColumn.of(ACCOUNT_TYPE, 0))
                .columns(prepareNonKeyColumns(3, 0))
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
                        j ->
                            expectedResults.add(
                                new ExpectedResultBuilder()
                                    .column(IntColumn.of(ACCOUNT_TYPE, j))
                                    .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
                                    .build())));
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
            .column(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .column(IntColumn.ofNull(SOME_COLUMN))
            .build();
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, Collections.singletonList(expectedResult));
  }

  @Test
  public void getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareScan(0, 0, 2, namespace1, TABLE_1);

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);

    Optional<Result> result1 = scanner.one();
    assertThat(result1).isPresent();
    assertResult(0, 0, result1.get());

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isPresent();
    assertResult(0, 1, result2.get());

    scanner.close();

    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  @Test
  public void resume_WithBeginningTransaction_ShouldReturnBegunTransaction()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    TwoPhaseCommitTransaction resumed = manager1.resume(transaction.getId());

    // Assert
    assertThat(resumed.getId()).isEqualTo(transaction.getId());

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void resume_WithoutBeginningTransaction_ShouldThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager1.resume("txId"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_WithBeginningAndCommittingTransaction_ShouldThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.prepare();
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager1.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      resume_WithBeginningAndRollingBackTransaction_ShouldThrowTransactionNotFoundException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager1.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void get_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Get get =
          Get.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.get(get);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void scan_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Scan scan = Scan.newBuilder().table(TABLE_1).all().build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.scan(scan);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void put_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Put put =
          Put.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .enableImplicitPreRead()
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.put(put);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void insert_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Insert insert =
          Insert.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.insert(insert);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void upsert_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Upsert upsert =
          Upsert.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 5))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.upsert(upsert);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void update_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Update update =
          Update.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.update(update);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void delete_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Delete delete =
          Delete.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.delete(delete);
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void mutate_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Mutation putAsMutation1 =
          Put.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .enableImplicitPreRead()
              .build();
      Mutation deleteAsMutation2 =
          Delete.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.mutate(ImmutableList.of(putAsMutation1, deleteAsMutation2));
                tx.commit();
              })
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void put_withPutIfWithVerifiedCondition_shouldPutProperly() throws TransactionException {
    // Arrange
    PutBuilder.Buildable initialData = Put.newBuilder(preparePut(2, 3, namespace1, TABLE_1));
    prepareNonKeyColumns(1, 2).forEach(initialData::value);
    put(initialData.build());

    List<ConditionalExpression> conditions =
        Lists.newArrayList(
            ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE),
            ConditionBuilder.column(SOME_COLUMN).isNotNullInt(),
            ConditionBuilder.column(BOOLEAN_COL).isNotEqualToBoolean(true),
            ConditionBuilder.column(BIGINT_COL).isLessThanBigInt(BigIntColumn.MAX_VALUE),
            ConditionBuilder.column(FLOAT_COL).isEqualToFloat(0.12F),
            ConditionBuilder.column(DOUBLE_COL).isGreaterThanDouble(-10),
            ConditionBuilder.column(TEXT_COL).isNotEqualToText("foo"),
            ConditionBuilder.column(DATE_COL).isLessThanDate(LocalDate.of(3000, 1, 1)),
            ConditionBuilder.column(TIME_COL).isNotNullTime(),
            ConditionBuilder.column(TIMESTAMPTZ_COL).isNotEqualToTimestampTZ(Instant.EPOCH));
    if (isTimestampTypeSupported()) {
      conditions.add(
          ConditionBuilder.column(TIMESTAMP_COL)
              .isGreaterThanOrEqualToTimestamp(LocalDateTime.of(1970, 1, 1, 1, 2)));
    }
    PutBuilder.Buildable putIf = Put.newBuilder(initialData.build()).clearValues();
    prepareNonKeyColumns(2, 3).forEach(putIf::value);
    putIf.condition(ConditionBuilder.putIf(conditions)).enableImplicitPreRead().build();

    // Act
    put(putIf.build());

    // Assert
    Optional<Result> optResult = get(prepareGet(2, 3, namespace1, TABLE_1));
    assertResult(2, 3, optResult);
  }

  @Test
  public void put_withPutIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put initialData =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    put(initialData);

    Put putIf =
        Put.newBuilder(initialData)
            .intValue(BALANCE, 2)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .enableImplicitPreRead()
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(putIf)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void put_withPutIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put putIf =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(BALANCE).isNullText()).build())
            .enableImplicitPreRead()
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(putIf)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> result = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put putIfExists =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(putIfExists)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> result = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfExistsWhenRecordExists_shouldPutProperly() throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0, namespace1, TABLE_1);
    put(put);
    Put putIfExists =
        Put.newBuilder(put)
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    // Act
    put(putIfExists);

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
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
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .condition(ConditionBuilder.putIfNotExists())
            .enableImplicitPreRead()
            .build();

    // Act
    put(putIfNotExists);

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordExists_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0, namespace1, TABLE_1);
    put(put);
    Put putIfNotExists =
        Put.newBuilder(put)
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfNotExists())
            .enableImplicitPreRead()
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(putIfNotExists)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
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
    PutBuilder.Buildable initialData = Put.newBuilder(preparePut(1, 2, namespace1, TABLE_1));
    prepareNonKeyColumns(1, 2).forEach(initialData::value);
    put(initialData.build());

    List<ConditionalExpression> conditions =
        Lists.newArrayList(
            ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE),
            ConditionBuilder.column(SOME_COLUMN).isNotNullInt(),
            ConditionBuilder.column(BOOLEAN_COL).isNotEqualToBoolean(true),
            ConditionBuilder.column(BIGINT_COL).isLessThanBigInt(BigIntColumn.MAX_VALUE),
            ConditionBuilder.column(FLOAT_COL).isEqualToFloat(0.12F),
            ConditionBuilder.column(DOUBLE_COL).isGreaterThanDouble(-10),
            ConditionBuilder.column(TEXT_COL).isNotEqualToText("foo"),
            ConditionBuilder.column(DATE_COL).isLessThanDate(LocalDate.of(3000, 1, 1)),
            ConditionBuilder.column(TIME_COL).isNotNullTime(),
            ConditionBuilder.column(TIMESTAMPTZ_COL).isNotEqualToTimestampTZ(Instant.EPOCH));

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(1, 2, namespace1, TABLE_1))
            .condition(ConditionBuilder.deleteIf(conditions))
            .build();

    // Act
    delete(deleteIf);

    // Assert
    Optional<Result> optResult = get(prepareGet(1, 2, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isFalse();
  }

  @Test
  public void delete_withDeleteIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).build();
    put(initialData);

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0, namespace1, TABLE_1))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> delete(deleteIf)).isInstanceOf(UnsatisfiedConditionException.class);

    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordsExists_shouldDeleteProperly()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).build();
    put(initialData);

    Delete deleteIfExists =
        Delete.newBuilder(prepareDelete(0, 0, namespace1, TABLE_1))
            .condition(ConditionBuilder.deleteIfExists())
            .build();

    // Act
    delete(deleteIfExists);

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isFalse();
  }

  @Test
  public void
      delete_withDeleteIfExistsWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException() {
    // Arrange
    Delete deleteIfExists =
        Delete.newBuilder(prepareDelete(0, 0, namespace1, TABLE_1))
            .condition(ConditionBuilder.deleteIfExists())
            .build();

    // Act Assert
    assertThatThrownBy(() -> delete(deleteIfExists))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void insertAndCommit_InsertGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    transaction.insert(prepareInsert(0, 0, namespace1, TABLE_1));
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
    assertResult(0, 0, result);
  }

  @Test
  public void
      insertAndCommit_InsertGivenForExisting_ShouldThrowCrudConflictExceptionOrPreparationConflictException()
          throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act Assert
    int expected = INITIAL_BALANCE + 100;
    Insert insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    try {
      transaction.insert(insert);
      transaction.prepare();
      transaction.validate();
      transaction.commit();
      fail("Should have thrown CrudConflictException or PreparationConflictException");
    } catch (CrudConflictException | PreparationConflictException ignored) {
      // Expected
    }
    transaction.rollback();
  }

  @Test
  public void upsertAndCommit_UpsertGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    UpsertBuilder.Buildable upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(0, 0).forEach(upsert::value);
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    transaction.upsert(upsert.build());
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
    assertResult(0, 0, result);
  }

  @Test
  public void upsertAndCommit_UpsertGivenForExisting_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(1, 1).forEach(insert::value);
    insert(insert.build());
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    UpsertBuilder.Buildable upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(0, 0).forEach(upsert::value);
    transaction.upsert(upsert.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertThat(actual.isPresent()).isTrue();
    assertResult(0, 0, actual);
  }

  @Test
  public void updateAndCommit_UpdateGivenForNonExisting_ShouldDoNothing()
      throws TransactionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertThat(actual).isEmpty();
  }

  @Test
  public void
      updateAndCommit_UpdateWithUpdateIfExistsGivenForNonExisting_ShouldThrowUnsatisfiedConditionException()
          throws TransactionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.updateIfExists())
            .build();
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);
    transaction.rollback();
  }

  @Test
  public void updateAndCommit_UpdateGivenForExisting_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(1, 1).forEach(insert::value);
    insert(insert.build());
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    UpdateBuilder.Buildable update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(0, 0).forEach(update::value);
    transaction.update(update.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertResult(0, 0, actual);
  }

  @Test
  public void updateAndCommit_UpdateWithUpdateIfExistsGivenForExisting_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0));
    prepareNonKeyColumns(1, 1).forEach(insert::value);
    insert(insert.build());
    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    UpdateBuilder.Buildable update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .condition(ConditionBuilder.updateIfExists());
    prepareNonKeyColumns(0, 0).forEach(update::value);
    transaction.update(update.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.start();
    Optional<Result> actual = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.validate();
    another.commit();

    assertResult(0, 0, actual);
  }

  @Test
  public void update_withUpdateIfWithVerifiedCondition_shouldUpdateProperly()
      throws TransactionException {
    // Arrange
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 3));
    prepareNonKeyColumns(1, 2).forEach(insert::value);
    insert(insert.build());

    List<ConditionalExpression> conditions =
        Lists.newArrayList(
            ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE),
            ConditionBuilder.column(SOME_COLUMN).isNotNullInt(),
            ConditionBuilder.column(BOOLEAN_COL).isNotEqualToBoolean(true),
            ConditionBuilder.column(BIGINT_COL).isLessThanBigInt(BigIntColumn.MAX_VALUE),
            ConditionBuilder.column(FLOAT_COL).isEqualToFloat(0.12F),
            ConditionBuilder.column(DOUBLE_COL).isGreaterThanDouble(-10),
            ConditionBuilder.column(TEXT_COL).isNotEqualToText("foo"),
            ConditionBuilder.column(DATE_COL).isLessThanDate(LocalDate.of(3000, 1, 1)),
            ConditionBuilder.column(TIME_COL).isNotNullTime(),
            ConditionBuilder.column(TIMESTAMPTZ_COL).isNotEqualToTimestampTZ(Instant.EPOCH));
    if (isTimestampTypeSupported()) {
      conditions.add(
          ConditionBuilder.column(TIMESTAMP_COL)
              .isGreaterThanOrEqualToTimestamp(LocalDateTime.of(1970, 1, 1, 1, 2)));
    }
    UpdateBuilder.Buildable updateIf =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 3))
            .condition(ConditionBuilder.updateIf(conditions));
    prepareNonKeyColumns(2, 3).forEach(updateIf::value);

    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act
    transaction.update(updateIf.build());
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    Optional<Result> actual = get(prepareGet(2, 3, namespace1, TABLE_1));
    assertResult(2, 3, actual);
  }

  @Test
  public void update_withUpdateIfWhenRecordDoesNotExist_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Update updateIf =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column(BALANCE).isNullInt()).build())
            .build();

    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act Assert
    assertThatThrownBy(() -> transaction.update(updateIf))
        .isInstanceOf(UnsatisfiedConditionException.class);
    transaction.rollback();

    Optional<Result> result = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void update_withUpdateIfWithNonVerifiedCondition_shouldThrowUnsatisfiedConditionException()
      throws TransactionException {
    // Arrange
    Put initialData =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    put(initialData);

    Update updateIf =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 2)
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    TwoPhaseCommitTransaction transaction = manager1.start();

    // Act Assert
    assertThatThrownBy(() -> transaction.update(updateIf))
        .isInstanceOf(UnsatisfiedConditionException.class);
    transaction.rollback();

    Optional<Result> optResult = get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void manager_get_GetGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = manager1.get(get);

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().getInt(SOME_COLUMN)).isEqualTo(0);
  }

  @Test
  public void manager_scan_ScanGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1);

    // Act
    List<Result> results = manager1.scan(scan);

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
  public void manager_getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1);

    // Act Assert
    TransactionManagerCrudOperable.Scanner scanner = manager1.getScanner(scan);

    Optional<Result> result1 = scanner.one();
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result1.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result1.get().getInt(SOME_COLUMN)).isEqualTo(0);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isPresent();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(result2.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result2.get().getInt(SOME_COLUMN)).isEqualTo(1);

    Optional<Result> result3 = scanner.one();
    assertThat(result3).isPresent();
    assertThat(result3.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result3.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(result3.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result3.get().getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(scanner.one()).isNotPresent();

    scanner.close();
  }

  @Test
  public void manager_put_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager1.put(put);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager1.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void manager_put_PutGivenForExisting_ShouldUpdateRecord() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    manager1.put(put);

    // Assert
    Optional<Result> actual = manager1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void manager_insert_InsertGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Insert insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager1.insert(insert);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager1.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void manager_insert_InsertGivenForExisting_ShouldThrowCrudConflictException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);

    int expected = INITIAL_BALANCE + 100;
    Insert insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act Assert
    assertThatThrownBy(() -> manager1.insert(insert)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void manager_upsert_UpsertGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager1.upsert(upsert);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager1.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void manager_upsert_UpsertGivenForExisting_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();
    manager1.upsert(upsert);

    // Assert
    Optional<Result> actual = manager1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void manager_update_UpdateGivenForNonExisting_ShouldDoNothing()
      throws TransactionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();

    // Act
    assertThatCode(() -> manager1.update(update)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = manager1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual).isEmpty();
  }

  @Test
  public void manager_update_UpdateGivenForExisting_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();
    manager1.update(update);

    // Assert
    Optional<Result> actual = manager1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void manager_delete_DeleteGivenForExisting_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);

    // Act
    manager1.delete(delete);

    // Assert
    Optional<Result> result = manager1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void manager_get_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Get get =
          Get.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.get(get))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_scan_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Scan scan = Scan.newBuilder().table(TABLE_1).all().build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.scan(scan))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_put_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Put put =
          Put.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .enableImplicitPreRead()
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.put(put))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_insert_DefaultNamespaceGiven_ShouldWorkProperly()
      throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Insert insert =
          Insert.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.insert(insert))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_upsert_DefaultNamespaceGiven_ShouldWorkProperly()
      throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Upsert upsert =
          Upsert.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 5))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.upsert(upsert))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_update_DefaultNamespaceGiven_ShouldWorkProperly()
      throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Update update =
          Update.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.update(update))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_delete_DefaultNamespaceGiven_ShouldWorkProperly()
      throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Delete delete =
          Delete.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .build();

      // Act Assert
      Assertions.assertThatCode(() -> managerWithDefaultNamespace.delete(delete))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void manager_mutate_DefaultNamespaceGiven_ShouldWorkProperly()
      throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    try (DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange
      populateRecords(manager1, namespace1, TABLE_1);
      Mutation putAsMutation1 =
          Put.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
              .intValue(BALANCE, 300)
              .enableImplicitPreRead()
              .build();
      Mutation deleteAsMutation2 =
          Delete.newBuilder()
              .table(TABLE_1)
              .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
              .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () ->
                  managerWithDefaultNamespace.mutate(
                      ImmutableList.of(putAsMutation1, deleteAsMutation2)))
          .doesNotThrowAnyException();
    }
  }

  private Optional<Result> get(Get get) throws TransactionException {
    TwoPhaseCommitTransaction tx = manager1.start();
    try {
      Optional<Result> result = tx.get(get);
      tx.prepare();
      tx.validate();
      tx.commit();
      return result;
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  private void put(Put put) throws TransactionException {
    TwoPhaseCommitTransaction tx = manager1.start();
    try {
      tx.put(put);
      tx.prepare();
      tx.validate();
      tx.commit();
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  protected void insert(Insert... insert) throws TransactionException {
    TwoPhaseCommitTransaction tx = manager1.start();
    try {
      for (Insert i : insert) {
        tx.insert(i);
      }
      tx.prepare();
      tx.validate();
      tx.commit();
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  private void delete(Delete delete) throws TransactionException {
    TwoPhaseCommitTransaction tx = manager1.start();
    try {
      tx.delete(delete);
      tx.prepare();
      tx.validate();
      tx.commit();
    } catch (TransactionException e) {
      tx.rollback();
      throw e;
    }
  }

  protected void populateRecords(
      TwoPhaseCommitTransactionManager manager1, String namespaceName, String tableName)
      throws TransactionException {
    TwoPhaseCommitTransaction transaction = manager1.begin();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        InsertBuilder.Buildable insert =
            Insert.newBuilder()
                .namespace(namespaceName)
                .table(tableName)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey);
        prepareNonKeyColumns(i, j).forEach(insert::value);
        try {
          transaction.insert(insert.build());
        } catch (CrudException e) {
          throw new RuntimeException(e);
        }
      }
    }
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  protected void populateSingleRecord(String namespaceName, String tableName)
      throws TransactionException {
    TwoPhaseCommitTransaction transaction = manager1.begin();

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    Put put =
        Put.newBuilder()
            .namespace(namespaceName)
            .table(tableName)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();

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

  protected List<Get> prepareGets(String namespaceName, String tableName) {
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

  protected ScanAll prepareScanAll(String namespaceName, String tableName) {
    return new ScanAll()
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected Put preparePut(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected Insert prepareInsert(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, type);
    InsertBuilder.Buildable insert =
        Insert.newBuilder()
            .namespace(namespaceName)
            .table(tableName)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey);
    prepareNonKeyColumns(id, type).forEach(insert::value);

    return insert.build();
  }

  protected List<Put> preparePuts(String namespaceName, String tableName) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> puts.add(preparePut(i, j, namespaceName, tableName))));

    return puts;
  }

  protected Delete prepareDelete(int id, int type, String namespaceName, String tableName) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespaceName)
        .forTable(tableName)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }

  protected boolean isTimestampTypeSupported() {
    return true;
  }

  private void assertResult(int accountId, int accountType, Optional<Result> optResult) {
    assertResult(accountId, accountType, optResult.orElse(null));
  }

  private void assertResult(int accountId, int accountType, Result result) {
    String resultErrorMessage =
        String.format("Result { accountId=%d, accountType=%d }", accountId, accountType);

    assertThat(result).describedAs(resultErrorMessage + " is null").isNotNull();

    List<String> columns =
        Lists.newArrayList(
            ACCOUNT_ID,
            ACCOUNT_TYPE,
            BALANCE,
            SOME_COLUMN,
            BOOLEAN_COL,
            BIGINT_COL,
            FLOAT_COL,
            DOUBLE_COL,
            TEXT_COL,
            BLOB_COL,
            DATE_COL,
            TIME_COL,
            TIMESTAMPTZ_COL);
    if (isTimestampTypeSupported()) {
      columns.add(TIMESTAMP_COL);
    }
    assertThat(result.getContainedColumnNames())
        .describedAs("Columns are missing. %s", resultErrorMessage)
        .containsExactlyInAnyOrderElementsOf(columns);
    for (String column : columns) {
      assertThat(result.isNull(column))
          .describedAs("Column {%s} is null. %s", column, resultErrorMessage)
          .isFalse();
    }

    String columnMessage = "Unexpected value for column {%s}. %s";
    assertThat(result.getInt(ACCOUNT_ID))
        .describedAs(columnMessage, ACCOUNT_ID, resultErrorMessage)
        .isEqualTo(accountId);
    assertThat(result.getInt(ACCOUNT_TYPE))
        .describedAs(columnMessage, ACCOUNT_TYPE, resultErrorMessage)
        .isEqualTo(accountType);
    assertThat(result.getInt(BALANCE))
        .describedAs(columnMessage, BALANCE, resultErrorMessage)
        .isEqualTo(INITIAL_BALANCE);
    assertThat(result.getInt(SOME_COLUMN))
        .describedAs(columnMessage, SOME_COLUMN, resultErrorMessage)
        .isEqualTo(accountId * accountType);
    assertThat(result.getBoolean(BOOLEAN_COL))
        .describedAs(columnMessage, BOOLEAN_COL, resultErrorMessage)
        .isEqualTo(accountId % 2 == 0);
    assertThat(result.getBigInt(BIGINT_COL))
        .describedAs(columnMessage, BIGINT_COL, resultErrorMessage)
        .isEqualTo((long) Math.pow(accountId, accountType));
    assertThat(result.getFloat(FLOAT_COL))
        .describedAs(columnMessage, FLOAT_COL, resultErrorMessage)
        .isEqualTo(Float.parseFloat("0." + accountId + accountType));
    assertThat(result.getDouble(DOUBLE_COL))
        .describedAs(columnMessage, DOUBLE_COL, resultErrorMessage)
        .isEqualTo(Float.parseFloat("10." + accountId + accountType));
    assertThat(result.getText(TEXT_COL))
        .describedAs(columnMessage, TEXT_COL, resultErrorMessage)
        .isEqualTo(accountId + "" + accountType);
    assertThat(result.getBlobAsBytes(BLOB_COL))
        .describedAs(columnMessage, BLOB_COL, resultErrorMessage)
        .isEqualTo((accountId + "" + accountType).getBytes(StandardCharsets.UTF_8));
    assertThat(result.getDate(DATE_COL))
        .describedAs(columnMessage, DATE_COL, resultErrorMessage)
        .isEqualTo(LocalDate.ofEpochDay(accountId + accountType));
    assertThat(result.getTime(TIME_COL))
        .describedAs(columnMessage, TIME_COL, resultErrorMessage)
        .isEqualTo(LocalTime.of(accountId, accountType));
    assertThat(result.getTimestampTZ(TIMESTAMPTZ_COL))
        .describedAs(columnMessage, TIMESTAMPTZ_COL, resultErrorMessage)
        .isEqualTo(LocalDateTime.of(1970, 1, 1, accountId, accountType).toInstant(ZoneOffset.UTC));
    if (isTimestampTypeSupported()) {
      assertThat(result.getTimestamp(TIMESTAMP_COL))
          .describedAs(columnMessage, TIMESTAMP_COL, resultErrorMessage)
          .isEqualTo(LocalDateTime.of(1970, 1, 1, accountId, accountType));
    }
  }

  protected List<Column<?>> prepareNonKeyColumns(int accountId, int accountType) {
    ImmutableList.Builder<Column<?>> columns =
        new ImmutableList.Builder<Column<?>>()
            .add(
                IntColumn.of(BALANCE, INITIAL_BALANCE),
                IntColumn.of(SOME_COLUMN, accountId * accountType),
                BooleanColumn.of(BOOLEAN_COL, accountId % 2 == 0),
                BigIntColumn.of(BIGINT_COL, (long) Math.pow(accountId, accountType)),
                FloatColumn.of(FLOAT_COL, Float.parseFloat("0." + accountId + accountType)),
                DoubleColumn.of(DOUBLE_COL, Float.parseFloat("10." + accountId + accountType)),
                TextColumn.of(TEXT_COL, accountId + "" + accountType),
                BlobColumn.of(
                    BLOB_COL, (accountId + "" + accountType).getBytes(StandardCharsets.UTF_8)),
                DateColumn.of(DATE_COL, LocalDate.ofEpochDay(accountId + accountType)),
                TimeColumn.of(TIME_COL, LocalTime.of(accountId, accountType)),
                TimestampTZColumn.of(
                    TIMESTAMPTZ_COL,
                    LocalDateTime.of(1970, 1, 1, accountId, accountType)
                        .toInstant(ZoneOffset.UTC)));
    if (isTimestampTypeSupported()) {
      columns.add(
          TimestampColumn.of(TIMESTAMP_COL, LocalDateTime.of(1970, 1, 1, accountId, accountType)));
    }
    return columns.build();
  }
}
