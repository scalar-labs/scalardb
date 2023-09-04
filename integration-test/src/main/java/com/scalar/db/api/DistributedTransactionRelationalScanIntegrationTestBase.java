package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.TestUtils;
import com.scalar.db.util.TestUtils.ExpectedResult;
import com.scalar.db.util.TestUtils.ExpectedResult.ExpectedResultBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedTransactionRelationalScanIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "int_rscan_tx_test_";
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
  public void scan_RelationalScanGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareRelationalScan(1, 0, 2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, prepareExpectedResults(1, 0, 2, true));
  }

  @Test
  public void scan_RelationalScanWithProjectionsGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(1, 0, 2))
            .projection(ACCOUNT_ID)
            .projection(ACCOUNT_TYPE)
            .projection(BALANCE)
            .build();

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
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, prepareExpectedResults(1, 0, 2, false));
  }

  @Test
  public void scan_RelationalScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(1, 0, 2))
            .ordering(Ordering.desc(ACCOUNT_TYPE))
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(12);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(11);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(1);

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(10);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(2))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(2).getInt(SOME_COLUMN)).isEqualTo(0);
  }

  @Test
  public void scan_RelationalScanWithLimitGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(1, 0, 2))
            .ordering(Ordering.asc(ACCOUNT_TYPE))
            .limit(2)
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(10);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(0).getInt(SOME_COLUMN)).isEqualTo(0);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(11);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(SOME_COLUMN)).isEqualTo(1);
  }

  @Test
  public void scan_RelationalScanGivenForNonExisting_ShouldReturnEmpty()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareRelationalScan(0, 4, 6);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void
      scan_RelationalScanWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    populateSingleRecord();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(0, 0, 0))
            .projections(Arrays.asList(BALANCE, SOME_COLUMN))
            .build();

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
  public void operation_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace);
    final DistributedTransactionManager managerWithDefaultNamespace =
        TransactionFactory.create(properties).getTransactionManager();
    try {
      // Arrange
      populateRecords();
      Scan scan = Scan.newBuilder().table(TABLE).all().build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                DistributedTransaction tx = managerWithDefaultNamespace.start();
                tx.scan(scan);
                tx.commit();
              })
          .doesNotThrowAnyException();
    } finally {
      managerWithDefaultNamespace.close();
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
                          Put put =
                              Put.newBuilder()
                                  .namespace(namespace)
                                  .table(TABLE)
                                  .partitionKey(Key.ofInt(ACCOUNT_ID, i * 10 + j))
                                  .value(IntColumn.of(ACCOUNT_TYPE, j))
                                  .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
                                  .value(IntColumn.of(SOME_COLUMN, i * j))
                                  .build();
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
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .value(IntColumn.of(ACCOUNT_TYPE, 0))
            .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .build();
    DistributedTransaction transaction = manager.start();
    transaction.put(put);
    transaction.commit();
  }

  protected Put preparePut(int id, int type) throws TransactionException {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .value(IntColumn.of(ACCOUNT_TYPE, type))
        .build();
  }

  protected Scan prepareRelationalScan(int offset, int fromType, int toType) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .all()
        .where(ConditionBuilder.column(ACCOUNT_ID).isGreaterThanOrEqualToInt(offset * 10))
        .and(ConditionBuilder.column(ACCOUNT_ID).isLessThanInt((offset + 1) * 10))
        .and(ConditionBuilder.column(ACCOUNT_TYPE).isGreaterThanOrEqualToInt(fromType))
        .and(ConditionBuilder.column(ACCOUNT_TYPE).isLessThanOrEqualToInt(toType))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  protected int getBalance(Result result) {
    Map<String, Column<?>> columns = result.getColumns();
    assertThat(columns.containsKey(BALANCE)).isTrue();
    return columns.get(BALANCE).getIntValue();
  }

  private List<ExpectedResult> prepareExpectedResults(
      int offset, int fromType, int toType, boolean withSomeColumn) {
    List<ExpectedResult> expectedResults = new ArrayList<>();
    IntStream.range(fromType, toType + 1)
        .forEach(
            j -> {
              ExpectedResultBuilder builder =
                  new ExpectedResultBuilder()
                      .column(IntColumn.of(ACCOUNT_ID, offset * 10 + j))
                      .column(IntColumn.of(ACCOUNT_TYPE, j))
                      .column(IntColumn.of(BALANCE, INITIAL_BALANCE));
              if (withSomeColumn) {
                builder.column(IntColumn.of(SOME_COLUMN, offset * j));
              }
              expectedResults.add(builder.build());
            });
    return expectedResults;
  }
}
