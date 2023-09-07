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
public abstract class TwoPhaseCommitTransactionRelationalScanIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "int_rscan_2pc_test_";
  protected static final String TABLE_1 = "test_table1";
  protected static final String TABLE_2 = "test_table2";
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
  public void setUp() throws Exception {
    admin1.truncateTable(namespace1, TABLE_1);
    admin1.truncateCoordinatorTables();
    admin2.truncateTable(namespace2, TABLE_2);
  }

  @AfterAll
  public void afterAll() throws Exception {
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
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecords() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, prepareExpectedResults(1, 0, 2, true));
  }

  @Test
  public void scan_ScanWithProjectionsGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2))
            .projection(ACCOUNT_ID)
            .projection(ACCOUNT_TYPE)
            .projection(BALANCE)
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, prepareExpectedResults(1, 0, 2, false));
  }

  @Test
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2))
            .ordering(Ordering.desc(ACCOUNT_TYPE))
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
  public void scan_ScanWithLimitGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan =
        Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2))
            .ordering(Ordering.asc(ACCOUNT_TYPE))
            .limit(2)
            .build();

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
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
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Scan scan = prepareRelationalScan(namespace1, TABLE_1, 0, 4, 4);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void operation_DefaultNamespaceGiven_ShouldWorkProperly() throws TransactionException {
    Properties properties = getProperties1(getTestName());
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, namespace1);
    final TwoPhaseCommitTransactionManager manager1WithDefaultNamespace =
        TransactionFactory.create(properties).getTwoPhaseCommitTransactionManager();
    try {
      // Arrange
      populateRecords(manager1WithDefaultNamespace, namespace1, TABLE_1);
      Scan scan = Scan.newBuilder().table(TABLE_1).all().build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                TwoPhaseCommitTransaction tx = manager1WithDefaultNamespace.start();
                tx.scan(scan);
                tx.prepare();
                tx.validate();
                tx.commit();
              })
          .doesNotThrowAnyException();
    } finally {
      manager1WithDefaultNamespace.close();
    }
  }

  protected void populateRecords(
      TwoPhaseCommitTransactionManager manager, String namespaceName, String tableName)
      throws TransactionException {
    TwoPhaseCommitTransaction transaction = manager.begin();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j -> {
                          Put put =
                              Put.newBuilder()
                                  .namespace(namespaceName)
                                  .table(tableName)
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
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  protected Put preparePut(String namespaceName, String tableNam, int id, int type)
      throws TransactionException {
    return Put.newBuilder()
        .namespace(namespaceName)
        .table(tableNam)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .value(IntColumn.of(ACCOUNT_TYPE, type))
        .build();
  }

  protected Scan prepareRelationalScan(
      String namespaceName, String tableName, int offset, int fromType, int toType) {
    return Scan.newBuilder()
        .namespace(namespaceName)
        .table(tableName)
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
