package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
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
public abstract class DistributedTransactionCrossPartitionScanIntegrationTestBase {

  protected static final String NAMESPACE_BASE_NAME = "int_cpscan_tx_test_";
  protected static final String TABLE = "test_table";
  protected static final String TABLE_WITH_TEXT = "test_table_with_text";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String ACCOUNT_NAME = "account_name";
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
  protected static final TableMetadata TABLE_METADATA_WITH_TEXT =
      TableMetadata.newBuilder()
          .addColumn(ACCOUNT_ID, DataType.INT)
          .addColumn(ACCOUNT_NAME, DataType.TEXT)
          .addColumn(BALANCE, DataType.INT)
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
    admin.createTable(namespace, TABLE_WITH_TEXT, TABLE_METADATA_WITH_TEXT, true, options);
    admin.createCoordinatorTables(true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    admin.truncateTable(namespace, TABLE);
    admin.truncateTable(namespace, TABLE_WITH_TEXT);
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
    admin.dropTable(namespace, TABLE_WITH_TEXT);
    admin.dropNamespace(namespace);
    admin.dropCoordinatorTables();
  }

  @Test
  public void scan_CrossPartitionScanGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareCrossPartitionScan(1, 0, 2);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    TestUtils.assertResultsContainsExactlyInAnyOrder(
        results, prepareExpectedResults(1, 0, 2, true));
  }

  @Test
  public void scan_CrossPartitionScanWithProjectionsGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan(1, 0, 2))
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
  public void scan_CrossPartitionScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan(1, 0, 2))
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
  public void scan_CrossPartitionScanWithLimitGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan(1, 0, 2))
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
  public void scan_CrossPartitionScanGivenForNonExisting_ShouldReturnEmpty()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Scan scan = prepareCrossPartitionScan(0, 4, 6);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void
      scan_CrossPartitionScanWithProjectionsGivenOnNonPrimaryKeyColumnsForCommittedRecord_ShouldReturnOnlyProjectedColumns()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    populateSingleRecord();
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan(0, 0, 0))
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
  public void scan_CrossPartitionScanWithLikeGivenForCommittedRecord_ShouldReturnRecords()
      throws TransactionException {
    // Arrange
    populateRecordsForLike();
    DistributedTransaction transaction = manager.start();
    Scan scan1 = prepareCrossPartitionScanWithLike(true, "%scalar[$]");
    Scan scan2 = prepareCrossPartitionScanWithLike(true, "+_scalar[$]", "+");
    Scan scan3 = prepareCrossPartitionScanWithLike(false, "\\_scalar[$]");

    // Act
    List<Result> actual1 = transaction.scan(scan1);
    List<Result> actual2 = transaction.scan(scan2);
    List<Result> actual3 = transaction.scan(scan3);
    transaction.commit();

    // Assert
    assertScanResult(actual1, ImmutableList.of(1, 2, 3));
    assertScanResult(actual2, ImmutableList.of(3));
    assertScanResult(actual3, ImmutableList.of(1, 2));
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

  protected void populateRecordsForLike() throws TransactionException {
    DistributedTransaction transaction = manager.start();
    try {
      transaction.put(preparePut(1, "@scalar[$]"));
      transaction.put(preparePut(2, "@@scalar[$]"));
      transaction.put(preparePut(3, "_scalar[$]"));
    } catch (CrudException e) {
      throw new RuntimeException(e);
    }
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

  protected Put preparePut(int id, String name) throws TransactionException {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE_WITH_TEXT)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .value(TextColumn.of(ACCOUNT_NAME, name))
        .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
        .build();
  }

  protected Scan prepareCrossPartitionScan(int offset, int fromType, int toType) {
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

  protected Scan prepareCrossPartitionScanWithLike(boolean isLike, String pattern) {
    LikeExpression condition =
        isLike
            ? ConditionBuilder.column(ACCOUNT_NAME).isLikeText(pattern)
            : ConditionBuilder.column(ACCOUNT_NAME).isNotLikeText(pattern);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(TABLE_WITH_TEXT)
        .all()
        .where(condition)
        .ordering(Ordering.asc(ACCOUNT_ID))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  protected Scan prepareCrossPartitionScanWithLike(boolean isLike, String pattern, String escape) {
    LikeExpression condition =
        isLike
            ? ConditionBuilder.column(ACCOUNT_NAME).isLikeText(pattern, escape)
            : ConditionBuilder.column(ACCOUNT_NAME).isNotLikeText(pattern, escape);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(TABLE_WITH_TEXT)
        .all()
        .where(condition)
        .ordering(Ordering.asc(ACCOUNT_ID))
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

  private void assertScanResult(List<Result> actualResults, List<Integer> expected) {
    List<Integer> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      actual.add(actualResult.getInt(ACCOUNT_ID));
    }
    assertThat(actual).isEqualTo(expected);
  }
}
