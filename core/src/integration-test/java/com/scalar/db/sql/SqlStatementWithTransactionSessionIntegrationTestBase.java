package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.builder.StatementBuilder;
import com.scalar.db.util.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SqlStatementWithTransactionSessionIntegrationTestBase {

  private static final String TEST_NAME = "sql_stmt_tx";
  private static final String NAMESPACE = "integration_testing_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final String SOME_COLUMN = "some_column";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;

  private SqlStatementSessionFactory sqlStatementSessionFactory;
  private String namespace;

  @BeforeAll
  public void beforeAll() {
    DatabaseConfig config = TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME);
    namespace = getNamespace();
    createTables(config);
    sqlStatementSessionFactory =
        SqlStatementSessionFactory.builder().withProperties(config.getProperties()).build();
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTables(DatabaseConfig config) {
    Map<String, String> options = getCreateOptions();
    try (SqlStatementSessionFactory factory =
        SqlStatementSessionFactory.builder().withProperties(config.getProperties()).build()) {
      SqlStatementSession session = factory.getTransactionSession();
      session.execute(
          StatementBuilder.createNamespace(namespace).ifNotExists().withOptions(options).build());
      session.execute(
          StatementBuilder.createTable(namespace, TABLE)
              .ifNotExists()
              .withPartitionKey(ACCOUNT_ID, DataType.INT)
              .withClusteringKey(ACCOUNT_TYPE, DataType.INT)
              .withColumn(BALANCE, DataType.INT)
              .withColumn(SOME_COLUMN, DataType.INT)
              .withOptions(options)
              .build());
      session.execute(
          StatementBuilder.createIndex()
              .ifNotExists()
              .onTable(namespace, TABLE)
              .column(SOME_COLUMN)
              .withOptions(options)
              .build());
      session.execute(
          StatementBuilder.createCoordinatorTables().ifNotExist().withOptions(options).build());
    }
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() {
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();
    session.execute(StatementBuilder.truncateTable(namespace, TABLE).build());
    session.execute(StatementBuilder.truncateCoordinatorTables().build());
  }

  @AfterAll
  public void afterAll() {
    dropTables();
    sqlStatementSessionFactory.close();
  }

  private void dropTables() {
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();
    session.execute(StatementBuilder.dropTable(namespace, TABLE).build());
    session.execute(StatementBuilder.dropNamespace(namespace).build());
    session.execute(StatementBuilder.dropCoordinatorTables().build());
  }

  @Test
  public void execute_SelectStatementGivenForCommittedRecord_ShouldReturnRecord() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    // Assert
    Optional<Record> record = resultSet.one();
    assertThat(record.isPresent()).isTrue();
    assertThat(record.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(record.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record.get().getInt(SOME_COLUMN)).isEqualTo(0);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void execute_SelectStatementWithProjectionGivenForCommittedRecord_ShouldReturnRecord() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE)
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    // Assert
    Optional<Record> record = resultSet.one();
    assertThat(record.isPresent()).isTrue();
    assertThat(record.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(record.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record.get().contains(SOME_COLUMN)).isFalse();

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void
      execute_SelectStatementWithRangePredicatesGivenForCommittedRecord_ShouldReturnRecords() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(1)))
                .and(Predicate.column(ACCOUNT_TYPE).isGreaterThanOrEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isLessThanOrEqualTo(Value.ofInt(2)))
                .build());
    session.commit();

    // Assert
    Optional<Record> record1 = resultSet.one();
    assertThat(record1).isPresent();
    assertThat(record1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record1.get().getInt(SOME_COLUMN)).isEqualTo(0);

    Optional<Record> record2 = resultSet.one();
    assertThat(record2).isPresent();
    assertThat(record2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(record2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record2.get().getInt(SOME_COLUMN)).isEqualTo(1);

    Optional<Record> record3 = resultSet.one();
    assertThat(record3).isPresent();
    assertThat(record3.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record3.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(record3.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record3.get().getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void
      execute_SelectStatementWithProjectionsAndRangePredicatesGivenForCommittedRecord_ShouldReturnRecords() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE)
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(1)))
                .and(Predicate.column(ACCOUNT_TYPE).isGreaterThanOrEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isLessThanOrEqualTo(Value.ofInt(2)))
                .build());
    session.commit();

    // Assert
    Optional<Record> record1 = resultSet.one();
    assertThat(record1).isPresent();
    assertThat(record1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record1.get().contains(SOME_COLUMN)).isFalse();

    Optional<Record> record2 = resultSet.one();
    assertThat(record2).isPresent();
    assertThat(record2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(record2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record2.get().contains(SOME_COLUMN)).isFalse();

    Optional<Record> record3 = resultSet.one();
    assertThat(record3).isPresent();
    assertThat(record3.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record3.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(record3.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record3.get().contains(SOME_COLUMN)).isFalse();

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void
      execute_SelectStatementWithRangePredicatesAndClusteringOrderingGivenForCommittedRecord_ShouldReturnRecords() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(1)))
                .and(Predicate.column(ACCOUNT_TYPE).isGreaterThanOrEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isLessThanOrEqualTo(Value.ofInt(2)))
                .orderBy(ClusteringOrdering.column(ACCOUNT_TYPE).desc())
                .build());
    session.commit();

    // Assert
    Optional<Record> record1 = resultSet.one();
    assertThat(record1).isPresent();
    assertThat(record1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record1.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(record1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record1.get().getInt(SOME_COLUMN)).isEqualTo(2);

    Optional<Record> record2 = resultSet.one();
    assertThat(record2).isPresent();
    assertThat(record2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(record2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record2.get().getInt(SOME_COLUMN)).isEqualTo(1);

    Optional<Record> record3 = resultSet.one();
    assertThat(record3).isPresent();
    assertThat(record3.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record3.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record3.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record3.get().getInt(SOME_COLUMN)).isEqualTo(0);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void
      execute_SelectStatementWithRangePredicatesAndLimitGivenForCommittedRecord_ShouldReturnRecords() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(1)))
                .and(Predicate.column(ACCOUNT_TYPE).isGreaterThanOrEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isLessThanOrEqualTo(Value.ofInt(2)))
                .limit(2)
                .build());
    session.commit();

    // Assert
    Optional<Record> record1 = resultSet.one();
    assertThat(record1).isPresent();
    assertThat(record1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(record1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record1.get().getInt(SOME_COLUMN)).isEqualTo(0);

    Optional<Record> record2 = resultSet.one();
    assertThat(record2).isPresent();
    assertThat(record2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(record2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record2.get().getInt(SOME_COLUMN)).isEqualTo(1);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void execute_SelectStatementGivenForNonExisting_ShouldReturnEmpty() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(4)))
                .build());
    session.commit();

    // Assert
    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void execute_SelectStatementWithRangePredicateGivenForNonExisting_ShouldReturnEmpty() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isGreaterThanOrEqualTo(Value.ofInt(4)))
                .and(Predicate.column(ACCOUNT_TYPE).isLessThanOrEqualTo(Value.ofInt(6)))
                .build());
    session.commit();

    // Assert
    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void execute_SelectStatementWithPredicateForIndexColumnGiven_ShouldReturnEmpty() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(SOME_COLUMN).isEqualTo(Value.ofInt(2)))
                .build());
    session.commit();

    // Assert
    Optional<Record> record1 = resultSet.one();
    assertThat(record1).isPresent();
    assertThat(record1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(record1.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(record1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record1.get().getInt(SOME_COLUMN)).isEqualTo(2);

    Optional<Record> record2 = resultSet.one();
    assertThat(record2).isPresent();
    assertThat(record2.get().getInt(ACCOUNT_ID)).isEqualTo(2);
    assertThat(record2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(record2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(record2.get().getInt(SOME_COLUMN)).isEqualTo(2);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndCommit_InsertStatementGivenForNonExisting_ShouldCreateRecord() {
    // Arrange
    int expected = INITIAL_BALANCE;

    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    session.execute(
        StatementBuilder.insertInto(namespace, TABLE)
            .values(
                Assignment.column(ACCOUNT_ID).value(Value.ofInt(0)),
                Assignment.column(ACCOUNT_TYPE).value(Value.ofInt(0)),
                Assignment.column(BALANCE).value(Value.ofInt(expected)))
            .build());
    session.commit();

    // Assert
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    Optional<Record> record = resultSet.one();
    assertThat(record).isPresent();
    assertThat(record.get().getInt(BALANCE)).isEqualTo(expected);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndCommit_UpdateStatementForExistingAfterRead_ShouldUpdateRecord() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    ResultSet resultSet;
    Optional<Record> record;

    // Act
    session.begin();

    resultSet =
        session.execute(
            StatementBuilder.select(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE)
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());

    record = resultSet.one();
    assertThat(record).isPresent();
    int expected = record.get().getInt(BALANCE) + 100;

    session.execute(
        StatementBuilder.update(namespace, TABLE)
            .set(Assignment.column(BALANCE).value(Value.ofInt(expected)))
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.commit();

    // Assert
    session.begin();
    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    record = resultSet.one();
    assertThat(record).isPresent();
    assertThat(record.get().getInt(BALANCE)).isEqualTo(expected);

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndCommit_InsertStatementWithNullValueGiven_ShouldCreateRecordProperly() {
    // Arrange
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    session.execute(
        StatementBuilder.insertInto(namespace, TABLE)
            .values(
                Assignment.column(ACCOUNT_ID).value(Value.ofInt(0)),
                Assignment.column(ACCOUNT_TYPE).value(Value.ofInt(0)),
                Assignment.column(BALANCE).value(Value.ofNull()))
            .build());
    session.commit();

    // Assert
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    Optional<Record> record = resultSet.one();
    assertThat(record).isPresent();
    assertThat(record.get().isNull(BALANCE)).isTrue();

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndCommit_SelectStatementsAndUpdateStatementsGiven_ShouldCommitProperly() {
    // Arrange
    populateRecords();
    int amount = 100;
    int fromId = 0;
    int toId = 1;

    ResultSet resultSet;
    Optional<Record> record;

    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();

    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(fromId)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    record = resultSet.one();
    assertThat(record).isPresent();
    int fromBalance = record.get().getInt(BALANCE) - amount;

    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(toId)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    record = resultSet.one();
    assertThat(record).isPresent();
    int toBalance = record.get().getInt(BALANCE) - amount;

    session.execute(
        StatementBuilder.update(namespace, TABLE)
            .set(Assignment.column(BALANCE).value(Value.ofInt(fromBalance)))
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(fromId)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.execute(
        StatementBuilder.update(namespace, TABLE)
            .set(Assignment.column(BALANCE).value(Value.ofInt(toBalance)))
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(toId)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.commit();

    // Assert
    session.begin();

    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(fromId)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    record = resultSet.one();
    assertThat(record).isPresent();
    assertThat(record.get().getInt(BALANCE)).isEqualTo(fromBalance);

    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(toId)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    record = resultSet.one();
    assertThat(record).isPresent();
    assertThat(record.get().getInt(BALANCE)).isEqualTo(toBalance);

    session.commit();
  }

  @Test
  public void executeAndRollback_InsertStatementGiven_ShouldNotCreateRecord() {
    // Arrange
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    // Act
    session.begin();
    session.execute(
        StatementBuilder.insertInto(namespace, TABLE)
            .values(
                Assignment.column(ACCOUNT_ID).value(Value.ofInt(0)),
                Assignment.column(ACCOUNT_TYPE).value(Value.ofInt(0)),
                Assignment.column(BALANCE).value(Value.ofInt(INITIAL_BALANCE)))
            .build());
    session.rollback();

    // Assert
    session.begin();
    ResultSet resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndCommit_DeleteStatementGivenForExistingAfterRead_ShouldDeleteRecord() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    ResultSet resultSet;

    // Act
    session.begin();

    session.execute(
        StatementBuilder.select()
            .from(namespace, TABLE)
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.execute(
        StatementBuilder.deleteFrom(namespace, TABLE)
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.commit();

    // Assert
    session.begin();
    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    assertThat(resultSet.one()).isNotPresent();
  }

  @Test
  public void executeAndRollback_DeleteStatementGivenForExistingAfterRead_ShouldNotDeleteRecord() {
    // Arrange
    populateRecords();
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();

    ResultSet resultSet;

    // Act
    session.begin();

    session.execute(
        StatementBuilder.select()
            .from(namespace, TABLE)
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.execute(
        StatementBuilder.deleteFrom(namespace, TABLE)
            .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
            .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
            .build());

    session.rollback();

    // Assert
    session.begin();
    resultSet =
        session.execute(
            StatementBuilder.select()
                .from(namespace, TABLE)
                .where(Predicate.column(ACCOUNT_ID).isEqualTo(Value.ofInt(0)))
                .and(Predicate.column(ACCOUNT_TYPE).isEqualTo(Value.ofInt(0)))
                .build());
    session.commit();

    assertThat(resultSet.one()).isPresent();
  }

  private void populateRecords() {
    SqlStatementSession session = sqlStatementSessionFactory.getTransactionSession();
    session.begin();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j ->
                            session.execute(
                                StatementBuilder.insertInto(namespace, TABLE)
                                    .values(
                                        Assignment.column(ACCOUNT_ID).value(Value.ofInt(i)),
                                        Assignment.column(ACCOUNT_TYPE).value(Value.ofInt(j)),
                                        Assignment.column(BALANCE)
                                            .value(Value.ofInt(INITIAL_BALANCE)),
                                        Assignment.column(SOME_COLUMN).value(Value.ofInt(i * j)))
                                    .build())));
    session.commit();
  }
}
