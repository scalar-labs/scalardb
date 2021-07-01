package com.scalar.db.server;

import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.CREATED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.transaction.rpc.GrpcTransaction;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedTransactionServiceWithConsensusCommitIntegrationTest {

  static final String NAMESPACE = "integration_testing";
  static final String TABLE_1 = "tx_test_table1";
  static final String TABLE_2 = "tx_test_table2";
  static final String ACCOUNT_ID = "account_id";
  static final String ACCOUNT_TYPE = "account_type";
  static final String BALANCE = "balance";
  static final int INITIAL_BALANCE = 1000;
  static final int NUM_ACCOUNTS = 4;
  static final int NUM_TYPES = 4;

  static final String CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  static final String USERNAME = "root";
  static final String PASSWORD = "mysql";

  private static TestEnv testEnv;
  private static ScalarDbServer server;
  private static GrpcTransactionManager manager;

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    GrpcTransaction transaction = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat((new TransactionResult(result.get())).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    GrpcTransaction transaction = manager.start();
    Scan scan = prepareScan(0, 0, 0, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat((new TransactionResult(results.get(0))).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    GrpcTransaction transaction = manager.start();
    Get get = prepareGet(0, 4, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    GrpcTransaction transaction = manager.start();
    Scan scan = prepareScan(0, 4, 4, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));
    transaction.commit();

    GrpcTransaction transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));

    GrpcTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 2)));
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.commit();

    // Assert
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    Put put = preparePut(0, 0, TABLE_1).withValue(expected);
    GrpcTransaction transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, TABLE_1);
    GrpcTransaction another = manager.start();
    TransactionResult result = new TransactionResult(another.get(get).get());
    another.commit();
    assertThat(result.getValue(BALANCE).get()).isEqualTo(expected);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    Get get = prepareGet(0, 0, TABLE_1);
    GrpcTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    int afterBalance = ((IntValue) result.get().getValue(BALANCE).get()).get() + 100;
    IntValue expected = new IntValue(BALANCE, afterBalance);
    Put put = preparePut(0, 0, TABLE_1).withValue(expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    GrpcTransaction another = manager.start();
    TransactionResult actual = new TransactionResult(another.get(get).get());
    another.commit();
    assertThat(actual.getValue(BALANCE).get()).isEqualTo(expected);
    assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowCommitException()
      throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    List<Put> puts = preparePuts(TABLE_1);
    puts.get(0).withValue(new IntValue(BALANCE, 1100));
    GrpcTransaction transaction = manager.start();

    // Act Assert
    transaction.put(puts.get(0));
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
  }

  private void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(String fromTable, String toTable)
      throws TransactionException {
    // Arrange
    boolean differentTables = !fromTable.equals(toTable);

    populateRecords(fromTable);
    if (differentTables) {
      populateRecords(toTable);
    }

    List<Get> fromGets = prepareGets(fromTable);
    List<Get> toGets = differentTables ? prepareGets(toTable) : fromGets;

    int amount = 100;
    IntValue fromBalance = new IntValue(BALANCE, INITIAL_BALANCE - amount);
    IntValue toBalance = new IntValue(BALANCE, INITIAL_BALANCE + amount);
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, fromTable, to, toTable, amount).commit();

    // Assert
    GrpcTransaction another = manager.start();
    assertThat(another.get(fromGets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(fromBalance));
    assertThat(another.get(toGets.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(toBalance));
    another.commit();
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly()
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(TABLE_1, TABLE_1);
  }

  @Test
  public void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly()
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts1 = preparePuts(table1);
    List<Put> puts2 = differentTables ? preparePuts(table2) : puts1;

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts1.get(from).withValue(expected);
    puts2.get(to).withValue(expected);

    // Act Assert
    GrpcTransaction transaction = manager.start();
    transaction.put(puts1.get(from));
    transaction.put(puts2.get(to));

    GrpcTransaction conflictingTransaction = manager.start();
    puts1.get(anotherTo).withValue(expected);
    assertThatCode(
            () -> {
              conflictingTransaction.put(puts2.get(anotherFrom));
              conflictingTransaction.put(puts1.get(anotherTo));
              conflictingTransaction.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;

    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(from)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(expected));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    another.commit();
  }

  @Test
  public void
      commit_ConflictingPutsForSameTableGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    // Act
    GrpcTransaction transaction = manager.start();
    List<Get> gets1 = prepareGets(table1);
    List<Delete> deletes1 = prepareDeletes(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;
    List<Delete> deletes2 = differentTables ? prepareDeletes(table2) : deletes1;

    transaction.get(gets1.get(from));
    transaction.delete(deletes1.get(from));
    transaction.get(gets2.get(to));
    transaction.delete(deletes2.get(to));

    assertThatCode(() -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount).commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount)));
    another.commit();
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForSameTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForDifferentTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    // Act
    GrpcTransaction transaction = prepareTransfer(from, table1, to, table2, amount1);

    assertThatCode(() -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount2).commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = prepareGets(table2);

    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
    another.commit();
  }

  @Test
  public void commit_ConflictingPutsForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = NUM_TYPES * 2;
    int anotherTo = NUM_TYPES * 3;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    // Act
    GrpcTransaction transaction = prepareTransfer(from, table1, to, table2, amount1);

    assertThatCode(() -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount2).commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = prepareGets(table2);

    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount1)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount1)));
    assertThat(another.get(gets2.get(anotherFrom)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
    another.commit();
  }

  @Test
  public void commit_NonConflictingPutsForSameTableGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_2);
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts1 = preparePuts(TABLE_1);
    List<Put> puts2 = preparePuts(TABLE_2);

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = from;
    int anotherTo = to;
    puts1.get(from).withValue(expected);
    puts1.get(to).withValue(expected);

    // Act Assert
    GrpcTransaction transaction = manager.start();
    transaction.put(puts1.get(from));
    transaction.put(puts1.get(to));

    GrpcTransaction conflictingTransaction = manager.start();
    puts2.get(from).withValue(expected);
    puts2.get(to).withValue(expected);
    assertThatCode(
            () -> {
              conflictingTransaction.put(puts2.get(anotherFrom));
              conflictingTransaction.put(puts2.get(anotherTo));
              conflictingTransaction.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(TABLE_1);
    List<Get> gets2 = prepareGets(TABLE_2);
    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    assertThat(another.get(gets1.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(expected));
    assertThat(another.get(gets2.get(anotherFrom)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    assertThat(another.get(gets2.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    another.commit();
  }

  @Test
  public void commit_DeleteGivenWithoutRead_ShouldThrowCommitException()
      throws TransactionException {
    // Arrange
    Delete delete = prepareDelete(0, 0, TABLE_1);
    GrpcTransaction transaction = manager.start();

    // Act Assert
    transaction.delete(delete);
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
  }

  @Test
  public void commit_DeleteGivenForNonExisting_ShouldThrowCommitException()
      throws TransactionException {
    // Arrange
    Get get = prepareGet(0, 0, TABLE_1);
    Delete delete = prepareDelete(0, 0, TABLE_1);
    GrpcTransaction transaction = manager.start();

    // Act Assert
    transaction.get(get);
    transaction.delete(delete);
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords(TABLE_1);
    Get get = prepareGet(0, 0, TABLE_1);
    Delete delete = prepareDelete(0, 0, TABLE_1);
    GrpcTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    GrpcTransaction another = manager.start();
    assertThat(another.get(get).isPresent()).isFalse();
    another.commit();
  }

  private void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    // Act
    GrpcTransaction transaction = prepareDeletes(account1, table1, account2, table2);

    assertThatCode(() -> prepareDeletes(account2, table2, account3, table1).commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;

    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(account1)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account3)).isPresent()).isFalse();
    another.commit();
  }

  @Test
  public void
      commit_ConflictingDeletesForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      String table1, String table2) throws TransactionException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    // Act
    GrpcTransaction transaction = prepareDeletes(account1, table1, account2, table2);

    assertThatCode(() -> prepareDeletes(account3, table2, account4, table1).commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;
    GrpcTransaction another = manager.start();
    assertThat(another.get(gets1.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account4)).isPresent()).isFalse();
    another.commit();
  }

  @Test
  public void commit_NonConflictingDeletesForSameTableGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_2);
  }

  private void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
      String table1, String table2) throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, table1).withValue(new IntValue(BALANCE, 1)),
            preparePut(0, 1, table2).withValue(new IntValue(BALANCE, 1)));
    GrpcTransaction transaction = manager.start();
    transaction.put(puts);
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    GrpcTransaction transaction2 = manager.start();
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    int current1 = ((IntValue) result1.get().getValue(BALANCE).get()).get();
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    int current2 = ((IntValue) result2.get().getValue(BALANCE).get()).get();
    Put put1 = preparePut(0, 0, table1).withValue(new IntValue(BALANCE, current1 + 1));
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(new IntValue(BALANCE, current2 + 1));
    transaction2.put(put2);
    transaction1.commit();
    transaction2.commit();

    // Assert
    transaction = manager.start();
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    transaction.commit();
    // the results can not be produced by executing the transactions serially
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSnapshot_ShouldProduceNonSerializableResult()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSnapshot_ShouldProduceNonSerializableResult()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        TABLE_1, TABLE_2);
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 2)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));
    int balance1 = 0;
    if (result1.isPresent()) {
      balance1 = ((IntValue) result1.get().getValue(BALANCE).get()).get();
    }
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, balance1 + 1)));

    GrpcTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    GrpcTransaction transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = ((IntValue) result3.get().getValue(BALANCE).get()).get();
    }
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, balance3 + 1)));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.commit();
    assertThat(((IntValue) result.get().getValue(BALANCE).get()).get()).isEqualTo(1);
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 2)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    GrpcTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    GrpcTransaction transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = ((IntValue) result3.get().getValue(BALANCE).get()).get();
    }
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, balance3 + 1)));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.commit();
    assertThat(((IntValue) result.get().getValue(BALANCE).get()).get()).isEqualTo(1);
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(get);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void scan_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    List<Result> resultBefore = transaction1.scan(scan);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    List<Result> resultAfter = transaction1.scan(scan);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void delete_PutCalledBefore_ShouldDelete() throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 2)));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    GrpcTransaction transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldPut() throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 2)));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    GrpcTransaction transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isTrue();
    assertThat(resultAfter.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));

    // Act
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.abort();

    // Assert
    assertThat(thrown).isInstanceOf(CrudException.class);
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    GrpcTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, 1)));

    // Act
    Scan scan = prepareScan(0, 1, 1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  private GrpcTransaction prepareTransfer(
      int fromId, String fromTable, int toId, String toTable, int amount)
      throws TransactionException {
    boolean differentTables = !toTable.equals(fromTable);

    GrpcTransaction transaction = manager.start();

    List<Get> fromGets = prepareGets(fromTable);
    List<Get> toGets = differentTables ? prepareGets(toTable) : fromGets;
    Optional<Result> fromResult = transaction.get(fromGets.get(fromId));
    Optional<Result> toResult = transaction.get(toGets.get(toId));

    IntValue fromBalance =
        new IntValue(BALANCE, ((IntValue) fromResult.get().getValue(BALANCE).get()).get() - amount);
    IntValue toBalance =
        new IntValue(BALANCE, ((IntValue) toResult.get().getValue(BALANCE).get()).get() + amount);

    List<Put> fromPuts = preparePuts(fromTable);
    List<Put> toPuts = differentTables ? preparePuts(toTable) : fromPuts;
    fromPuts.get(fromId).withValue(fromBalance);
    toPuts.get(toId).withValue(toBalance);
    transaction.put(fromPuts.get(fromId));
    transaction.put(toPuts.get(toId));

    return transaction;
  }

  private GrpcTransaction prepareDeletes(int one, String table, int another, String anotherTable)
      throws TransactionException {
    boolean differentTables = !table.equals(anotherTable);

    GrpcTransaction transaction = manager.start();

    List<Get> gets = prepareGets(table);
    List<Get> anotherGets = differentTables ? prepareGets(anotherTable) : gets;
    transaction.get(gets.get(one));
    transaction.get(anotherGets.get(another));

    List<Delete> deletes = prepareDeletes(table);
    List<Delete> anotherDeletes = differentTables ? prepareDeletes(anotherTable) : deletes;
    transaction.delete(deletes.get(one));
    transaction.delete(anotherDeletes.get(another));

    return transaction;
  }

  private void populateRecords(String table) throws TransactionException {
    GrpcTransaction transaction = manager.start();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(new IntValue(ACCOUNT_ID, i));
                          Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, j));
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .forNamespace(NAMESPACE)
                                  .forTable(table)
                                  .withValue(new IntValue(BALANCE, INITIAL_BALANCE));
                          try {
                            transaction.put(put);
                          } catch (TransactionException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    transaction.commit();
  }

  static Get prepareGet(int id, int type, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Get(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static List<Get> prepareGets(String table) {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> IntStream.range(0, NUM_TYPES).forEach(j -> gets.add(prepareGet(i, j, table))));
    return gets;
  }

  static Scan prepareScan(int id, int fromType, int toType, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    return new Scan(partitionKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(new IntValue(ACCOUNT_TYPE, fromType)))
        .withEnd(new Key(new IntValue(ACCOUNT_TYPE, toType)));
  }

  static Put preparePut(int id, int type, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Put(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static List<Put> preparePuts(String table) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> IntStream.range(0, NUM_TYPES).forEach(j -> puts.add(preparePut(i, j, table))));
    return puts;
  }

  static Delete prepareDelete(int id, int type, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static List<Delete> prepareDeletes(String table) {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> deletes.add(prepareDelete(i, j, table))));
    return deletes;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv(CONTACT_POINT, USERNAME, PASSWORD, Optional.empty());

    // For the coordinator table
    testEnv.register(
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        TableMetadata.newBuilder()
            .addColumn(ID, DataType.TEXT)
            .addColumn(STATE, DataType.INT)
            .addColumn(CREATED_AT, DataType.BIGINT)
            .addPartitionKey(ID)
            .build());

    // For the test tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      testEnv.register(
          NAMESPACE,
          table,
          TableMetadata.newBuilder()
              .addColumn(ACCOUNT_ID, DataType.INT)
              .addColumn(ACCOUNT_TYPE, DataType.INT)
              .addColumn(BALANCE, DataType.INT)
              .addColumn(ID, DataType.TEXT)
              .addColumn(STATE, DataType.INT)
              .addColumn(VERSION, DataType.INT)
              .addColumn(PREPARED_AT, DataType.BIGINT)
              .addColumn(COMMITTED_AT, DataType.BIGINT)
              .addColumn(BEFORE_PREFIX + BALANCE, DataType.INT)
              .addColumn(BEFORE_ID, DataType.TEXT)
              .addColumn(BEFORE_STATE, DataType.INT)
              .addColumn(BEFORE_VERSION, DataType.INT)
              .addColumn(BEFORE_PREPARED_AT, DataType.BIGINT)
              .addColumn(BEFORE_COMMITTED_AT, DataType.BIGINT)
              .addPartitionKey(ACCOUNT_ID)
              .addClusteringKey(ACCOUNT_TYPE)
              .build());
    }

    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    Properties serverProperties = new Properties(testEnv.getJdbcDatabaseConfig().getProperties());
    server = new ScalarDbServer(serverProperties);
    server.start();

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    properties.setProperty(DatabaseConfig.CONTACT_PORT, "60051");
    manager = new GrpcTransactionManager(new DatabaseConfig(properties));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    manager.close();
    server.shutdown();
    server.blockUntilShutdown();
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
