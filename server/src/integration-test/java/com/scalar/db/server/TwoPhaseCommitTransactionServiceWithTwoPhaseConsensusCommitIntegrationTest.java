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
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransaction;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitIntegrationTest {

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
  private static GrpcTwoPhaseCommitTransactionManager manager;

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(getAccountId(results.get(0))).isEqualTo(0);
    assertThat(getAccountType(results.get(0))).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void
      get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime()
          throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    populate(TABLE_1);
    Optional<Result> result2 = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 4, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 4, 4, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction2.prepare();
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.prepare();
    transaction1.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, expected));
    transaction.prepare();
    transaction.commit();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(0, 0, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);

    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result.isPresent()).isTrue();

    int afterBalance = getBalance(result.get()) + 100;
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, afterBalance));
    transaction.prepare();
    transaction.commit();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    result = another.get(prepareGet(0, 0, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(afterBalance);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowPreparationException()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);

    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1100));

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(0, 0, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE); // a rolled back value
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    populate(TABLE_2);

    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount);

    // Act Assert
    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(fromBalance);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(toBalance);

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    fromTx.put(preparePut(fromId, fromType, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, TABLE_2).withValue(BALANCE, expected));

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction anotherFromTx = manager.start();
              GrpcTwoPhaseCommitTransaction anotherToTx = manager.join(anotherFromTx.getId());
              anotherFromTx.put(
                  preparePut(anotherFromId, anotherFromType, TABLE_2).withValue(BALANCE, expected));
              anotherToTx.put(
                  preparePut(anotherToId, anotherToType, TABLE_1).withValue(BALANCE, expected));
              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete()
      throws TransactionException {
    // Arrange
    int amount = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    populate(TABLE_1);
    populate(TABLE_2);

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    fromTx.get(prepareGet(fromId, fromType, TABLE_1));
    fromTx.delete(prepareDelete(fromId, fromType, TABLE_1));
    toTx.get(prepareGet(toId, toType, TABLE_2));
    toTx.delete(prepareDelete(toId, toType, TABLE_2));

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction anotherFromTx = manager.start();
              GrpcTwoPhaseCommitTransaction anotherToTx = manager.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  TABLE_1,
                  anotherToTx,
                  amount);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount);

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    populate(TABLE_1);
    populate(TABLE_2);

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount1);

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction anotherFromTx = manager.start();
              GrpcTwoPhaseCommitTransaction anotherToTx = manager.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  TABLE_1,
                  anotherToTx,
                  amount2);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = 2;
    int anotherFromType = 0;
    int anotherToId = 3;
    int anotherToType = 0;

    populate(TABLE_1);
    populate(TABLE_2);

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount1);

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction anotherFromTx = manager.start();
              GrpcTwoPhaseCommitTransaction anotherToTx = manager.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  TABLE_1,
                  anotherToTx,
                  amount2);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount1);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount1);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);

    another.prepare();
    another.commit();
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = fromId;
    int anotherFromType = fromType;
    int anotherToId = toId;
    int anotherToType = toType;

    GrpcTwoPhaseCommitTransaction fromTx = manager.start();
    GrpcTwoPhaseCommitTransaction toTx = manager.join(fromTx.getId());
    fromTx.put(preparePut(fromId, fromType, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, TABLE_2).withValue(BALANCE, expected));

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction anotherFromTx = manager.start();
              GrpcTwoPhaseCommitTransaction anotherToTx = manager.join(anotherFromTx.getId());
              anotherFromTx.put(
                  preparePut(anotherFromId, anotherFromType, TABLE_2).withValue(BALANCE, expected));
              anotherToTx.put(
                  preparePut(anotherToId, anotherToType, TABLE_1).withValue(BALANCE, expected));

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    another.prepare();
    another.commit();
  }

  @Test
  public void prepare_DeleteGivenWithoutRead_ShouldThrowPreparationException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.delete(prepareDelete(0, 0, TABLE_1));

    // Act
    Throwable throwable = catchThrowable(transaction::prepare);
    transaction.rollback();

    // Assert
    assertThat(throwable).isInstanceOf(PreparationException.class);
  }

  @Test
  public void prepare_DeleteGivenForNonExisting_ShouldThrowPreparationException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.delete(prepareDelete(0, 0, TABLE_1));

    // Act
    Throwable throwable = catchThrowable(transaction::prepare);
    transaction.rollback();

    // Assert
    assertThat(throwable).isInstanceOf(PreparationException.class);
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.delete(prepareDelete(0, 0, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();

    GrpcTwoPhaseCommitTransaction another = manager.start();
    result = another.get(prepareGet(0, 0, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int account1Id = 0;
    int account1Type = 0;
    int account2Id = 1;
    int account2Type = 0;
    int account3Id = 2;
    int account3Type = 0;

    populate(TABLE_1);
    populate(TABLE_2);

    GrpcTwoPhaseCommitTransaction tx1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx2 = manager.join(tx1.getId());
    deletes(account1Id, account1Type, TABLE_1, tx1, account2Id, account2Type, TABLE_2, tx2);

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction tx3 = manager.start();
              GrpcTwoPhaseCommitTransaction tx4 = manager.join(tx3.getId());
              deletes(
                  account2Id, account2Type, TABLE_2, tx3, account3Id, account3Type, TABLE_1, tx4);

              tx3.prepare();
              tx4.prepare();
              tx3.commit();
              tx4.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              tx1.prepare();
              tx2.prepare();
            })
        .isInstanceOf(PreparationException.class);
    tx1.rollback();
    tx2.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(account1Id, account1Type, TABLE_1));
    assertThat(result).isPresent();

    result = another.get(prepareGet(account2Id, account2Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account3Id, account3Type, TABLE_1));
    assertThat(result).isNotPresent();

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int account1Id = 0;
    int account1Type = 0;
    int account2Id = 1;
    int account2Type = 0;
    int account3Id = 2;
    int account3Type = 0;
    int account4Id = 3;
    int account4Type = 0;

    populate(TABLE_1);
    populate(TABLE_2);

    GrpcTwoPhaseCommitTransaction tx1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx2 = manager.join(tx1.getId());
    deletes(account1Id, account1Type, TABLE_1, tx1, account2Id, account2Type, TABLE_2, tx2);

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction tx3 = manager.start();
              GrpcTwoPhaseCommitTransaction tx4 = manager.join(tx3.getId());
              deletes(
                  account3Id, account3Type, TABLE_2, tx3, account4Id, account4Type, TABLE_1, tx4);

              tx3.prepare();
              tx4.prepare();
              tx3.commit();
              tx4.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              tx1.prepare();
              tx2.prepare();
              tx1.commit();
              tx2.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction another = manager.start();
    Optional<Result> result = another.get(prepareGet(account1Id, account1Type, TABLE_1));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account2Id, account2Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account3Id, account3Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account4Id, account4Type, TABLE_1));
    assertThat(result).isNotPresent();

    another.prepare();
    another.commit();
  }

  @Test
  public void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction tx1Sub1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx1Sub2 = manager.join(tx1Sub1.getId());
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    GrpcTwoPhaseCommitTransaction tx2Sub1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx2Sub2 = manager.join(tx2Sub1.getId());
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    tx2Sub1.commit();
    tx2Sub2.commit();

    // Assert
    transaction = manager.start();

    // the results can not be produced by executing the transactions serially
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2 + 1);

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Optional<Result> result = transaction1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int balance1 = getBalance(result.get());
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance1 + 1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    // the same transaction processing as transaction1
    GrpcTwoPhaseCommitTransaction transaction3 = manager.start();
    result = transaction3.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    GrpcTwoPhaseCommitTransaction transaction3 = manager.start();
    Optional<Result> result = transaction3.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(prepareGet(0, 0, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void scan_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    List<Result> resultBefore = transaction1.scan(prepareScan(0, 0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    List<Result> resultAfter = transaction1.scan(prepareScan(0, 0, 0, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void delete_PutCalledBefore_ShouldDelete() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldPut() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isTrue();
    assertThat(getBalance(resultAfter.get())).isEqualTo(2);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act Assert
    assertThatThrownBy(() -> transaction.scan(prepareScan(0, 0, 0, TABLE_1)))
        .isInstanceOf(CrudException.class);
    transaction.rollback();
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act Assert
    assertThatCode(() -> transaction.scan(prepareScan(0, 1, 1, TABLE_1)))
        .doesNotThrowAnyException();
    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void start_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    String transactionId = "id";

    // Act Assert
    assertThatCode(
            () -> {
              GrpcTwoPhaseCommitTransaction transaction = manager.start(transactionId);
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void start_EmptyTransactionIdGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    String transactionId = "";

    // Act Assert
    assertThatThrownBy(() -> manager.start(transactionId))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TransactionState state = manager.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction2.prepare();
    transaction2.commit();

    assertThatCode(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Act
    TransactionState state = manager.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager.abort(transaction.getId());

    transaction.prepare();
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    TransactionState state = manager.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  private void populate(String table) throws TransactionException {
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        transaction.put(preparePut(i, j, table).withValue(BALANCE, INITIAL_BALANCE));
      }
    }
    transaction.prepare();
    transaction.commit();
  }

  private void transfer(
      int fromId,
      int fromType,
      String fromTable,
      GrpcTwoPhaseCommitTransaction fromTx,
      int toId,
      int toType,
      String toTable,
      GrpcTwoPhaseCommitTransaction toTx,
      int amount)
      throws TransactionException {
    int fromBalance =
        fromTx
            .get(prepareGet(fromId, fromType, fromTable))
            .get()
            .getValue(BALANCE)
            .get()
            .getAsInt();
    int toBalance =
        toTx.get(prepareGet(toId, toType, toTable)).get().getValue(BALANCE).get().getAsInt();
    fromTx.put(preparePut(fromId, fromType, fromTable).withValue(BALANCE, fromBalance - amount));
    toTx.put(preparePut(toId, toType, toTable).withValue(BALANCE, toBalance + amount));
  }

  private void deletes(
      int id,
      int type,
      String table,
      GrpcTwoPhaseCommitTransaction tx,
      int anotherId,
      int anotherType,
      String anotherTable,
      GrpcTwoPhaseCommitTransaction anotherTx)
      throws TransactionException {
    tx.get(prepareGet(id, type, table));
    anotherTx.get(prepareGet(anotherId, anotherType, anotherTable));
    tx.delete(prepareDelete(id, type, table));
    anotherTx.delete(prepareDelete(anotherId, anotherType, anotherTable));
  }

  static Get prepareGet(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static Scan prepareScan(int id, int fromType, int toType, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  static Put preparePut(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static Delete prepareDelete(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  static int getAccountId(Result result) {
    Optional<Value<?>> id = result.getValue(ACCOUNT_ID);
    assertThat(id).isPresent();
    return id.get().getAsInt();
  }

  static int getAccountType(Result result) {
    Optional<Value<?>> type = result.getValue(ACCOUNT_TYPE);
    assertThat(type).isPresent();
    return type.get().getAsInt();
  }

  static int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv(CONTACT_POINT, USERNAME, PASSWORD, Optional.empty());

    // For the coordinator table
    testEnv.createTable(
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
      testEnv.createTable(
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

    Properties serverProperties = new Properties(testEnv.getJdbcConfig().getProperties());
    serverProperties.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");
    server = new ScalarDbServer(serverProperties);
    server.start();

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    properties.setProperty(DatabaseConfig.CONTACT_PORT, "60051");
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    manager = new GrpcTwoPhaseCommitTransactionManager(new GrpcConfig(properties));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    manager.close();
    server.shutdown();
    server.blockUntilShutdown();
    testEnv.deleteTables();
    testEnv.close();
  }
}
