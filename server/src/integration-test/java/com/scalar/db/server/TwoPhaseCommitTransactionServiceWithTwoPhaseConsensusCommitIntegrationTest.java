package com.scalar.db.server;

import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.BALANCE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.INITIAL_BALANCE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.NUM_ACCOUNTS;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.NUM_TYPES;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_2;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.createTables;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.deleteTables;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.getAccountId;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.getAccountType;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.getBalance;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareDelete;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareGet;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.preparePut;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareScan;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.truncateTables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransaction;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitIntegrationTest {

  private static ScalarDbServer server;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static GrpcTwoPhaseCommitTransactionManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, IOException {
    ServerConfig serverConfig = ServerEnv.getServerConfig();
    if (serverConfig != null) {
      server = new ScalarDbServer(serverConfig);
      server.start();
    }

    GrpcConfig grpcConfig = ServerEnv.getGrpcConfig();
    StorageFactory factory = new StorageFactory(grpcConfig);
    admin = factory.getAdmin();
    consensusCommitAdmin =
        new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(grpcConfig.getProperties()));
    createTables(admin, consensusCommitAdmin);
    manager = new GrpcTwoPhaseCommitTransactionManager(grpcConfig);
  }

  @Before
  public void setUp() throws ExecutionException {
    truncateTables(admin, consensusCommitAdmin);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables(admin, consensusCommitAdmin);
    admin.close();
    manager.close();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
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
  public void get_PutCalledBefore_ShouldGet() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();

    // Act
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> result = transaction.get(get);
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
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
  public void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    Throwable thrown =
        catchThrowable(() -> transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2)));
    transaction1.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 5, TABLE_1).withValue(BALANCE, 3));
    transaction.put(preparePut(0, 3, TABLE_1).withValue(BALANCE, 2));

    // Act
    Scan scan = prepareScan(0, 0, 10, TABLE_1);
    List<Result> results = transaction.scan(scan);
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getBalance(results.get(0))).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(2);
    assertThat(getBalance(results.get(2))).isEqualTo(3);
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
}
