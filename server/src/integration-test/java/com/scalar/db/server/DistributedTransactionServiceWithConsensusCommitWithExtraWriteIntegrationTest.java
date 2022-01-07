package com.scalar.db.server;

import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.BALANCE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_2;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.createTables;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.deleteTables;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.getBalance;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareGet;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.preparePut;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareScan;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.truncateTables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Isolation;
import com.scalar.db.transaction.consensuscommit.SerializableStrategy;
import com.scalar.db.transaction.rpc.GrpcTransaction;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedTransactionServiceWithConsensusCommitWithExtraWriteIntegrationTest {

  private static ScalarDbServer server;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static GrpcTransactionManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, IOException {
    ServerConfig serverConfig =
        ServerEnv.getServerConfig(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    manager = new GrpcTransactionManager(grpcConfig);
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

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2) throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, table1).withValue(BALANCE, 1),
            preparePut(0, 1, table2).withValue(BALANCE, 1));
    GrpcTransaction transaction = manager.start();
    transaction.put(puts);
    transaction.commit();

    // Act
    GrpcTransaction transaction1 = manager.start();
    GrpcTransaction transaction2 = manager.start();
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    assertThat(result1).isPresent();
    int current1 = getBalance(result1.get());
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    assertThat(result2).isPresent();
    int current2 = getBalance(result2.get());
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start();
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    transaction.commit();
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2) throws TransactionException {
    // Arrange
    // no records

    // Act
    GrpcTransaction transaction1 = manager.start();
    GrpcTransaction transaction2 = manager.start();
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    int current1 = 0;
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    int current2 = 0;
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    GrpcTransaction transaction = manager.start();
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    transaction.commit();
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    // no records

    // Act
    GrpcTransaction transaction1 = manager.start();
    GrpcTransaction transaction2 = manager.start();
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();
    GrpcTransaction transaction = manager.start();
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, TABLE_1));
    transaction.commit();
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    assertThat(thrown1).isInstanceOf(CommitConflictException.class);
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }
}
