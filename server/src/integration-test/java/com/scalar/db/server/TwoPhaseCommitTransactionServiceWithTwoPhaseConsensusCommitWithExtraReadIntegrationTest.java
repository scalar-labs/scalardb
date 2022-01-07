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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Isolation;
import com.scalar.db.transaction.consensuscommit.SerializableStrategy;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransaction;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public
class TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitWithExtraReadIntegrationTest {

  private static ScalarDbServer server;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static GrpcTwoPhaseCommitTransactionManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, IOException {
    ServerConfig serverConfig =
        ServerEnv.getServerConfig(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
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
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Act Assert
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
    tx1Sub1.validate();
    tx1Sub2.validate();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    assertThatThrownBy(
            () -> {
              tx2Sub1.validate();
              tx2Sub2.validate();
            })
        .isInstanceOf(ValidationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange

    // Act
    GrpcTwoPhaseCommitTransaction tx1Sub1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx1Sub2 = manager.join(tx1Sub1.getId());
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
    int current1 = 0;
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    GrpcTwoPhaseCommitTransaction tx2Sub1 = manager.start();
    GrpcTwoPhaseCommitTransaction tx2Sub2 = manager.join(tx2Sub1.getId());
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int current2 = 0;
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.validate();
    tx1Sub2.validate();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    assertThatThrownBy(
            () -> {
              tx2Sub1.validate();
              tx2Sub2.validate();
            })
        .isInstanceOf(ValidationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange

    // Act Assert
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count1 = 0;
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    results = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count2 = 0;
    transaction2.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.validate();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    transaction2.prepare();
    assertThatThrownBy(transaction2::validate).isInstanceOf(ValidationException.class);
    transaction2.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isNotPresent();
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Act Assert
    GrpcTwoPhaseCommitTransaction transaction1 = manager.start();
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count1 = results.size();
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1));

    GrpcTwoPhaseCommitTransaction transaction2 = manager.start();
    results = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count2 = results.size();
    transaction2.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.validate();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    transaction2.prepare();
    assertThatThrownBy(transaction2::validate).isInstanceOf(ValidationException.class);
    transaction2.rollback();

    // Assert
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
  }
}
