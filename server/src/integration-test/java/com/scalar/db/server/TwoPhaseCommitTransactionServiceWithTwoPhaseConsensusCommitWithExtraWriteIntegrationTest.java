package com.scalar.db.server;

import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.ACCOUNT_ID;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.ACCOUNT_TYPE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.BALANCE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.CONTACT_POINT;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.NAMESPACE;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.PASSWORD;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.TABLE_2;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.USERNAME;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareScan;
import static com.scalar.db.server.TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitIntegrationTest.getBalance;
import static com.scalar.db.server.TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitIntegrationTest.prepareGet;
import static com.scalar.db.server.TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitIntegrationTest.preparePut;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
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

public
class TwoPhaseCommitTransactionServiceWithTwoPhaseConsensusCommitWithExtraWriteIntegrationTest {

  private static TestEnv testEnv;
  private static ScalarDbServer server;
  private static GrpcTwoPhaseCommitTransactionManager manager;

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
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
    tx1Sub1.commit();
    tx1Sub2.commit();

    assertThatThrownBy(
            () -> {
              tx2Sub1.prepare();
              tx2Sub2.prepare();
            })
        .isInstanceOf(PreparationException.class);
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
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange

    // Act Assert
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
    tx1Sub1.commit();
    tx1Sub2.commit();

    assertThatThrownBy(
            () -> {
              tx2Sub1.prepare();
              tx2Sub2.prepare();
            })
        .isInstanceOf(PreparationException.class);
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
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowPreparationException()
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

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    assertThatThrownBy(transaction2::prepare).isInstanceOf(PreparationException.class);
    transaction2.rollback();

    // Assert
    GrpcTwoPhaseCommitTransaction transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isNotPresent();
    transaction.prepare();
    transaction.commit();
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
    serverProperties.setProperty(DatabaseConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    serverProperties.setProperty(ConsensusCommitConfig.SERIALIZABLE_STRATEGY, "EXTRA_WRITE");
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
