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
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareGet;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.preparePut;
import static com.scalar.db.server.DistributedTransactionServiceWithConsensusCommitIntegrationTest.prepareScan;
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
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.rpc.GrpcTransaction;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedTransactionServiceWithConsensusCommitWithExtraWriteIntegrationTest {

  private static TestEnv testEnv;
  private static ScalarDbServer server;
  private static GrpcTransactionManager manager;

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
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
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start();
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    transaction.commit();
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
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
    Put put1 = preparePut(0, 0, table1).withValue(new IntValue(BALANCE, current1 + 1));
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(new IntValue(BALANCE, current2 + 1));
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
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
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
    Put put1 = preparePut(0, 0, TABLE_1).withValue(new IntValue(BALANCE, count1 + 1));
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, TABLE_1).withValue(new IntValue(BALANCE, count2 + 1));
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

    Properties serverProperties = new Properties(testEnv.getJdbcConfig().getProperties());
    serverProperties.setProperty(DatabaseConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    serverProperties.setProperty(DatabaseConfig.SERIALIZABLE_STRATEGY, "EXTRA_WRITE");
    serverProperties.setProperty(ServerConfig.PROMETHEUS_HTTP_ENDPOINT_PORT, "0");
    server = new ScalarDbServer(serverProperties);
    server.start();

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    properties.setProperty(DatabaseConfig.CONTACT_PORT, "60051");
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    manager = new GrpcTransactionManager(new GrpcConfig(properties));
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
