package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.transaction.rpc.GrpcTransaction;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedTransactionServiceWithJdbcTransactionIntegrationTest {

  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "tx_test_table";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;

  private static final String CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "mysql";

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
    populateRecords();
    GrpcTransaction transaction = manager.start();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords();
    GrpcTransaction transaction = manager.start();
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords();
    GrpcTransaction transaction = manager.start();
    Get get = prepareGet(0, 4, NAMESPACE, TABLE);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords();
    GrpcTransaction transaction = manager.start();
    Scan scan = prepareScan(0, 4, 4, NAMESPACE, TABLE);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(expected);
    GrpcTransaction transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);
    GrpcTransaction another = manager.start();
    Result result = another.get(get).get();
    another.commit();
    assertThat(result.getValue(BALANCE).get()).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);
    GrpcTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    int afterBalance = ((IntValue) result.get().getValue(BALANCE).get()).get() + 100;
    IntValue expected = new IntValue(BALANCE, afterBalance);
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    GrpcTransaction another = manager.start();
    Result actual = another.get(get).get();
    another.commit();

    assertThat(actual.getValue(BALANCE).get()).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populateRecords();
    List<Get> gets = prepareGets(NAMESPACE, TABLE);
    int amount = 100;
    IntValue fromBalance = new IntValue(BALANCE, INITIAL_BALANCE - amount);
    IntValue toBalance = new IntValue(BALANCE, INITIAL_BALANCE + amount);
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, to, amount).commit();

    // Assert
    GrpcTransaction another = null;
    try {
      another = manager.start();
      assertThat(another.get(gets.get(from)).get().getValue(BALANCE))
          .isEqualTo(Optional.of(fromBalance));
      assertThat(another.get(gets.get(to)).get().getValue(BALANCE))
          .isEqualTo(Optional.of(toBalance));
    } finally {
      if (another != null) {
        another.commit();
      }
    }
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);
    Delete delete = prepareDelete(0, 0, NAMESPACE, TABLE);
    GrpcTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    GrpcTransaction another = manager.start();
    Optional<Result> result1 = another.get(get);
    another.commit();
    assertThat(result1.isPresent()).isFalse();
  }

  private GrpcTransaction prepareTransfer(int fromId, int toId, int amount)
      throws TransactionException {
    GrpcTransaction transaction = manager.start();
    List<Get> gets = prepareGets(NAMESPACE, TABLE);

    Optional<Result> result1 = transaction.get(gets.get(fromId));
    Optional<Result> result2 = transaction.get(gets.get(toId));
    IntValue fromBalance =
        new IntValue(BALANCE, ((IntValue) result1.get().getValue(BALANCE).get()).get() - amount);
    IntValue toBalance =
        new IntValue(BALANCE, ((IntValue) result2.get().getValue(BALANCE).get()).get() + amount);
    List<Put> puts = preparePuts(NAMESPACE, TABLE);
    puts.get(fromId).withValue(fromBalance);
    puts.get(toId).withValue(toBalance);
    transaction.put(puts.get(fromId));
    transaction.put(puts.get(toId));
    return transaction;
  }

  private void populateRecords() throws TransactionException {
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
                                  .forTable(TABLE)
                                  .withValue(new IntValue(BALANCE, INITIAL_BALANCE));
                          try {
                            transaction.put(put);
                          } catch (CrudException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    transaction.commit();
  }

  private Get prepareGet(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Get(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Get> prepareGets(String namespace, String table) {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> gets.add(prepareGet(i, j, namespace, table))));
    return gets;
  }

  private Scan prepareScan(int id, int fromType, int toType, String namespace, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(new IntValue(ACCOUNT_TYPE, fromType)))
        .withEnd(new Key(new IntValue(ACCOUNT_TYPE, toType)));
  }

  private Put preparePut(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Put(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Put> preparePuts(String namespace, String table) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> puts.add(preparePut(i, j, namespace, table))));
    return puts;
  }

  private Delete prepareDelete(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv(CONTACT_POINT, USERNAME, PASSWORD, Optional.empty());
    testEnv.register(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build());

    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    Properties serverProperties = new Properties(testEnv.getJdbcDatabaseConfig().getProperties());
    serverProperties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
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
