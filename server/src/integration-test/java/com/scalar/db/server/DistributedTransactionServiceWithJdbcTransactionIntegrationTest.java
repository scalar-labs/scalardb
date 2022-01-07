package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.rpc.GrpcTransaction;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
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

  private static ScalarDbServer server;
  private static DistributedStorageAdmin admin;
  private static GrpcTransactionManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, IOException {
    ServerConfig serverConfig = ServerEnv.getServerConfigWithJdbc();
    if (serverConfig != null) {
      server = new ScalarDbServer(serverConfig);
      server.start();
    }

    GrpcConfig grpcConfig = ServerEnv.getGrpcConfig();
    StorageFactory factory = new StorageFactory(grpcConfig);
    admin = factory.getAdmin();
    createTable();
    manager = new GrpcTransactionManager(grpcConfig);
  }

  private static void createTable() throws ExecutionException {
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(NAMESPACE, TABLE, tableMetadata, true);
  }

  @Before
  public void setUp() throws ExecutionException {
    truncateTable();
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    admin.close();
    manager.close();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
  }

  private static void deleteTable() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE);
    admin.dropNamespace(NAMESPACE);
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
    int expected = INITIAL_BALANCE;
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(BALANCE, expected);
    GrpcTransaction transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);
    GrpcTransaction another = manager.start();
    Optional<Result> result = another.get(get);
    assertThat(result).isPresent();
    another.commit();
    assertThat(getBalance(result.get())).isEqualTo(expected);
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
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(BALANCE, expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    GrpcTransaction another = manager.start();
    Optional<Result> actual = another.get(get);
    another.commit();

    assertThat(actual).isPresent();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populateRecords();
    List<Get> gets = prepareGets(NAMESPACE, TABLE);
    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, to, amount).commit();

    // Assert
    GrpcTransaction another = null;
    try {
      another = manager.start();
      Optional<Result> fromResult = another.get(gets.get(from));
      assertThat(fromResult).isPresent();
      assertThat(getBalance(fromResult.get())).isEqualTo(fromBalance);
      Optional<Result> toResult = another.get(gets.get(to));
      assertThat(toResult).isPresent();
      assertThat(getBalance(toResult.get())).isEqualTo(toBalance);
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
    assertThat(result1).isPresent();
    IntValue fromBalance = new IntValue(BALANCE, getBalance(result1.get()) - amount);
    Optional<Result> result2 = transaction.get(gets.get(toId));
    assertThat(result2).isPresent();
    IntValue toBalance = new IntValue(BALANCE, getBalance(result2.get()) + amount);

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
                          Key partitionKey = new Key(ACCOUNT_ID, i);
                          Key clusteringKey = new Key(ACCOUNT_TYPE, j);
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .forNamespace(NAMESPACE)
                                  .forTable(TABLE)
                                  .withValue(BALANCE, INITIAL_BALANCE);
                          try {
                            transaction.put(put);
                          } catch (CrudException e) {
                            throw new RuntimeException(e);
                          }
                        }));
    transaction.commit();
  }

  private Get prepareGet(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
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
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  private Put preparePut(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
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
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }
}
