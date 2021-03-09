package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcTransactionIntegrationTest {

  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "tx_test_table";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;

  private TestEnv testEnv;
  private JdbcTransactionManager manager;

  @Before
  public void setUp() throws Exception {
    testEnv = new TestEnv();
    testEnv.register(
        NAMESPACE,
        TABLE,
        Collections.singletonList(ACCOUNT_ID),
        Collections.singletonList(ACCOUNT_TYPE),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(ACCOUNT_TYPE, Scan.Ordering.Order.ASC);
          }
        },
        new HashMap<String, DataType>() {
          {
            put(ACCOUNT_ID, DataType.INT);
            put(ACCOUNT_TYPE, DataType.INT);
            put(BALANCE, DataType.INT);
          }
        });
    testEnv.createTables();

    manager = new JdbcTransactionManager(testEnv.getJdbcDatabaseConfig());
  }

  @After
  public void tearDown() throws Exception {
    testEnv.dropTables();
    testEnv.close();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
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
    DistributedTransaction transaction = manager.start();
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
    DistributedTransaction transaction = manager.start();
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
    DistributedTransaction transaction = manager.start();
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
    Value expected = new IntValue(BALANCE, INITIAL_BALANCE);
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(expected);
    DistributedTransaction transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, NAMESPACE, TABLE);
    DistributedTransaction another = manager.start();
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
    DistributedTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    int afterBalance = ((IntValue) result.get().getValue(BALANCE).get()).get() + 100;
    Value expected = new IntValue(BALANCE, afterBalance);
    Put put = preparePut(0, 0, NAMESPACE, TABLE).withValue(expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.start();
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
    DistributedTransaction another = null;
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
    DistributedTransaction transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    DistributedTransaction another = manager.start();
    Optional<Result> result1 = another.get(get);
    another.commit();
    assertThat(result1.isPresent()).isFalse();
  }

  private DistributedTransaction prepareTransfer(int fromId, int toId, int amount)
      throws TransactionException {
    DistributedTransaction transaction = manager.start();
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
    DistributedTransaction transaction = manager.start();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> {
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
                      });
            });
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
            i -> {
              IntStream.range(0, NUM_TYPES)
                  .forEach(
                      j -> {
                        gets.add(prepareGet(i, j, namespace, table));
                      });
            });
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
            i -> {
              IntStream.range(0, NUM_TYPES)
                  .forEach(
                      j -> {
                        puts.add(preparePut(i, j, namespace, table));
                      });
            });
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
}
