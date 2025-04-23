package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConsensusCommitNullMetadataIntegrationTestBase {

  private static final String TEST_NAME = "cc_null_metadata";
  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_1 = "test_table1";
  private static final String TABLE_2 = "test_table2";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
  private static final String ANY_ID_1 = "id1";

  private DistributedStorage originalStorage;
  private DistributedStorageAdmin admin;
  private DatabaseConfig databaseConfig;
  private ConsensusCommitConfig consensusCommitConfig;
  private ConsensusCommitAdmin consensusCommitAdmin;
  private String namespace1;
  private String namespace2;
  private ParallelExecutor parallelExecutor;

  private ConsensusCommitManager manager;
  private DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize();
    Properties properties = getProperties(TEST_NAME);

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, TEST_NAME);

    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    databaseConfig = new DatabaseConfig(properties);
    consensusCommitConfig = new ConsensusCommitConfig(databaseConfig);
    consensusCommitAdmin = new ConsensusCommitAdmin(admin, consensusCommitConfig, false);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    createTables();
    originalStorage = factory.getStorage();
    parallelExecutor = new ParallelExecutor(consensusCommitConfig);
  }

  protected void initialize() throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    consensusCommitAdmin.createCoordinatorTables(true, options);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    consensusCommitAdmin.createNamespace(namespace1, true, options);
    consensusCommitAdmin.createTable(namespace1, TABLE_1, tableMetadata, true, options);
    consensusCommitAdmin.createNamespace(namespace2, true, options);
    consensusCommitAdmin.createTable(namespace2, TABLE_2, tableMetadata, true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTables();
    storage = spy(originalStorage);
    coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, -1);
    recovery = spy(new RecoveryHandler(storage, coordinator, tableMetadataManager));
    CommitHandler commit =
        spy(new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor));
    manager =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recovery,
            commit);
  }

  private void truncateTables() throws ExecutionException {
    consensusCommitAdmin.truncateTable(namespace1, TABLE_1);
    consensusCommitAdmin.truncateTable(namespace2, TABLE_2);
    consensusCommitAdmin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTables();
    consensusCommitAdmin.close();
    originalStorage.close();
    parallelExecutor.close();
  }

  private void dropTables() throws ExecutionException {
    consensusCommitAdmin.dropTable(namespace1, TABLE_1);
    consensusCommitAdmin.dropNamespace(namespace1);
    consensusCommitAdmin.dropTable(namespace2, TABLE_2);
    consensusCommitAdmin.dropNamespace(namespace2);
    consensusCommitAdmin.dropCoordinatorTables();
  }

  private void populateRecordsWithNullMetadata(String namespace, String table)
      throws ExecutionException {
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Put put =
            Put.newBuilder()
                .namespace(namespace)
                .table(table)
                .partitionKey(Key.ofInt(ACCOUNT_ID, i))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, j))
                .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
                .build();
        storage.put(put);
      }
    }
  }

  private void populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
      DistributedStorage storage,
      String namespace,
      String table,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(table)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
            .value(TextColumn.of(Attribute.ID, ANY_ID_1))
            .value(IntColumn.of(Attribute.STATE, recordState.get()))
            .value(IntColumn.of(Attribute.VERSION, 1))
            .value(BigIntColumn.of(Attribute.PREPARED_AT, preparedAt))
            .value(TextColumn.ofNull(Attribute.BEFORE_ID))
            .value(IntColumn.ofNull(Attribute.BEFORE_STATE))
            .value(IntColumn.of(Attribute.BEFORE_VERSION, 0))
            .value(BigIntColumn.ofNull(Attribute.BEFORE_PREPARED_AT))
            .value(BigIntColumn.ofNull(Attribute.BEFORE_COMMITTED_AT))
            .build();
    storage.put(put);

    if (coordinatorState == null) {
      return;
    }
    Coordinator.State state = new Coordinator.State(ANY_ID_1, coordinatorState);
    coordinator.putState(state);
  }

  private Get prepareGet(int id, int type, String namespace, String table) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .consistency(Consistency.LINEARIZABLE)
        .build();
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
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .start(Key.ofInt(ACCOUNT_TYPE, fromType))
        .end(Key.ofInt(ACCOUNT_TYPE, toType))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Scan prepareScanAll(String namespace, String table) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .all()
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePut(int id, int type, String namespace, String table) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePut(int id, int type, int balance, String namespace, String table) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .value(IntColumn.of(BALANCE, balance))
        .consistency(Consistency.LINEARIZABLE)
        .build();
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
    return Delete.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Delete> prepareDeletes(String namespace, String table) {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> deletes.add(prepareDelete(i, j, namespace, table))));
    return deletes;
  }

  private int getBalance(Result result) {
    assertThat(result.getColumns()).containsKey(BALANCE);
    assertThat(result.getColumns().get(BALANCE).hasNullValue()).isFalse();
    return result.getColumns().get(BALANCE).getIntValue();
  }

  private DistributedTransaction prepareTransfer(
      int fromId,
      String fromNamespace,
      String fromTable,
      int toId,
      String toNamespace,
      String toTable,
      int amount)
      throws TransactionException {
    boolean differentTables = toNamespace.equals(fromNamespace) || !toTable.equals(fromTable);

    DistributedTransaction transaction = manager.begin();

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;
    Optional<Result> fromResult = transaction.get(fromGets.get(fromId));
    assertThat(fromResult).isPresent();
    int fromBalance = getBalance(fromResult.get()) - amount;
    Optional<Result> toResult = transaction.get(toGets.get(toId));
    assertThat(toResult).isPresent();
    int toBalance = getBalance(toResult.get()) + amount;

    List<Put> fromPuts = preparePuts(fromNamespace, fromTable);
    List<Put> toPuts = differentTables ? preparePuts(toNamespace, toTable) : fromPuts;
    Put fromPut =
        Put.newBuilder(fromPuts.get(fromId)).value(IntColumn.of(BALANCE, fromBalance)).build();
    Put toPut = Put.newBuilder(toPuts.get(toId)).value(IntColumn.of(BALANCE, toBalance)).build();
    transaction.put(fromPut);
    transaction.put(toPut);

    return transaction;
  }

  private DistributedTransaction prepareDeletes(
      int one,
      String namespace,
      String table,
      int another,
      String anotherNamespace,
      String anotherTable)
      throws TransactionException {
    boolean differentTables = !table.equals(anotherTable);

    DistributedTransaction transaction = manager.begin();

    List<Get> gets = prepareGets(namespace, table);
    List<Get> anotherGets = differentTables ? prepareGets(anotherNamespace, anotherTable) : gets;
    transaction.get(gets.get(one));
    transaction.get(anotherGets.get(another));

    List<Delete> deletes = prepareDeletes(namespace, table);
    List<Delete> anotherDeletes =
        differentTables ? prepareDeletes(anotherNamespace, anotherTable) : deletes;
    transaction.delete(deletes.get(one));
    transaction.delete(anotherDeletes.get(another));

    return transaction;
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    Optional<Result> result2 = transaction.get(get);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void
      get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime()
          throws TransactionException, ExecutionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    Optional<Result> result2 = transaction.get(get);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.COMMITTED);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isGreaterThan(0);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));

    transaction.commit();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.COMMITTED);

    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      DistributedTransaction another = manager.begin();
                      if (s instanceof Get) {
                        another.get((Get) s);
                      } else {
                        another.scan((Scan) s);
                      }
                      another.commit();
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery, times(2)).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, times(2))
        .rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isGreaterThan(0);
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);

    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      DistributedTransaction another = manager.begin();
                      if (s instanceof Get) {
                        another.get((Get) s);
                      } else {
                        another.scan((Scan) s);
                      }
                      another.commit();
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery, times(2)).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, times(2)).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    // rollback called twice but executed once actually
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.COMMITTED);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s).isPresent()).isFalse();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }
    transaction.commit();
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));

    transaction.commit();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.COMMITTED);

    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      DistributedTransaction another = manager.begin();
                      if (s instanceof Get) {
                        another.get((Get) s);
                      } else {
                        another.scan((Scan) s);
                      }
                      another.commit();
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery, times(2)).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, times(2))
        .rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s).isPresent()).isFalse();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }
    transaction.commit();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordWithNullMetadataAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);

    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      DistributedTransaction another = manager.begin();
                      if (s instanceof Get) {
                        another.get((Get) s);
                      } else {
                        another.scan((Scan) s);
                      }
                      another.commit();
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act
    assertThatThrownBy(
            () -> {
              if (s instanceof Get) {
                transaction.get((Get) s);
              } else {
                transaction.scan((Scan) s);
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery, times(2)).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery, times(2)).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    // rollback called twice but executed once actually
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scan);
  }

  @Test
  public void getThenScanAndGet_CommitHappenedInBetween_OnlyGetShouldReadRepeatably()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);

    DistributedTransaction transaction1 = manager.begin();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, 2, namespace1, TABLE_1));
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isNotEqualTo(result2);
    assertThat(result2.getInt(BALANCE)).isEqualTo(2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0, expected, namespace1, TABLE_1);
    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutWithImplicitPreReadEnabledGivenForExisting_ShouldUpdateRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);

    DistributedTransaction transaction = manager.begin();

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put =
        Put.newBuilder(preparePut(0, 0, expected, namespace1, TABLE_1))
            .enableImplicitPreRead()
            .build();
    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(1);
  }

  private void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(
      String fromNamespace, String fromTable, String toNamespace, String toTable)
      throws TransactionException, ExecutionException {
    // Arrange
    boolean differentTables = !fromNamespace.equals(toNamespace) || !fromTable.equals(toTable);

    populateRecordsWithNullMetadata(fromNamespace, fromTable);
    if (differentTables) {
      populateRecordsWithNullMetadata(toNamespace, toTable);
    }

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;

    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, fromNamespace, fromTable, to, toNamespace, toTable, amount).commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> fromResult = another.get(fromGets.get(from));
    assertThat(fromResult).isPresent();
    assertThat(fromResult.get().getColumns()).containsKey(BALANCE);
    assertThat(fromResult.get().getInt(BALANCE)).isEqualTo(fromBalance);
    Optional<Result> toResult = another.get(toGets.get(to));
    assertThat(toResult).isPresent();
    assertThat(toResult.get().getColumns()).containsKey(BALANCE);
    assertThat(toResult.get().getInt(BALANCE)).isEqualTo(toBalance);
    another.commit();
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly()
      throws TransactionException, ExecutionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly()
      throws TransactionException, ExecutionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    DistributedTransaction another = manager.begin();
    assertThat(another.get(get).isPresent()).isFalse();
    another.commit();
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException, ExecutionException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;

    populateRecordsWithNullMetadata(namespace1, table1);
    if (differentTables) {
      populateRecordsWithNullMetadata(namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareDeletes(account1, namespace1, table1, account2, namespace2, table2);

    // Act
    assertThatCode(
            () ->
                prepareDeletes(account3, namespace2, table2, account4, namespace1, table1).commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    DistributedTransaction another = manager.begin();
    assertThat(another.get(gets1.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account4)).isPresent()).isFalse();
    another.commit();
  }

  @Test
  public void commit_NonConflictingDeletesForSameTableGivenForExisting_ShouldCommitBoth()
      throws TransactionException, ExecutionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws TransactionException, ExecutionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void scanAll_ScanAllGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecordsWithNullMetadata(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Scan scanAll = prepareScanAll(namespace1, TABLE_1).withLimit(1);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
  }
}
