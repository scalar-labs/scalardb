package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.AdminTestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase {

  private static final String TEST_NAME = "cc_import_decoupling";
  private static final String NAMESPACE_BASE_NAME = "int_test_";
  private static final String STORAGE_TABLE = "test_table";
  private static final String IMPORTED_TABLE = "test_table_scalardb";
  private static final String ACCOUNT_ID = "account_id";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final String ANY_ID_1 = "id1";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ACCOUNT_ID, DataType.INT)
          .addColumn(BALANCE, DataType.INT)
          .addPartitionKey(ACCOUNT_ID)
          .build();

  private DistributedStorage originalStorage;
  private DistributedStorageAdmin admin;
  private DatabaseConfig databaseConfig;
  private ConsensusCommitConfig consensusCommitConfig;
  private ConsensusCommitAdmin consensusCommitAdmin;
  protected String namespace;
  private ParallelExecutor parallelExecutor;

  private ConsensusCommitManager manager;
  private DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;
  private RecoveryExecutor recoveryExecutor;
  @Nullable private CoordinatorGroupCommitter groupCommitter;

  private AdminTestUtils adminTestUtils;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);

    namespace = getNamespaceBaseName() + testName;

    Properties properties = getProperties(testName);

    // Add testName as a coordinator namespace suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    databaseConfig = new DatabaseConfig(properties);
    consensusCommitConfig = new ConsensusCommitConfig(databaseConfig);
    consensusCommitAdmin = new ConsensusCommitAdmin(admin, consensusCommitConfig, false);
    originalStorage = factory.getStorage();
    parallelExecutor = new ParallelExecutor(consensusCommitConfig);

    adminTestUtils = getAdminTestUtils(testName);
  }

  protected void initialize(String testName) throws Exception {}

  protected String getTestName() {
    return TEST_NAME;
  }

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  @BeforeEach
  public void setUp() throws Exception {
    dropTables();
    storage = spy(originalStorage);
    coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, -1);
    recovery = spy(new RecoveryHandler(storage, coordinator, tableMetadataManager));
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(consensusCommitConfig).orElse(null);
    CrudHandler crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            consensusCommitConfig.isIncludeMetadataEnabled(),
            parallelExecutor);
    CommitHandler commit = spy(createCommitHandler(tableMetadataManager, groupCommitter));
    manager =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit,
            consensusCommitConfig.getIsolation(),
            groupCommitter);
  }

  private CommitHandler createCommitHandler(
      TransactionTableMetadataManager tableMetadataManager,
      @Nullable CoordinatorGroupCommitter groupCommitter) {
    MutationsGrouper mutationsGrouper = new MutationsGrouper(new StorageInfoProvider(admin));
    if (groupCommitter != null) {
      return new CommitHandlerWithGroupCommit(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          true,
          false,
          groupCommitter);
    } else {
      return new CommitHandler(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          true,
          false);
    }
  }

  @AfterEach
  public void tearDown() {
    if (groupCommitter != null) {
      groupCommitter.close();
    }
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTables();
    consensusCommitAdmin.close();
    originalStorage.close();
    parallelExecutor.close();
    recoveryExecutor.close();
    adminTestUtils.close();
  }

  private void dropTables() throws ExecutionException {
    consensusCommitAdmin.dropTable(namespace, IMPORTED_TABLE, true);
    consensusCommitAdmin.dropNamespace(namespace, true);
    consensusCommitAdmin.dropCoordinatorTables(true);
  }

  private void prepareImportedTableAndRecords() throws Exception {
    createStorageTable();

    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      Put put =
          Put.newBuilder()
              .namespace(namespace)
              .table(STORAGE_TABLE)
              .partitionKey(Key.ofInt(ACCOUNT_ID, i))
              .value(IntColumn.of(BALANCE, INITIAL_BALANCE))
              .build();
      originalStorage.put(put);
    }

    adminTestUtils.truncateNamespacesTable();
    adminTestUtils.truncateMetadataTable();
    importTableWithMetadataDecoupling();

    consensusCommitAdmin.createCoordinatorTables(true, getCreationOptions());
  }

  private void prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
      TransactionState recordState, long preparedAt, TransactionState coordinatorState)
      throws Exception {
    createStorageTable();
    adminTestUtils.truncateNamespacesTable();
    adminTestUtils.truncateMetadataTable();
    importTableWithMetadataDecoupling();

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(IMPORTED_TABLE)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
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
    originalStorage.put(put);

    consensusCommitAdmin.createCoordinatorTables(true, getCreationOptions());

    if (coordinatorState == null) {
      return;
    }
    Coordinator.State state = new Coordinator.State(ANY_ID_1, coordinatorState);
    coordinator.putState(state);
  }

  private void createStorageTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(this.namespace, true, options);
    admin.createTable(namespace, STORAGE_TABLE, TABLE_METADATA, true, options);
  }

  private void importTableWithMetadataDecoupling() throws ExecutionException {
    Map<String, String> options = new HashMap<>(getCreationOptions());
    options.put("transaction_metadata_decoupling", "true");
    consensusCommitAdmin.importTable(namespace, STORAGE_TABLE, options);
  }

  private Get prepareGet(int id) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(IMPORTED_TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Get> prepareGets() {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS).forEach(i -> gets.add(prepareGet(i)));
    return gets;
  }

  private Scan prepareScanAll() {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(IMPORTED_TABLE)
        .all()
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePut(int id) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(IMPORTED_TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePut(int id, int balance) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(IMPORTED_TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .value(IntColumn.of(BALANCE, balance))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS).forEach(i -> puts.add(preparePut(i)));
    return puts;
  }

  private Delete prepareDelete(int id) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(IMPORTED_TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS).forEach(i -> deletes.add(prepareDelete(i)));
    return deletes;
  }

  private int getBalance(Result result) {
    assertThat(result.getColumns()).containsKey(BALANCE);
    assertThat(result.getColumns().get(BALANCE).hasNullValue()).isFalse();
    return result.getColumns().get(BALANCE).getIntValue();
  }

  private DistributedTransaction prepareTransfer(int fromId, int toId, int amount)
      throws TransactionException {
    DistributedTransaction transaction = manager.begin();

    List<Get> gets = prepareGets();
    Optional<Result> fromResult = transaction.get(gets.get(fromId));
    assertThat(fromResult).isPresent();
    int fromBalance = getBalance(fromResult.get()) - amount;
    Optional<Result> toResult = transaction.get(gets.get(toId));
    assertThat(toResult).isPresent();
    int toBalance = getBalance(toResult.get()) + amount;

    List<Put> puts = preparePuts();
    Put fromPut =
        Put.newBuilder(puts.get(fromId)).value(IntColumn.of(BALANCE, fromBalance)).build();
    Put toPut = Put.newBuilder(puts.get(toId)).value(IntColumn.of(BALANCE, toBalance)).build();
    transaction.put(fromPut);
    transaction.put(toPut);

    return transaction;
  }

  private DistributedTransaction prepareDeletes(int one, int another) throws TransactionException {
    DistributedTransaction transaction = manager.begin();

    List<Get> gets = prepareGets();
    transaction.get(gets.get(one));
    transaction.get(gets.get(another));

    List<Delete> deletes = prepareDeletes();
    transaction.delete(deletes.get(one));
    transaction.delete(deletes.get(another));

    return transaction;
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0);

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
  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0);

    // Act
    Optional<Result> result1 = transaction.get(get);
    Optional<Result> result2 = transaction.get(get);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void scanAll_ScanAllGivenForCommittedRecord_ShouldReturnRecord() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();
    DistributedTransaction transaction = manager.begin();
    Scan scanAll = Scan.newBuilder(prepareScanAll()).limit(1).build();

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws Exception {
    // Arrange
    long current = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.PREPARED, current, TransactionState.COMMITTED);
    DistributedTransaction transaction = manager.begin();

    // Act
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isGreaterThan(0);

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws Exception {
    // Arrange
    long current = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.PREPARED, current, TransactionState.ABORTED);
    DistributedTransaction transaction = manager.begin();

    // Act
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

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback() throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws Exception {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.PREPARED, prepared_at, null);
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

    transaction.rollback();

    // Assert
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws Exception {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.PREPARED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
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

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws Exception {
    // Arrange
    long current = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.DELETED, current, TransactionState.COMMITTED);
    DistributedTransaction transaction = manager.begin();

    // Act
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s).isPresent()).isFalse();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws Exception {
    // Arrange
    long current = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.DELETED, current, TransactionState.ABORTED);
    DistributedTransaction transaction = manager.begin();

    // Act
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

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback() throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws Exception {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.DELETED, prepared_at, null);
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

    transaction.rollback();

    // Assert
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws Exception {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    prepareImportedTableAndPreparedRecordWithNullAndCoordinatorStateRecord(
        TransactionState.DELETED, prepared_at, null);
    DistributedTransaction transaction = manager.begin();

    // Act
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

    assertThat(result.getId()).isNull();
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(0);
    assertThat(result.getCommittedAt()).isEqualTo(0);

    transaction.commit();

    // Wait for the recovery to complete
    ((ConsensusCommit) transaction).waitForRecoveryCompletion();

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws Exception {
    Get get = prepareGet(0);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws Exception {
    Scan scanAll = prepareScanAll();
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();
    Get get = prepareGet(0);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, expected);
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
      throws Exception {
    // Arrange
    prepareImportedTableAndRecords();

    DistributedTransaction transaction = manager.begin();

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put = Put.newBuilder(preparePut(0, expected)).enableImplicitPreRead().build();
    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(prepareGet(0));
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();

    List<Get> gets = prepareGets();

    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int from = 0;
    int to = 1;

    // Act
    prepareTransfer(from, to, amount).commit();

    // Assert
    DistributedTransaction another = manager.begin();
    Optional<Result> fromResult = another.get(gets.get(from));
    assertThat(fromResult).isPresent();
    assertThat(fromResult.get().getColumns()).containsKey(BALANCE);
    assertThat(fromResult.get().getInt(BALANCE)).isEqualTo(fromBalance);
    Optional<Result> toResult = another.get(gets.get(to));
    assertThat(toResult).isPresent();
    assertThat(toResult.get().getColumns()).containsKey(BALANCE);
    assertThat(toResult.get().getInt(BALANCE)).isEqualTo(toBalance);
    another.commit();
  }

  @Test
  public void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth() throws Exception {
    // Arrange
    int account1 = 0;
    int account2 = 1;
    int account3 = 2;
    int account4 = 3;

    prepareImportedTableAndRecords();

    DistributedTransaction transaction = prepareDeletes(account1, account2);

    // Act
    assertThatCode(() -> prepareDeletes(account3, account4).commit()).doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets = prepareGets();
    DistributedTransaction another = manager.begin();
    assertThat(another.get(gets.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account4)).isPresent()).isFalse();
    another.commit();
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord() throws Exception {
    // Arrange
    prepareImportedTableAndRecords();
    Get get = prepareGet(0);
    Delete delete = prepareDelete(0);
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
}
