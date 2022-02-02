package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.TestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class ConsensusCommitIntegrationTestBase {

  private static final String TEST_NAME = "cc";
  private static final String NAMESPACE_1 = "integration_testing_" + TEST_NAME + "1";
  private static final String NAMESPACE_2 = "integration_testing_" + TEST_NAME + "2";
  private static final String TABLE_1 = "test_table1";
  private static final String TABLE_2 = "test_table2";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";

  private static boolean initialized;
  private static DistributedStorage originalStorage;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitConfig consensusCommitConfig;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static String namespace1;
  private static String namespace2;
  private static TransactionalTableMetadataManager tableMetadataManager;
  private static ParallelExecutor parallelExecutor;

  private ConsensusCommitManager manager;
  private DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      DatabaseConfig config = TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME);
      StorageFactory factory = new StorageFactory(config);
      admin = factory.getAdmin();
      consensusCommitConfig = new ConsensusCommitConfig(config.getProperties());
      consensusCommitAdmin = new ConsensusCommitAdmin(admin, consensusCommitConfig);
      namespace1 = getNamespace1();
      namespace2 = getNamespace2();
      createTables();
      originalStorage = factory.getStorage();
      tableMetadataManager = new TransactionalTableMetadataManager(admin, -1);
      parallelExecutor = new ParallelExecutor(consensusCommitConfig);
      initialized = true;
    }

    truncateTables();
    storage = spy(originalStorage);
    coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    recovery =
        spy(new RecoveryHandler(storage, coordinator, tableMetadataManager, parallelExecutor));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery, parallelExecutor));
    manager =
        new ConsensusCommitManager(
            storage, admin, consensusCommitConfig, coordinator, parallelExecutor, recovery, commit);
  }

  protected void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    admin.createNamespace(namespace1, true, options);
    consensusCommitAdmin.createTransactionalTable(
        namespace1, TABLE_1, tableMetadata, true, options);
    admin.createNamespace(namespace2, true, options);
    consensusCommitAdmin.createTransactionalTable(
        namespace2, TABLE_2, tableMetadata, true, options);
    consensusCommitAdmin.createCoordinatorTable(options);
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void truncateTables() throws ExecutionException {
    admin.truncateTable(namespace1, TABLE_1);
    admin.truncateTable(namespace2, TABLE_2);
    consensusCommitAdmin.truncateCoordinatorTable();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    admin.close();
    originalStorage.close();
    parallelExecutor.close();
  }

  private static void deleteTables() throws ExecutionException {
    admin.dropTable(namespace1, TABLE_1);
    admin.dropNamespace(namespace1);
    admin.dropTable(namespace2, TABLE_2);
    admin.dropNamespace(namespace2);
    consensusCommitAdmin.dropCoordinatorTable();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isTrue();
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime()
      throws CrudException, ExecutionException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    Optional<Result> result2 = transaction.get(get);

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void
      get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime()
          throws CrudException, ExecutionException, CommitException,
              UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    populateRecords(namespace1, TABLE_1);
    Optional<Result> result2 = transaction.get(get);

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 4, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();
    Scan scan = prepareScan(0, 4, 4, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s)
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();

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

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    assertThat(result.getId()).isEqualTo(ANY_ID_2);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(2);
    assertThat(result.getCommittedAt()).isEqualTo(0);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s)
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();

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

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  //  private void
  //
  // selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //          Selection s)
  //          throws ExecutionException, CoordinatorException, RecoveryException, CrudException {
  //    // Arrange
  //    long prepared_at = System.currentTimeMillis();
  //    populatePreparedRecordAndCoordinatorStateRecord(
  //        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null);
  //    ConsensusCommit transaction = manager.start();
  //
  //    // Act
  //    TransactionResult result;
  //    if (s instanceof Get) {
  //      Optional<Result> r = transaction.get((Get) s);
  //      assertThat(r).isPresent();
  //      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
  //    } else {
  //      List<Result> results = transaction.scan((Scan) s);
  //      assertThat(results.size()).isEqualTo(1);
  //      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
  //    }
  //
  //    // Assert
  //    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
  //    verify(recovery, never()).rollbackRecord(any(Selection.class),
  // any(TransactionResult.class));
  //    verify(coordinator, never()).putState(any(Coordinator.State.class));
  //
  //    assertThat(result.getId()).isEqualTo(ANY_ID_1);
  //    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
  //    assertThat(result.getVersion()).isEqualTo(1);
  //    assertThat(result.getCommittedAt()).isEqualTo(1);
  //  }
  //
  //  @Test
  //  public void
  //
  // get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
  //          throws ExecutionException, CoordinatorException, RecoveryException, CrudException {
  //    Get get = prepareGet(0, 0, namespace1, TABLE_1);
  //
  // selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //        get);
  //  }
  //
  //  @Test
  //  public void
  //
  // scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
  //          throws ExecutionException, CoordinatorException, RecoveryException, CrudException {
  //    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
  //
  // selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //        scan);
  //  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s)
          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(coordinator).putState(new Coordinator.State(ANY_ID_2, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s)
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();

    // Act Assert
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s)).isNotPresent();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s)
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();

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

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  //  private void
  //
  // selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //          Selection s)
  //          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
  //    // Arrange
  //    long prepared_at = System.currentTimeMillis();
  //    populatePreparedRecordAndCoordinatorStateRecord(
  //        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null);
  //    ConsensusCommit transaction = manager.start();
  //
  //    // Act
  //    TransactionResult result;
  //    if (s instanceof Get) {
  //      Optional<Result> r = transaction.get((Get) s);
  //      assertThat(r).isPresent();
  //      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
  //    } else {
  //      List<Result> results = transaction.scan((Scan) s);
  //      assertThat(results.size()).isEqualTo(1);
  //      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
  //    }
  //
  //    // Assert
  //    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
  //    verify(recovery, never()).rollbackRecord(any(Selection.class),
  // any(TransactionResult.class));
  //    verify(coordinator, never()).putState(any(Coordinator.State.class));
  //
  //    assertThat(result.getId()).isEqualTo(ANY_ID_1);
  //    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
  //    assertThat(result.getVersion()).isEqualTo(1);
  //    assertThat(result.getCommittedAt()).isEqualTo(1);
  //  }
  //
  //  @Test
  //  public void
  //
  // get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
  //          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
  //    Get get = prepareGet(0, 0, namespace1, TABLE_1);
  //
  // selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //        get);
  //  }
  //
  //  @Test
  //  public void
  //
  // scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
  //          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
  //    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
  //
  // selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
  //        scan);
  //  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s)
          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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

    // Assert
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class));
    verify(coordinator).putState(new Coordinator.State(ANY_ID_2, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException, RecoveryException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    ConsensusCommit transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    ConsensusCommit transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    ConsensusCommit another = manager.start();

    Optional<Result> r = another.get(get);
    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    ConsensusCommit another = manager.start();
    Optional<Result> r = another.get(get);
    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowCommitException()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(0).withValue(BALANCE, 1100);
    ConsensusCommit transaction = manager.start();

    // Act Assert
    transaction.put(puts.get(0));
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
  }

  @Test
  public void
      putAndCommit_SinglePartitionMutationsGiven_ShouldAccessStorageOnceForPrepareAndCommit()
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException {
    // Arrange
    IntValue balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(1).withValue(balance);
    ConsensusCommit transaction = manager.start();

    // Act
    transaction.put(puts.get(0));
    transaction.put(puts.get(1));
    transaction.commit();

    // Assert
    // one for prepare, one for commit
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(any(Coordinator.State.class));
  }

  @Test
  public void putAndCommit_TwoPartitionsMutationsGiven_ShouldAccessStorageTwiceForPrepareAndCommit()
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException {
    // Arrange
    IntValue balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(NUM_TYPES).withValue(balance); // next account
    ConsensusCommit transaction = manager.start();

    // Act
    transaction.put(puts.get(0));
    transaction.put(puts.get(NUM_TYPES));
    transaction.commit();

    // Assert
    // twice for prepare, twice for commit
    verify(storage, times(4)).mutate(anyList());
    verify(coordinator).putState(any(Coordinator.State.class));
  }

  private void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(
      String fromNamespace, String fromTable, String toNamespace, String toTable)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !fromNamespace.equals(toNamespace) || !fromTable.equals(toTable);

    populateRecords(fromNamespace, fromTable);
    if (differentTables) {
      populateRecords(toNamespace, toTable);
    }

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;

    int amount = 100;
    IntValue fromBalance = new IntValue(BALANCE, INITIAL_BALANCE - amount);
    IntValue toBalance = new IntValue(BALANCE, INITIAL_BALANCE + amount);
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, fromNamespace, fromTable, to, toNamespace, toTable, amount).commit();

    // Assert
    ConsensusCommit another = manager.start();
    Optional<Result> fromResult = another.get(fromGets.get(from));
    assertThat(fromResult).isPresent();
    assertThat(fromResult.get().getValue(BALANCE)).isEqualTo(Optional.of(fromBalance));
    Optional<Result> toResult = another.get(toGets.get(to));
    assertThat(toResult).isPresent();
    assertThat(toResult.get().getValue(BALANCE)).isEqualTo(Optional.of(toBalance));
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2) throws CrudException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int expected = INITIAL_BALANCE;
    List<Put> puts1 = preparePuts(namespace1, table1);
    List<Put> puts2 = differentTables ? preparePuts(namespace2, table2) : puts1;

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts1.get(from).withValue(BALANCE, expected);
    puts2.get(to).withValue(BALANCE, expected);

    ConsensusCommit transaction = manager.start();
    transaction.setBeforeCommitHook(
        () -> {
          ConsensusCommit another = manager.start();
          puts1.get(anotherTo).withValue(BALANCE, expected);
          assertThatCode(
                  () -> {
                    another.put(puts2.get(anotherFrom));
                    another.put(puts1.get(anotherTo));
                    another.commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act Assert
    transaction.put(puts1.get(from));
    transaction.put(puts2.get(to));
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollbackRecords(any(Snapshot.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).isPresent()).isFalse();
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(expected);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(expected);
  }

  @Test
  public void
      commit_ConflictingPutsForSameTableGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitConflictException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitConflictException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
      String namespace1, String table1, String namespace2, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    ConsensusCommit transaction = manager.start();
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Delete> deletes1 = prepareDeletes(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    List<Delete> deletes2 = differentTables ? prepareDeletes(namespace2, table2) : deletes1;

    transaction.get(gets1.get(from));
    transaction.delete(deletes1.get(from));
    transaction.get(gets2.get(to));
    transaction.delete(deletes2.get(to));
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () ->
                        prepareTransfer(
                                anotherFrom,
                                namespace2,
                                table2,
                                anotherTo,
                                namespace1,
                                table1,
                                amount)
                            .commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollbackRecords(any(Snapshot.class));
    ConsensusCommit another = manager.start();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount);
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForSameTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForDifferentTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    ConsensusCommit transaction =
        prepareTransfer(from, namespace1, table1, to, namespace2, table2, amount1);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () ->
                        prepareTransfer(
                                anotherFrom,
                                namespace2,
                                table2,
                                anotherTo,
                                namespace1,
                                table1,
                                amount2)
                            .commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollbackRecords(any(Snapshot.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    ConsensusCommit another = manager.start();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount2);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount2);
  }

  @Test
  public void commit_ConflictingPutsForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
      String namespace1, String table1, String namespace2, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = NUM_TYPES * 2;
    int anotherTo = NUM_TYPES * 3;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    ConsensusCommit transaction =
        prepareTransfer(from, namespace1, table1, to, namespace2, table2, amount1);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () ->
                        prepareTransfer(
                                anotherFrom,
                                namespace2,
                                table2,
                                anotherTo,
                                namespace1,
                                table1,
                                amount2)
                            .commit())
                .doesNotThrowAnyException());

    // Act
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    ConsensusCommit another = manager.start();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE - amount1);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE + amount1);
    Optional<Result> anotherFromResult = another.get(gets2.get(anotherFrom));
    assertThat(anotherFromResult).isPresent();
    assertThat(getBalance(anotherFromResult.get())).isEqualTo(INITIAL_BALANCE - amount2);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount2);
  }

  @Test
  public void commit_NonConflictingPutsForSameTableGivenForExisting_ShouldCommitBoth()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws CrudException {
    // Arrange
    int expected = INITIAL_BALANCE;
    List<Put> puts1 = preparePuts(namespace1, TABLE_1);
    List<Put> puts2 = preparePuts(namespace2, TABLE_2);

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = from;
    int anotherTo = to;
    puts1.get(from).withValue(BALANCE, expected);
    puts1.get(to).withValue(BALANCE, expected);

    ConsensusCommit transaction = manager.start();
    transaction.setBeforeCommitHook(
        () -> {
          ConsensusCommit another = manager.start();
          puts2.get(from).withValue(BALANCE, expected);
          puts2.get(to).withValue(BALANCE, expected);
          assertThatCode(
                  () -> {
                    another.put(puts2.get(anotherFrom));
                    another.put(puts2.get(anotherTo));
                    another.commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act Assert
    transaction.put(puts1.get(from));
    transaction.put(puts1.get(to));
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, TABLE_1);
    List<Get> gets2 = prepareGets(namespace2, TABLE_2);
    ConsensusCommit another = manager.start();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(expected);
    Optional<Result> toResult = another.get(gets1.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(expected);
    Optional<Result> anotherFromResult = another.get(gets2.get(anotherFrom));
    assertThat(anotherFromResult).isPresent();
    assertThat(getBalance(anotherFromResult.get())).isEqualTo(expected);
    Optional<Result> anotherToResult = another.get(gets2.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(expected);
  }

  @Test
  public void commit_DeleteGivenWithoutRead_ShouldThrowIllegalArgumentException() {
    // Arrange
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();

    // Act Assert
    transaction.delete(delete);
    assertThatCode(transaction::commit)
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void commit_DeleteGivenForNonExisting_ShouldThrowIllegalArgumentException()
      throws CrudException {
    // Arrange
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();

    // Act Assert
    transaction.get(get);
    transaction.delete(delete);
    assertThatCode(transaction::commit)
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    ConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    ConsensusCommit another = manager.start();
    assertThat(another.get(get).isPresent()).isFalse();
  }

  private void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    ConsensusCommit transaction =
        prepareDeletes(account1, namespace1, table1, account2, namespace2, table2);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () ->
                        prepareDeletes(account2, namespace2, table2, account3, namespace1, table1)
                            .commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollbackRecords(any(Snapshot.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    ConsensusCommit another = manager.start();
    Optional<Result> result = another.get(gets1.get(account1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account3)).isPresent()).isFalse();
  }

  @Test
  public void
      commit_ConflictingDeletesForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      String namespace1, String table1, String namespace2, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    ConsensusCommit transaction =
        prepareDeletes(account1, namespace1, table1, account2, namespace2, table2);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () ->
                        prepareDeletes(account3, namespace2, table2, account4, namespace1, table1)
                            .commit())
                .doesNotThrowAnyException());

    // Act
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account4)).isPresent()).isFalse();
  }

  @Test
  public void commit_NonConflictingDeletesForSameTableGivenForExisting_ShouldCommitBoth()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
      String namespace1, String table1, String namespace2, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction = manager.start();
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    ConsensusCommit transaction2 = manager.start();

    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    assertThat(result1).isPresent();
    int current1 = getBalance(result1.get());
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    assertThat(result2).isPresent();
    int current2 = getBalance(result2.get());
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace2, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    transaction2.commit();

    // Assert
    transaction = manager.start();
    // the results can not be produced by executing the transactions serially
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(2);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSnapshot_ShouldProduceNonSerializableResult()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSnapshot_ShouldProduceNonSerializableResult()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    assertThat(result1).isPresent();
    int current1 = getBalance(result1.get());
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    assertThat(result2).isPresent();
    int current2 = getBalance(result2.get());
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace2, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);

    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    assertThat(result1).isPresent();
    int current1 = getBalance(result1.get());
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    assertThat(result2).isPresent();
    int current2 = getBalance(result2.get());
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace2, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2) throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    int current1 = 0;
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    int current2 = 0;
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace2, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    assertThat(result1.isPresent()).isFalse();
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
          String namespace1, String table1, String namespace2, String table2)
          throws CrudException, CoordinatorException {
    // Arrange
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.ABORTED);
    coordinator.putState(state);

    // Act
    ConsensusCommit transaction1 =
        manager.start(ANY_ID_1, Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction1.get(get1_2);
    int current1 = 0;
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Throwable thrown1 = catchThrowable(transaction1::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get1_2);
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    assertThat(thrown1).isInstanceOf(CommitException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws CrudException, CoordinatorException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTableWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws CrudException, CoordinatorException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2) throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    int current1 = 0;
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    int current2 = 0;
    Put put1 = preparePut(0, 0, namespace1, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace2, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    assertThat(result1.isPresent()).isFalse();
    result2 = transaction.get(get2_1);
    assertThat(result2.isPresent()).isTrue();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowCommitConflictException()
          throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    assertThat(thrown1).isInstanceOf(CommitConflictException.class);
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitConflictException()
          throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result1.isPresent()).isTrue();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result2.isPresent()).isFalse();
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitConflictException()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(3);
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void scanAndCommit_MultipleScansGivenInTransactionWithExtraRead_ShouldCommitProperly()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(namespace1, TABLE_1);

    // Act Assert
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.scan(prepareScan(0, namespace1, TABLE_1));
    transaction.scan(prepareScan(1, namespace1, TABLE_1));
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance1 = 0;
    if (result1.isPresent()) {
      balance1 = getBalance(result1.get());
    }
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance1 + 1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    ConsensusCommit transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    ConsensusCommit transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void get_PutCalledBefore_ShouldGet() throws CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();

    // Act
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = transaction.get(get);
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(get);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void scan_DeleteCalledBefore_ShouldReturnEmpty()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    List<Result> resultBefore = transaction1.scan(scan);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    List<Result> resultAfter = transaction1.scan(scan);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void delete_PutCalledBefore_ShouldDelete()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    ConsensusCommit transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Throwable thrown =
        catchThrowable(
            () -> transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2)));
    transaction1.abort();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException() {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.abort();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 1, 1, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void scan_DeleteGivenBefore_ShouldScan()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Scan scan = prepareScan(0, 0, 1, namespace1, TABLE_1);
    List<Result> results = transaction1.scan(scan);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void start_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    String transactionId = ANY_ID_1;

    // Act Assert
    assertThatCode(
            () -> {
              ConsensusCommit transaction = manager.start(transactionId);
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
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    TransactionState state = manager.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.commit();

    assertThatCode(transaction1::commit).isInstanceOf(CommitException.class);

    // Act
    TransactionState state = manager.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager.abort(transaction.getId());

    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    TransactionState state = manager.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  private ConsensusCommit prepareTransfer(
      int fromId,
      String fromNamespace,
      String fromTable,
      int toId,
      String toNamespace,
      String toTable,
      int amount)
      throws CrudException {
    boolean differentTables = toNamespace.equals(fromNamespace) || !toTable.equals(fromTable);

    ConsensusCommit transaction = manager.start();

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;
    Optional<Result> fromResult = transaction.get(fromGets.get(fromId));
    assertThat(fromResult).isPresent();
    IntValue fromBalance = new IntValue(BALANCE, getBalance(fromResult.get()) - amount);
    Optional<Result> toResult = transaction.get(toGets.get(toId));
    assertThat(toResult).isPresent();
    IntValue toBalance = new IntValue(BALANCE, getBalance(toResult.get()) + amount);

    List<Put> fromPuts = preparePuts(fromNamespace, fromTable);
    List<Put> toPuts = differentTables ? preparePuts(toNamespace, toTable) : fromPuts;
    fromPuts.get(fromId).withValue(fromBalance);
    toPuts.get(toId).withValue(toBalance);
    transaction.put(fromPuts.get(fromId));
    transaction.put(toPuts.get(toId));

    return transaction;
  }

  private ConsensusCommit prepareDeletes(
      int one,
      String namespace,
      String table,
      int another,
      String anotherNamespace,
      String anotherTable)
      throws CrudException {
    boolean differentTables = !table.equals(anotherTable);

    ConsensusCommit transaction = manager.start();

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

  private void populateRecords(String namespace, String table)
      throws CommitException, UnknownTransactionStatusException {
    ConsensusCommit transaction = manager.start();
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
                                  .forNamespace(namespace)
                                  .forTable(table)
                                  .withValue(BALANCE, INITIAL_BALANCE);
                          transaction.put(put);
                        }));
    transaction.commit();
  }

  private void populatePreparedRecordAndCoordinatorStateRecord(
      DistributedStorage storage,
      String namespace,
      String table,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = new Key(ACCOUNT_ID, 0);
    Key clusteringKey = new Key(ACCOUNT_TYPE, 0);
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(namespace)
            .forTable(table)
            .withValue(BALANCE, INITIAL_BALANCE)
            .withValue(Attribute.toIdValue(ANY_ID_2))
            .withValue(Attribute.toStateValue(recordState))
            .withValue(Attribute.toVersionValue(2))
            .withValue(Attribute.toPreparedAtValue(preparedAt))
            .withValue(Attribute.toBeforeIdValue(ANY_ID_1))
            .withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED))
            .withValue(Attribute.toBeforeVersionValue(1))
            .withValue(Attribute.toBeforePreparedAtValue(1))
            .withValue(Attribute.toBeforeCommittedAtValue(1));
    storage.put(put);

    if (coordinatorState == null) {
      return;
    }
    Coordinator.State state = new Coordinator.State(ANY_ID_2, coordinatorState);
    coordinator.putState(state);
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

  private Scan prepareScan(int id, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
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
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }
}
