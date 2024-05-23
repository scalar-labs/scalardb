package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.Arrays;
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
public abstract class ConsensusCommitSpecificIntegrationTestBase {

  private static final String TEST_NAME = "cc";
  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_1 = "test_table1";
  private static final String TABLE_2 = "test_table2";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final String SOME_COLUMN = "some_column";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NEW_BALANCE = 2000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";

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
  private CommitHandler commit;

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
            .addColumn(SOME_COLUMN, DataType.TEXT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .addSecondaryIndex(BALANCE)
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
    commit = spy(new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor));
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

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
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
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
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
    populateRecords(namespace1, TABLE_1);
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
    populateRecords(namespace1, TABLE_1);
    Optional<Result> result2 = transaction.get(get);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Get get = prepareGet(0, 4, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Scan scan = prepareScan(0, 4, 4, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_2);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(2);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
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
    verify(coordinator).putState(new Coordinator.State(ANY_ID_2, TransactionState.ABORTED));
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_2);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(2);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
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
    verify(coordinator).putState(new Coordinator.State(ANY_ID_2, TransactionState.ABORTED));
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
    populatePreparedRecordAndCoordinatorStateRecord(
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
    populatePreparedRecordAndCoordinatorStateRecord(
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

    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
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
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    DistributedTransaction transaction1 = manager.begin();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutWithImplicitPreReadEnabledGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put = preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));

    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @Test
  public void putAndCommit_PutWithImplicitPreReadEnabledGivenForExisting_ShouldUpdateRecord()
      throws TransactionException, ExecutionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    transaction.put(put);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));

    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @Test
  public void putAndCommit_PutGivenForExisting_ShouldThrowCommitConflictException()
      throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE + 100)
            .build();
    transaction.put(put);
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void putAndCommit_PutWithInsertModeEnabledGivenForNonExisting_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableInsertMode()
            .build();
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.begin();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutWithInsertModeEnabledGivenForNonExistingAfterRead_ShouldCreateRecord()
      throws TransactionException {
    // Arrange
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableInsertMode()
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isNotPresent();

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
  public void
      putAndCommit_PutWithInsertModeGivenForExistingAfterRead_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();

    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, getBalance(result.get()) + 100)
            .enableInsertMode()
            .build();
    transaction.put(put);
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      putAndCommit_SinglePartitionMutationsGiven_ShouldAccessStorageOnceForPrepareAndCommit()
          throws TransactionException, ExecutionException, CoordinatorException {
    // Arrange
    IntValue balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(1).withValue(balance);
    DistributedTransaction transaction = manager.begin();

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
      throws TransactionException, ExecutionException, CoordinatorException {
    // Arrange
    IntValue balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(namespace1, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(NUM_TYPES).withValue(balance); // next account
    DistributedTransaction transaction = manager.begin();

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
      throws TransactionException {
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
    DistributedTransaction another = manager.begin();
    Optional<Result> fromResult = another.get(fromGets.get(from));
    assertThat(fromResult).isPresent();
    assertThat(fromResult.get().getValue(BALANCE)).isEqualTo(Optional.of(fromBalance));
    Optional<Result> toResult = another.get(toGets.get(to));
    assertThat(toResult).isPresent();
    assertThat(toResult.get().getValue(BALANCE)).isEqualTo(Optional.of(toBalance));
    another.commit();
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly()
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly()
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int expected = INITIAL_BALANCE;
    List<Put> puts1 = preparePuts(namespace1, table1);
    List<Put> puts2 = differentTables ? preparePuts(namespace2, table2) : puts1;
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts1.get(from).withValue(BALANCE, expected);
    puts2.get(to).withValue(BALANCE, expected);

    DistributedTransaction transaction1 = manager.begin();
    transaction1.get(gets1.get(from));
    transaction1.get(gets2.get(to));
    transaction1.put(puts1.get(from));
    transaction1.put(puts2.get(to));

    DistributedTransaction transaction2 = manager.begin();
    puts1.get(anotherTo).withValue(BALANCE, expected);

    // Act Assert
    assertThatCode(
            () -> {
              transaction2.put(puts2.get(anotherFrom));
              transaction2.put(puts1.get(anotherTo));
              transaction2.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction1::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(Snapshot.class));

    DistributedTransaction another = manager.begin();
    assertThat(another.get(gets1.get(from)).isPresent()).isFalse();
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(expected);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(expected);
    another.commit();
  }

  @Test
  public void
      commit_ConflictingPutsForSameTableGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
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

    DistributedTransaction transaction = manager.begin();
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Delete> deletes1 = prepareDeletes(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    List<Delete> deletes2 = differentTables ? prepareDeletes(namespace2, table2) : deletes1;
    transaction.get(gets1.get(from));
    transaction.delete(deletes1.get(from));
    transaction.get(gets2.get(to));
    transaction.delete(deletes2.get(to));

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        anotherFrom, namespace2, table2, anotherTo, namespace1, table1, amount)
                    .commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(Snapshot.class));
    DistributedTransaction another = manager.begin();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount);
    another.commit();
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForSameTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForDifferentTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
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

    DistributedTransaction transaction =
        prepareTransfer(from, namespace1, table1, to, namespace2, table2, amount1);

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        anotherFrom, namespace2, table2, anotherTo, namespace1, table1, amount2)
                    .commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(Snapshot.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    DistributedTransaction another = manager.begin();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount2);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount2);
    another.commit();
  }

  @Test
  public void commit_ConflictingPutsForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
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

    DistributedTransaction transaction =
        prepareTransfer(from, namespace1, table1, to, namespace2, table2, amount1);

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        anotherFrom, namespace2, table2, anotherTo, namespace1, table1, amount2)
                    .commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    DistributedTransaction another = manager.begin();
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
    another.commit();
  }

  @Test
  public void commit_NonConflictingPutsForSameTableGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws TransactionException {
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

    DistributedTransaction transaction1 = manager.begin();
    transaction1.put(puts1.get(from));
    transaction1.put(puts1.get(to));

    DistributedTransaction transaction2 = manager.begin();
    puts2.get(from).withValue(BALANCE, expected);
    puts2.get(to).withValue(BALANCE, expected);

    // Act Assert
    assertThatCode(
            () -> {
              transaction2.put(puts2.get(anotherFrom));
              transaction2.put(puts2.get(anotherTo));
              transaction2.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, TABLE_1);
    List<Get> gets2 = prepareGets(namespace2, TABLE_2);
    DistributedTransaction another = manager.begin();
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
    another.commit();
  }

  @Test
  public void commit_DeleteGivenWithoutRead_ShouldNotThrowAnyExceptions()
      throws TransactionException {
    // Arrange
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    transaction.delete(delete);
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void commit_DeleteGivenForNonExisting_ShouldNotThrowAnyExceptions()
      throws TransactionException {
    // Arrange
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    transaction.get(get);
    transaction.delete(delete);
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
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

  private void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;

    populateRecords(namespace1, table1);
    if (differentTables) {
      populateRecords(namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareDeletes(account1, namespace1, table1, account2, namespace2, table2);

    // Act
    assertThatCode(
            () ->
                prepareDeletes(account2, namespace2, table2, account3, namespace1, table1).commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(Snapshot.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    DistributedTransaction another = manager.begin();
    Optional<Result> result = another.get(gets1.get(account1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account3)).isPresent()).isFalse();
    another.commit();
  }

  @Test
  public void
      commit_ConflictingDeletesForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
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
      throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
      String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    DistributedTransaction transaction = manager.begin();
    transaction.put(puts);
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    DistributedTransaction transaction2 = manager.begin();

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
    transaction = manager.begin();
    // the results can not be produced by executing the transactions serially
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(2);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSnapshot_ShouldProduceNonSerializableResult()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSnapshot_ShouldProduceNonSerializableResult()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    transaction.put(puts);
    transaction.commit();

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    transaction = manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    transaction.commit();

    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, table1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace2, table2).withValue(BALANCE, 1));
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);

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
    transaction = manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(2);
    transaction.commit();

    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws TransactionException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws TransactionException {
    // Arrange
    // no records

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    assertThat(result1.isPresent()).isFalse();
    result2 = transaction.get(get2_1);
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    transaction.commit();

    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
          String namespace1, String table1, String namespace2, String table2)
          throws TransactionException, CoordinatorException {
    // Arrange
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.ABORTED);
    coordinator.putState(state);

    // Act
    DistributedTransaction transaction1 =
        manager.begin(ANY_ID_1, Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get1_2);
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    transaction.commit();

    assertThat(thrown1).isInstanceOf(CommitException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws TransactionException, CoordinatorException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTableWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws TransactionException, CoordinatorException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String namespace1, String table1, String namespace2, String table2)
          throws TransactionException {
    // Arrange
    // no records

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
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
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    assertThat(result1.isPresent()).isFalse();
    result2 = transaction.get(get2_1);
    assertThat(result2.isPresent()).isTrue();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    transaction.commit();

    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws TransactionException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    // no records

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    transaction.commit();

    assertThat(thrown1).isInstanceOf(CommitConflictException.class);
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    // no records

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
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
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result1.isPresent()).isTrue();
    assertThat(getBalance(result1.get())).isEqualTo(1);
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result2.isPresent()).isFalse();
    transaction.commit();

    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1),
            preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    DistributedTransaction transaction1 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction2 =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
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
    transaction = manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result1).isPresent();
    assertThat(getBalance(result1.get())).isEqualTo(3);
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result2).isPresent();
    assertThat(getBalance(result2.get())).isEqualTo(1);
    transaction.commit();

    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void scanAndCommit_MultipleScansGivenInTransactionWithExtraRead_ShouldCommitProperly()
      throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);

    // Act Assert
    DistributedTransaction transaction =
        manager.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.scan(prepareScan(0, namespace1, TABLE_1));
    transaction.scan(prepareScan(1, namespace1, TABLE_1));
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance1 = 0;
    if (result1.isPresent()) {
      balance1 = getBalance(result1.get());
    }
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance1 + 1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    DistributedTransaction transaction3 = manager.begin();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.begin();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.commit();
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    DistributedTransaction transaction3 = manager.begin();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.begin();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.commit();
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void get_PutCalledBefore_ShouldGet() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();

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
  public void get_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
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
  public void scan_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
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
  public void delete_PutCalledBefore_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    DistributedTransaction transaction2 = manager.begin();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Throwable thrown =
        catchThrowable(
            () -> transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2)));
    transaction1.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 1, 1, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      scan_NonOverlappingPutGivenButOverlappingPutExists_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 9999));
    Scan scan =
        Scan.newBuilder(prepareScan(0, 1, 1, namespace1, TABLE_1))
            .where(ConditionBuilder.column(BALANCE).isLessThanOrEqualToInt(INITIAL_BALANCE))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutWithConjunctionsGivenBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        preparePut(0, 0, namespace1, TABLE_1)
            .withValue(BALANCE, 999)
            .withValue(SOME_COLUMN, false));
    Scan scan =
        Scan.newBuilder(prepareScan(0, namespace1, TABLE_1))
            .where(ConditionBuilder.column(BALANCE).isLessThanInt(1000))
            .and(ConditionBuilder.column(SOME_COLUMN).isEqualToBoolean(false))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      scanWithIndex_OverlappingPutWithNonIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .textValue(SOME_COLUMN, "aaa")
            .build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, INITIAL_BALANCE);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      scanWithIndex_NonOverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 999).build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, INITIAL_BALANCE);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 999).build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, 999);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnAndConjunctionsGivenBefore_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        preparePut(0, 0, namespace1, TABLE_1)
            .withValue(BALANCE, 999)
            .withValue(SOME_COLUMN, false));
    Scan scan =
        Scan.newBuilder(prepareScanWithIndex(namespace1, TABLE_1, 999))
            .where(ConditionBuilder.column(BALANCE).isLessThanInt(1000))
            .and(ConditionBuilder.column(SOME_COLUMN).isEqualToBoolean(false))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_DeleteGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Scan scan = prepareScan(0, 0, 1, namespace1, TABLE_1);
    List<Result> results = transaction1.scan(scan);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void begin_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    String transactionId = ANY_ID_1;

    // Act Assert
    assertThatCode(
            () -> {
              DistributedTransaction transaction = manager.begin(transactionId);
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void begin_EmptyTransactionIdGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    String transactionId = "";

    // Act Assert
    assertThatThrownBy(() -> manager.begin(transactionId))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scanAll_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    List<Result> resultBefore = transaction1.scan(scanAll);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    List<Result> resultAfter = transaction1.scan(scanAll);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void scanAll_DeleteGivenBefore_ShouldScanAll() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    List<Result> results = transaction1.scan(scanAll);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void scanAll_NonOverlappingPutGivenBefore_ShouldScanAll() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));

    // Act
    ScanAll scanAll = prepareScanAll(namespace2, TABLE_2);
    Throwable thrown = catchThrowable(() -> transaction.scan(scanAll));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void scanAll_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));

    // Act
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scanAll));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scanAll_ScanAllGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException {
    // Arrange
    populateRecords(namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1).withLimit(1);

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
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    DistributedTransaction putTransaction = manager.begin();
    putTransaction.put(preparePut(0, 0, namespace1, TABLE_1));
    putTransaction.commit();

    DistributedTransaction transaction = manager.begin();
    ScanAll scanAll = prepareScanAll(namespace2, TABLE_2);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, ExecutionException, CoordinatorException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        scanAll);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scanAll);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scanAll);
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

  private void populateRecords(String namespace, String table) throws TransactionException {
    DistributedTransaction transaction = manager.begin();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        Put put =
            Put.newBuilder()
                .namespace(namespace)
                .table(table)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .intValue(BALANCE, INITIAL_BALANCE)
                .build();
        transaction.put(put);
      }
    }
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
            .withValue(BALANCE, NEW_BALANCE)
            .withValue(Attribute.toIdValue(ANY_ID_2))
            .withValue(Attribute.toStateValue(recordState))
            .withValue(Attribute.toVersionValue(2))
            .withValue(Attribute.toPreparedAtValue(preparedAt))
            .withValue(Attribute.BEFORE_PREFIX + BALANCE, INITIAL_BALANCE)
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

  private Scan prepareScanWithIndex(String namespace, String table, int balance) {
    Key indexKey = new Key(BALANCE, balance);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .indexKey(indexKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private ScanAll prepareScanAll(String namespace, String table) {
    return new ScanAll()
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
