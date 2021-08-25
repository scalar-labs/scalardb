package com.scalar.db.transaction.consensuscommit;

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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.CrudRuntimeException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public abstract class ConsensusCommitIntegrationTestBase {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String TABLE_1 = "tx_test_table1";
  protected static final String TABLE_2 = "tx_test_table2";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final int INITIAL_BALANCE = 1000;
  protected static final int NUM_ACCOUNTS = 4;
  protected static final int NUM_TYPES = 4;
  protected static final String ANY_ID_1 = "id1";
  protected static final String ANY_ID_2 = "id2";
  static DistributedStorageAdmin admin;
  private ConsensusCommitManager manager;
  // assume that storage is a spied object
  private DistributedStorage storage;
  // assume that coordinator is a spied object
  private Coordinator coordinator;
  // assume that recovery is a spied object
  private RecoveryHandler recovery;

  protected static void createTables() throws ExecutionException {
    TableMetadata tableMetadata =
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
            .build();
    admin.createNamespace(ConsensusCommitIntegrationTestBase.NAMESPACE);
    admin.createTable(
        ConsensusCommitIntegrationTestBase.NAMESPACE,
        ConsensusCommitIntegrationTestBase.TABLE_1,
        tableMetadata,
        new HashMap<>());
    admin.createTable(
        ConsensusCommitIntegrationTestBase.NAMESPACE,
        ConsensusCommitIntegrationTestBase.TABLE_2,
        tableMetadata,
        new HashMap<>());

    TableMetadata coordinatorTableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ID, DataType.TEXT)
            .addColumn(STATE, DataType.INT)
            .addColumn(CREATED_AT, DataType.BIGINT)
            .addPartitionKey(ID)
            .build();
    admin.createNamespace(Coordinator.NAMESPACE);
    admin.createTable(
        Coordinator.NAMESPACE, Coordinator.TABLE, coordinatorTableMetadata, new HashMap<>());
  }

  protected static void deleteTables() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE_1);
    admin.dropTable(NAMESPACE, TABLE_2);
    admin.dropTable(Coordinator.NAMESPACE, Coordinator.TABLE);
    admin.dropNamespace(NAMESPACE);
    admin.dropNamespace(Coordinator.NAMESPACE);
    admin.close();
  }

  protected static void truncateTables() throws ExecutionException {
    admin.truncateTable(NAMESPACE, TABLE_1);
    admin.truncateTable(NAMESPACE, TABLE_2);
    admin.truncateTable(Coordinator.NAMESPACE, Coordinator.TABLE);
  }

  protected void setUp(
      ConsensusCommitManager manager,
      DistributedStorage storage,
      Coordinator coordinator,
      RecoveryHandler recovery) {
    this.manager = manager;
    this.storage = storage;
    this.coordinator = coordinator;
    this.recovery = recovery;
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isTrue();
    Assertions.assertThat(((TransactionResult) result.get()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    ConsensusCommit transaction = manager.start();
    Scan scan = prepareScan(0, 0, 0, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(((TransactionResult) results.get(0)).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime()
      throws CrudException, ExecutionException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);

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
    Get get = prepareGet(0, 0, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    populateRecords(TABLE_1);
    Optional<Result> result2 = transaction.get(get);

    // Assert
    verify(storage).get(any(Get.class));
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    ConsensusCommit transaction = manager.start();
    Get get = prepareGet(0, 4, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    ConsensusCommit transaction = manager.start();
    Scan scan = prepareScan(0, 4, 4, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, current, TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollforward(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_2);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(2);
    assertThat(result.getCommittedAt()).isGreaterThan(0);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollback(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery, never()).rollback(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollback(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, current, TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();
    transaction.setBeforeRecoveryHook(
        () -> {
          ConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (s instanceof Get) {
                      another.get((Get) s);
                    } else {
                      another.scan((Scan) s);
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

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
    verify(recovery, times(2)).rollforward(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_2);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(2);
    assertThat(result.getCommittedAt()).isGreaterThan(0);
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();
    transaction.setBeforeRecoveryHook(
        () -> {
          ConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (s instanceof Get) {
                      another.get((Get) s);
                    } else {
                      another.scan((Scan) s);
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

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
    verify(recovery, times(2)).rollback(any(Selection.class), any(TransactionResult.class));
    // rollback called twice but executed once actually
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, current, TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollforward(any(Selection.class), any(TransactionResult.class));
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s).isPresent()).isFalse();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollback(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery, never()).rollback(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, prepared_at, null);
    ConsensusCommit transaction = manager.start();

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
    verify(recovery).rollback(any(Selection.class), any(TransactionResult.class));
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, current, TransactionState.COMMITTED);
    ConsensusCommit transaction = manager.start();
    transaction.setBeforeRecoveryHook(
        () -> {
          ConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (s instanceof Get) {
                      another.get((Get) s);
                    } else {
                      another.scan((Scan) s);
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

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
    verify(recovery, times(2)).rollforward(any(Selection.class), any(TransactionResult.class));
    if (s instanceof Get) {
      assertThat(transaction.get((Get) s).isPresent()).isFalse();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(0);
    }
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);
    ConsensusCommit transaction = manager.start();
    transaction.setBeforeRecoveryHook(
        () -> {
          ConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (s instanceof Get) {
                      another.get((Get) s);
                    } else {
                      another.scan((Scan) s);
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

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
    verify(recovery, times(2)).rollback(any(Selection.class), any(TransactionResult.class));
    // rollback called twice but executed once actually
    TransactionResult result;
    if (s instanceof Get) {
      result = (TransactionResult) transaction.get((Get) s).get();
    } else {
      List<Result> results = transaction.scan((Scan) s);
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) results.get(0);
    }
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        get);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        scan);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    ConsensusCommit transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, TABLE_1));

    // Assert
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    Put put = preparePut(0, 0, TABLE_1).withValue(expected);
    ConsensusCommit transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, TABLE_1);
    ConsensusCommit another = manager.start();
    TransactionResult result = (TransactionResult) another.get(get).get();
    assertThat(result.getValue(BALANCE).get()).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords(TABLE_1);
    Get get = prepareGet(0, 0, TABLE_1);
    ConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    int afterBalance = result.get().getValue(BALANCE).get().getAsInt() + 100;
    IntValue expected = new IntValue(BALANCE, afterBalance);
    Put put = preparePut(0, 0, TABLE_1).withValue(expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    ConsensusCommit another = manager.start();
    TransactionResult actual = (TransactionResult) another.get(get).get();
    assertThat(actual.getValue(BALANCE).get()).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowCommitException()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords(TABLE_1);
    List<Put> puts = preparePuts(TABLE_1);
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
    List<Put> puts = preparePuts(TABLE_1);
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
    List<Put> puts = preparePuts(TABLE_1);
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

  private void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(String fromTable, String toTable)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !fromTable.equals(toTable);

    populateRecords(fromTable);
    if (differentTables) {
      populateRecords(toTable);
    }

    List<Get> fromGets = prepareGets(fromTable);
    List<Get> toGets = differentTables ? prepareGets(toTable) : fromGets;

    int amount = 100;
    IntValue fromBalance = new IntValue(BALANCE, INITIAL_BALANCE - amount);
    IntValue toBalance = new IntValue(BALANCE, INITIAL_BALANCE + amount);
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, fromTable, to, toTable, amount).commit();

    // Assert
    ConsensusCommit another = manager.start();
    assertThat(another.get(fromGets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(fromBalance));
    assertThat(another.get(toGets.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(toBalance));
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(TABLE_1, TABLE_1);
  }

  @Test
  public void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      String table1, String table2) throws CrudException, CommitConflictException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts1 = preparePuts(table1);
    List<Put> puts2 = differentTables ? preparePuts(table2) : puts1;

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts1.get(from).withValue(expected);
    puts2.get(to).withValue(expected);

    ConsensusCommit transaction = manager.start();
    transaction.setBeforeCommitHook(
        () -> {
          ConsensusCommit another = manager.start();
          puts1.get(anotherTo).withValue(expected);
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
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;

    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(expected));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
  }

  @Test
  public void
      commit_ConflictingPutsForSameTableGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitConflictException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitConflictException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
      String table1, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    ConsensusCommit transaction = manager.start();
    List<Get> gets1 = prepareGets(table1);
    List<Delete> deletes1 = prepareDeletes(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;
    List<Delete> deletes2 = differentTables ? prepareDeletes(table2) : deletes1;

    transaction.get(gets1.get(from));
    transaction.delete(deletes1.get(from));
    transaction.get(gets2.get(to));
    transaction.delete(deletes2.get(to));
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount).commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount)));
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForSameTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutAndDeleteForDifferentTableGivenForExisting_ShouldCommitPutAndAbortDelete()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(TABLE_1, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
      String table1, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    ConsensusCommit transaction = prepareTransfer(from, table1, to, table2, amount1);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount2).commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = prepareGets(table2);

    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
  }

  @Test
  public void commit_ConflictingPutsForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
      String table1, String table2)
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = NUM_TYPES * 2;
    int anotherTo = NUM_TYPES * 3;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    ConsensusCommit transaction = prepareTransfer(from, table1, to, table2, amount1);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(
                    () -> prepareTransfer(anotherFrom, table2, anotherTo, table1, amount2).commit())
                .doesNotThrowAnyException());

    // Act
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = prepareGets(table2);

    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount1)));
    assertThat(another.get(gets2.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount1)));
    assertThat(another.get(gets2.get(anotherFrom)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets1.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
  }

  @Test
  public void commit_NonConflictingPutsForSameTableGivenForExisting_ShouldCommitBoth()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_2);
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws CrudException {
    // Arrange
    IntValue expected = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts1 = preparePuts(TABLE_1);
    List<Put> puts2 = preparePuts(TABLE_2);

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = from;
    int anotherTo = to;
    puts1.get(from).withValue(expected);
    puts1.get(to).withValue(expected);

    ConsensusCommit transaction = manager.start();
    transaction.setBeforeCommitHook(
        () -> {
          ConsensusCommit another = manager.start();
          puts2.get(from).withValue(expected);
          puts2.get(to).withValue(expected);
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
    List<Get> gets1 = prepareGets(TABLE_1);
    List<Get> gets2 = prepareGets(TABLE_2);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    assertThat(another.get(gets1.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(expected));
    assertThat(another.get(gets2.get(anotherFrom)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
    assertThat(another.get(gets2.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
  }

  @Test
  public void commit_DeleteGivenWithoutRead_ShouldThrowIllegalArgumentException() {
    // Arrange
    Delete delete = prepareDelete(0, 0, TABLE_1);
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
    Get get = prepareGet(0, 0, TABLE_1);
    Delete delete = prepareDelete(0, 0, TABLE_1);
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
    populateRecords(TABLE_1);
    Get get = prepareGet(0, 0, TABLE_1);
    Delete delete = prepareDelete(0, 0, TABLE_1);
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
      String table1, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    ConsensusCommit transaction = prepareDeletes(account1, table1, account2, table2);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(() -> prepareDeletes(account2, table2, account3, table1).commit())
                .doesNotThrowAnyException());

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;

    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(account1)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account3)).isPresent()).isFalse();
  }

  @Test
  public void
      commit_ConflictingDeletesForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_ConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(TABLE_1, TABLE_2);
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      String table1, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    boolean differentTables = !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;

    populateRecords(table1);
    if (differentTables) {
      populateRecords(table2);
    }

    ConsensusCommit transaction = prepareDeletes(account1, table1, account2, table2);
    transaction.setBeforeCommitHook(
        () ->
            assertThatCode(() -> prepareDeletes(account3, table2, account4, table1).commit())
                .doesNotThrowAnyException());

    // Act
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(table1);
    List<Get> gets2 = differentTables ? prepareGets(table2) : gets1;
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets1.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account4)).isPresent()).isFalse();
  }

  @Test
  public void commit_NonConflictingDeletesForSameTableGivenForExisting_ShouldCommitBoth()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_1);
  }

  @Test
  public void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(TABLE_1, TABLE_2);
  }

  private void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
      String table1, String table2)
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, table1).withValue(BALANCE, 1),
            preparePut(0, 1, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction = manager.start();
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    ConsensusCommit transaction2 = manager.start();
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    int current1 = result1.get().getValue(BALANCE).get().getAsInt();
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    int current2 = result2.get().getValue(BALANCE).get().getAsInt();
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    transaction2.commit();

    // Assert
    transaction = manager.start();
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    // the results can not be produced by executing the transactions serially
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSnapshot_ShouldProduceNonSerializableResult()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSnapshot_ShouldProduceNonSerializableResult()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2)
          throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, table1).withValue(BALANCE, 1),
            preparePut(0, 1, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    int current1 = result1.get().getValue(BALANCE).get().getAsInt();
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    int current2 = result2.get().getValue(BALANCE).get().getAsInt();
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2)
          throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, table1).withValue(BALANCE, 1),
            preparePut(0, 1, table2).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    transaction1.get(get1_2);
    int current1 = result1.get().getValue(BALANCE).get().getAsInt();
    Get get2_1 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, table2);
    transaction2.get(get2_2);
    int current2 = result2.get().getValue(BALANCE).get().getAsInt();
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    transaction1.commit();
    Throwable thrown = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2) throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
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
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
          String table1, String table2) throws CrudException, CoordinatorException {
    // Arrange
    Coordinator.State state = new State(ANY_ID_1, TransactionState.ABORTED);
    coordinator.putState(state);

    // Act
    ConsensusCommit transaction1 =
        manager.start(ANY_ID_1, Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get1_1 = prepareGet(0, 1, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, table1);
    Optional<Result> result2 = transaction1.get(get1_2);
    int current1 = 0;
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
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
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTableWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws CrudException, CoordinatorException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly(
        TABLE_1, TABLE_2);
  }

  private void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
          String table1, String table2) throws CrudException {
    // Arrange
    // no records

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
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
    Put put1 = preparePut(0, 0, table1).withValue(BALANCE, current1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, table2).withValue(BALANCE, current2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    result1 = transaction.get(get1_1);
    result2 = transaction.get(get2_1);
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInSameTableWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_1);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsInDifferentTablesWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws CrudException {
    commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException(
        TABLE_1, TABLE_2);
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
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, TABLE_1));
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
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(result2.isPresent()).isFalse();
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitConflictException()
          throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    List<Put> puts =
        Arrays.asList(
            preparePut(0, 0, TABLE_1).withValue(BALANCE, 1),
            preparePut(0, 1, TABLE_1).withValue(BALANCE, 1));
    ConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.put(puts);
    transaction.commit();

    // Act
    ConsensusCommit transaction1 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    ConsensusCommit transaction2 =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    int count2 = results2.size();
    Put put1 = preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1);
    transaction1.put(put1);
    Put put2 = preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1);
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    transaction = manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    Optional<Result> result2 = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result1.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 3));
    assertThat(result2.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 1));
    assertThat(thrown1).doesNotThrowAnyException();
    assertThat(thrown2)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(CommitConflictException.class);
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));
    int balance1 = 0;
    if (result1.isPresent()) {
      balance1 = result1.get().getValue(BALANCE).get().getAsInt();
    }
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance1 + 1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    ConsensusCommit transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = result3.get().getValue(BALANCE).get().getAsInt();
    }
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result.get().getValue(BALANCE).get().getAsInt()).isEqualTo(1);
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    ConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    ConsensusCommit transaction3 = manager.start();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = result3.get().getValue(BALANCE).get().getAsInt();
    }
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown)
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result.get().getValue(BALANCE).get().getAsInt()).isEqualTo(1);
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
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
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    List<Result> resultBefore = transaction1.scan(scan);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
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
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    ConsensusCommit transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldPut()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.commit();

    // Act
    ConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    ConsensusCommit transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isTrue();
    assertThat(resultAfter.get().getValue(BALANCE).get()).isEqualTo(new IntValue(BALANCE, 2));
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowCrudRuntimeException() {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 0, 0, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.abort();

    // Assert
    assertThat(thrown).isInstanceOf(CrudRuntimeException.class);
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    ConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act
    Scan scan = prepareScan(0, 1, 1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
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

  private ConsensusCommit prepareTransfer(
      int fromId, String fromTable, int toId, String toTable, int amount) throws CrudException {
    boolean differentTables = !toTable.equals(fromTable);

    ConsensusCommit transaction = manager.start();

    List<Get> fromGets = prepareGets(fromTable);
    List<Get> toGets = differentTables ? prepareGets(toTable) : fromGets;
    Optional<Result> fromResult = transaction.get(fromGets.get(fromId));
    Optional<Result> toResult = transaction.get(toGets.get(toId));

    IntValue fromBalance =
        new IntValue(BALANCE, fromResult.get().getValue(BALANCE).get().getAsInt() - amount);
    IntValue toBalance =
        new IntValue(BALANCE, toResult.get().getValue(BALANCE).get().getAsInt() + amount);

    List<Put> fromPuts = preparePuts(fromTable);
    List<Put> toPuts = differentTables ? preparePuts(toTable) : fromPuts;
    fromPuts.get(fromId).withValue(fromBalance);
    toPuts.get(toId).withValue(toBalance);
    transaction.put(fromPuts.get(fromId));
    transaction.put(toPuts.get(toId));

    return transaction;
  }

  private ConsensusCommit prepareDeletes(int one, String table, int another, String anotherTable)
      throws CrudException {
    boolean differentTables = !table.equals(anotherTable);

    ConsensusCommit transaction = manager.start();

    List<Get> gets = prepareGets(table);
    List<Get> anotherGets = differentTables ? prepareGets(anotherTable) : gets;
    transaction.get(gets.get(one));
    transaction.get(anotherGets.get(another));

    List<Delete> deletes = prepareDeletes(table);
    List<Delete> anotherDeletes = differentTables ? prepareDeletes(anotherTable) : deletes;
    transaction.delete(deletes.get(one));
    transaction.delete(anotherDeletes.get(another));

    return transaction;
  }

  private void populateRecords(String table)
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
                                  .forNamespace(NAMESPACE)
                                  .forTable(table)
                                  .withValue(BALANCE, INITIAL_BALANCE);
                          transaction.put(put);
                        }));
    transaction.commit();
  }

  private void populatePreparedRecordAndCoordinatorStateRecord(
      DistributedStorage storage,
      String table,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = new Key(ACCOUNT_ID, 0);
    Key clusteringKey = new Key(ACCOUNT_TYPE, 0);
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(NAMESPACE)
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

  private Get prepareGet(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Get> prepareGets(String table) {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> IntStream.range(0, NUM_TYPES).forEach(j -> gets.add(prepareGet(i, j, table))));
    return gets;
  }

  private Scan prepareScan(int id, int fromType, int toType, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  private Put preparePut(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Put> preparePuts(String table) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> IntStream.range(0, NUM_TYPES).forEach(j -> puts.add(preparePut(i, j, table))));
    return puts;
  }

  private Delete prepareDelete(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private List<Delete> prepareDeletes(String table) {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> deletes.add(prepareDelete(i, j, table))));
    return deletes;
  }
}
