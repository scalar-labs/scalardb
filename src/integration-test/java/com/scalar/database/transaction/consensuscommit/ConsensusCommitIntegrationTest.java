package com.scalar.database.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.DistributedTransaction;
import com.scalar.database.api.Get;
import com.scalar.database.api.Put;
import com.scalar.database.api.Result;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Selection;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.transaction.CommitException;
import com.scalar.database.exception.transaction.CoordinatorException;
import com.scalar.database.exception.transaction.CrudException;
import com.scalar.database.exception.transaction.InvalidUsageException;
import com.scalar.database.exception.transaction.UncommittedRecordException;
import com.scalar.database.exception.transaction.UnknownTransactionStatusException;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;

public class ConsensusCommitIntegrationTest {
  static final String NAMESPACE = "integration_testing";
  static final String TABLE_1 = "tx_test_table1";
  static final String TABLE_2 = "tx_test_table2";
  static final String ACCOUNT_ID = "account_id";
  static final String ACCOUNT_TYPE = "account_type";
  static final String BALANCE = "balance";
  static final int INITIAL_BALANCE = 1000;
  static final int NUM_ACCOUNTS = 4;
  static final int NUM_TYPES = 4;
  static final String ANY_ID_1 = "id1";
  static final String ANY_ID_2 = "id2";
  private final ConsensusCommitManager manager;
  // assume that storage is a spied object
  private DistributedStorage storage;
  // assume that coordinator is a spied object
  private Coordinator coordinator;
  // assume that recovery is a spied object
  private RecoveryHandler recovery;
  private ConsensusCommit transaction;

  public ConsensusCommitIntegrationTest(
      ConsensusCommitManager manager,
      DistributedStorage storage,
      Coordinator coordinator,
      RecoveryHandler recovery) {
    this.manager = manager;
    this.storage = storage;
    this.coordinator = coordinator;
    this.recovery = recovery;
  }

  public void setUp() {}

  public void get_GetGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    transaction = manager.start();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isTrue();
    Assertions.assertThat(((TransactionResult) result.get()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    transaction = manager.start();
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(((TransactionResult) results.get(0)).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime()
      throws CrudException, ExecutionException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    transaction = manager.start();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);

    // Act
    transaction.get(get);
    transaction.get(get);

    // Assert
    verify(storage).get(any(Get.class));
  }

  public void get_GetGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    transaction = manager.start();
    Get get = prepareGet(0, 4, NAMESPACE, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    transaction = manager.start();
    Scan scan = prepareScan(0, 4, 4, NAMESPACE, TABLE_1);

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
        storage, TransactionState.PREPARED, current, TransactionState.COMMITTED);
    transaction = manager.start();

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
    TransactionResult result = null;
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

  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.PREPARED, current, TransactionState.ABORTED);
    transaction = manager.start();

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
    TransactionResult result = null;
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

  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws CrudException, ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.PREPARED, prepared_at, null);
    transaction = manager.start();

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

  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.PREPARED, prepared_at, null);
    transaction = manager.start();

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
    TransactionResult result = null;
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

  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.PREPARED, current, TransactionState.COMMITTED);
    transaction = manager.start();
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
    TransactionResult result = null;
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

  public void
      get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        get);
  }

  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.PREPARED, current, TransactionState.ABORTED);
    transaction = manager.start();
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
    TransactionResult result = null;
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

  public void
      get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        get);
  }

  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, current, TransactionState.COMMITTED);
    transaction = manager.start();

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

  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(get);
  }

  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(scan);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, current, TransactionState.ABORTED);
    transaction = manager.start();

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
    TransactionResult result = null;
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

  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(get);
  }

  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, prepared_at, null);
    transaction = manager.start();

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

  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        get);
  }

  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, prepared_at, null);
    transaction = manager.start();

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
    TransactionResult result = null;
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

  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        get);
  }

  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, current, TransactionState.COMMITTED);
    transaction = manager.start();
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

  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        get);
  }

  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly(
        scan);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
          Selection s) throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, TransactionState.DELETED, current, TransactionState.ABORTED);
    transaction = manager.start();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
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
    TransactionResult result = null;
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

  public void
      get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        get);
  }

  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, CrudException {
    Scan scan = prepareScan(0, 0, 0, NAMESPACE, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly(
        scan);
  }

  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    Value expected = new IntValue(BALANCE, INITIAL_BALANCE);
    Put put = preparePut(0, 0, NAMESPACE, TABLE_1).withValue(expected);
    transaction = manager.start();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    TransactionResult result = (TransactionResult) another.get(get).get();
    assertThat(result.getValue(BALANCE).get()).isEqualTo(expected);
    Assertions.assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    int afterBalance = ((IntValue) result.get().getValue(BALANCE).get()).get() + 100;
    Value expected = new IntValue(BALANCE, afterBalance);
    Put put = preparePut(0, 0, NAMESPACE, TABLE_1).withValue(expected);
    transaction.put(put);
    transaction.commit();

    // Assert
    ConsensusCommit another = manager.start();
    TransactionResult actual = (TransactionResult) another.get(get).get();
    assertThat(actual.getValue(BALANCE).get()).isEqualTo(expected);
    Assertions.assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowCommitException()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    populateRecords();
    List<Put> puts = preparePuts(NAMESPACE, TABLE_1);
    puts.get(0).withValue(new IntValue(BALANCE, 1100));
    transaction = manager.start();

    // Act Assert
    transaction.put(puts.get(0));
    assertThatThrownBy(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);
  }

  public void
      putAndCommit_SinglePartitionMutationsGiven_ShouldAccessStorageOnceForPrepareAndCommit()
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException {
    // Arrange
    Value balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(NAMESPACE, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(1).withValue(balance);
    transaction = manager.start();

    // Act
    transaction.put(puts.get(0));
    transaction.put(puts.get(1));
    transaction.commit();

    // Assert
    // one for prepare, one for commit
    verify(storage, times(2)).mutate(any(List.class));
    verify(coordinator).putState(any(Coordinator.State.class));
  }

  public void putAndCommit_TwoPartitionsMutationsGiven_ShouldAccessStorageTwiceForPrepareAndCommit()
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException {
    // Arrange
    Value balance = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(NAMESPACE, TABLE_1);
    puts.get(0).withValue(balance);
    puts.get(NUM_TYPES).withValue(balance); // next account
    transaction = manager.start();

    // Act
    transaction.put(puts.get(0));
    transaction.put(puts.get(NUM_TYPES));
    transaction.commit();

    // Assert
    // twice for prepare, twice for commit
    verify(storage, times(4)).mutate(any(List.class));
    verify(coordinator).putState(any(Coordinator.State.class));
  }

  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords();
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    int amount = 100;
    IntValue fromBalance = new IntValue(BALANCE, INITIAL_BALANCE - amount);
    IntValue toBalance = new IntValue(BALANCE, INITIAL_BALANCE + amount);
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(from, to, amount).commit();

    // Assert
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(fromBalance));
    assertThat(another.get(gets.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(toBalance));
  }

  public void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
      throws CrudException {
    // Arrange
    Value expected = new IntValue(BALANCE, INITIAL_BALANCE);
    List<Put> puts = preparePuts(NAMESPACE, TABLE_1);
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts.get(from).withValue(expected);
    puts.get(to).withValue(expected);

    transaction = manager.start();
    transaction.setBeforeCommitHook(
        () -> {
          ConsensusCommit another = manager.start();
          puts.get(anotherTo).withValue(expected);
          assertThatCode(
                  () -> {
                    another.put(puts.get(anotherFrom));
                    another.put(puts.get(anotherTo));
                    another.commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act Assert
    transaction.put(puts.get(from));
    transaction.put(puts.get(to));
    assertThatThrownBy(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(from)).isPresent()).isFalse();
    assertThat(another.get(gets.get(to)).get().getValue(BALANCE)).isEqualTo(Optional.of(expected));
    assertThat(another.get(gets.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(expected));
  }

  public void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    int amount = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    populateRecords();
    transaction = manager.start();
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    List<Delete> deletes = prepareDeletes(NAMESPACE, TABLE_1);
    transaction.get(gets.get(from));
    transaction.delete(deletes.get(from));
    transaction.get(gets.get(to));
    transaction.delete(deletes.get(to));
    transaction.setBeforeCommitHook(
        () -> {
          assertThatCode(
                  () -> {
                    prepareTransfer(anotherFrom, anotherTo, amount).commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act
    assertThatThrownBy(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount)));
    assertThat(another.get(gets.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount)));
  }

  public void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    populateRecords();
    ConsensusCommit transaction = prepareTransfer(from, to, amount1);
    transaction.setBeforeCommitHook(
        () -> {
          assertThatCode(
                  () -> {
                    prepareTransfer(anotherFrom, anotherTo, amount2).commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act
    assertThatThrownBy(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
  }

  public void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth()
      throws CrudException, CommitException, UnknownTransactionStatusException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = NUM_TYPES * 2;
    int anotherTo = NUM_TYPES * 3;
    populateRecords();
    ConsensusCommit transaction = prepareTransfer(from, to, amount1);
    transaction.setBeforeCommitHook(
        () -> {
          assertThatCode(
                  () -> {
                    prepareTransfer(anotherFrom, anotherTo, amount2).commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act
    assertThatCode(
            () -> {
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(from)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount1)));
    assertThat(another.get(gets.get(to)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount1)));
    assertThat(another.get(gets.get(anotherFrom)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE - amount2)));
    assertThat(another.get(gets.get(anotherTo)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE + amount2)));
  }

  public void commit_DeleteGivenWithoutRead_ShouldThrowInvalidUsageException() {
    // Arrange
    Delete delete = prepareDelete(0, 0, NAMESPACE, TABLE_1);
    transaction = manager.start();

    // Act
    transaction.delete(delete);
    assertThatCode(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(InvalidUsageException.class);
  }

  public void commit_DeleteGivenForNonExisting_ShouldThrowInvalidUsageException()
      throws CrudException {
    // Arrange
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    Delete delete = prepareDelete(0, 0, NAMESPACE, TABLE_1);
    transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    assertThatCode(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(InvalidUsageException.class);
  }

  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    populateRecords();
    Get get = prepareGet(0, 0, NAMESPACE, TABLE_1);
    Delete delete = prepareDelete(0, 0, NAMESPACE, TABLE_1);
    transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    ConsensusCommit another = manager.start();
    assertThat(another.get(get).isPresent()).isFalse();
  }

  public void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    populateRecords();
    ConsensusCommit transaction = prepareDeletes(account1, account2);
    transaction.setBeforeCommitHook(
        () -> {
          assertThatCode(
                  () -> {
                    prepareDeletes(account2, account3).commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act
    assertThatThrownBy(
            () -> {
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);

    // Assert
    verify(recovery).rollback(any(Snapshot.class));
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(account1)).get().getValue(BALANCE))
        .isEqualTo(Optional.of(new IntValue(BALANCE, INITIAL_BALANCE)));
    assertThat(another.get(gets.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account3)).isPresent()).isFalse();
  }

  public void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;
    populateRecords();
    ConsensusCommit transaction = prepareDeletes(account1, account2);
    transaction.setBeforeCommitHook(
        () -> {
          assertThatCode(
                  () -> {
                    prepareDeletes(account3, account4).commit();
                  })
              .doesNotThrowAnyException();
        });

    // Act
    assertThatCode(
            () -> {
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);
    ConsensusCommit another = manager.start();
    assertThat(another.get(gets.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets.get(account4)).isPresent()).isFalse();
  }

  private ConsensusCommit prepareTransfer(int fromId, int toId, int amount) throws CrudException {
    ConsensusCommit transaction = manager.start();
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);

    Optional<Result> result1 = transaction.get(gets.get(fromId));
    Optional<Result> result2 = transaction.get(gets.get(toId));
    IntValue fromBalance =
        new IntValue(BALANCE, ((IntValue) result1.get().getValue(BALANCE).get()).get() - amount);
    IntValue toBalance =
        new IntValue(BALANCE, ((IntValue) result2.get().getValue(BALANCE).get()).get() + amount);
    List<Put> puts = preparePuts(NAMESPACE, TABLE_1);
    puts.get(fromId).withValue(fromBalance);
    puts.get(toId).withValue(toBalance);
    transaction.put(puts.get(fromId));
    transaction.put(puts.get(toId));
    return transaction;
  }

  private ConsensusCommit prepareDeletes(int one, int another) throws CrudException {
    ConsensusCommit transaction = manager.start();
    List<Get> gets = prepareGets(NAMESPACE, TABLE_1);

    transaction.get(gets.get(one));
    transaction.get(gets.get(another));
    List<Delete> deletes = prepareDeletes(NAMESPACE, TABLE_1);
    transaction.delete(deletes.get(one));
    transaction.delete(deletes.get(another));
    return transaction;
  }

  private void populateRecords() throws CommitException, UnknownTransactionStatusException {
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
                                .forTable(TABLE_1)
                                .withValue(new IntValue(BALANCE, INITIAL_BALANCE));
                        transaction.put(put);
                      });
            });
    transaction.commit();
  }

  private void populatePreparedRecordAndCoordinatorStateRecord(
      DistributedStorage storage,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, 0));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, 0));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(NAMESPACE)
            .forTable(TABLE_1)
            .withValue(new IntValue(BALANCE, INITIAL_BALANCE))
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

  private List<Delete> prepareDeletes(String namespace, String table) {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i -> {
              IntStream.range(0, NUM_TYPES)
                  .forEach(
                      j -> {
                        deletes.add(prepareDelete(i, j, namespace, table));
                      });
            });
    return deletes;
  }

  private void printAll(String namespace, String table) {
    List<Get> gets = prepareGets(namespace, table);
    ConsensusCommit transaction = manager.start();
    gets.forEach(
        g -> {
          assertThatCode(
                  () -> {
                    g.withProjection(BALANCE);
                    Optional<Result> result = transaction.get(g);
                    System.out.println(result);
                  })
              .doesNotThrowAnyException();
        });
  }
}
