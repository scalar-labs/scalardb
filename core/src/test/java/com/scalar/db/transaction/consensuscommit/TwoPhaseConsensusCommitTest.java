package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommit.Status;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TwoPhaseConsensusCommitTest {

  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_TX_ID = "any_id";

  @Mock private Get get;
  @Mock private Scan scan;
  @Mock private Put put;
  @Mock private Delete delete;
  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private RecoveryHandler recovery;
  @Mock private TwoPhaseConsensusCommitManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(get.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(get.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(scan.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(scan.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(put.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(delete.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
  }

  @Test
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    TransactionResult result = mock(TransactionResult.class);
    when(crud.get(get)).thenReturn(Optional.of(result));
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    Optional<Result> actual = transaction.get(get);

    // Assert
    assertThat(actual).isPresent();
    verify(recovery, never()).recover(get, result);
    verify(crud).get(get);
  }

  @Test
  public void get_GetForUncommittedRecordGiven_ShouldRecoverRecord() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    when(crud.get(get)).thenThrow(toThrow);
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(get, result);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    TransactionResult result = mock(TransactionResult.class);
    List<Result> results = Collections.singletonList(result);
    when(crud.scan(scan)).thenReturn(results);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    List<Result> actual = transaction.scan(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    verify(crud).scan(scan);
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.put(put);

    // Assert
    verify(crud).put(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.delete(delete);

    // Assert
    verify(crud).delete(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldCallCrudHandlerPutAndDelete() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.mutate(Arrays.asList(put, delete));

    // Assert
    verify(crud).put(put);
    verify(crud).delete(delete);
  }

  @Test
  public void prepare_ProcessedCrudGiven_ShouldPrepareWithSnapshot()
      throws PreparationException, CommitException, UnknownTransactionStatusException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.prepare();

    // Assert
    verify(commit).prepare(snapshot, false);
  }

  @Test
  public void validate_ProcessedCrudGiven_ShouldPerformPreCommitValidationWithSnapshot()
      throws ValidationException, CommitException, UnknownTransactionStatusException {
    // Arrange
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, false, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.validate();

    // Assert
    verify(commit).preCommitValidation(snapshot, false);
  }

  @Test
  public void commit_CalledWithCoordinator_ShouldCommitStateAndRecords()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean isCoordinator = true;
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(snapshot);
    verify(commit).commitRecords(snapshot);
  }

  @Test
  public void commit_CalledWithParticipant_ShouldCommitRecordsOnly()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean isCoordinator = false; // means it's a participant process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);

    // Act
    transaction.commit();

    // Assert
    verify(commit, never()).commitState(snapshot);
    verify(commit).commitRecords(snapshot);
    verify(manager).removeTransaction(ANY_TX_ID);
  }

  @Test
  public void commit_ExtraReadUsedAndValidatedState_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    boolean isCoordinator = true; // means it's a coordinator process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.VALIDATED;
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);
    when(snapshot.isPreCommitValidationRequired()).thenReturn(true);

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(snapshot);
    verify(commit).commitRecords(snapshot);
  }

  @Test
  public void commit_ExtraReadUsedAndPreparedState_ShouldThrowIllegalStateException() {
    // Arrange
    boolean isCoordinator = true; // means it's a coordinator process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);
    when(snapshot.isPreCommitValidationRequired()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void rollback_CalledWithCoordinator_ShouldAbortStateAndRollbackRecords()
      throws RollbackException, UnknownTransactionStatusException {
    // Arrange
    boolean isCoordinator = true; // means it's a coordinator process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.rollback();

    // Assert
    verify(commit).abort(snapshot.getId());
    verify(commit).rollbackRecords(snapshot);
  }

  @Test
  public void rollback_CalledWithCoordinatorAfterPrepareFails_ShouldAbortStateAndRollbackRecords()
      throws CommitException, UnknownTransactionStatusException, RollbackException {
    // Arrange
    boolean isCoordinator = true; // means it's a coordinator process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CommitException.class).when(commit).prepare(snapshot, false);

    // Act
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    verify(commit).abort(snapshot.getId());
    verify(commit).rollbackRecords(snapshot);
  }

  @Test
  public void
      rollback_CalledWithCoordinatorAfterCommitFails_ShouldNeverAbortStateAndRollbackRecords()
          throws CommitException, UnknownTransactionStatusException, RollbackException {
    // Arrange
    boolean isCoordinator = true; // means it's a coordinator process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CommitException.class).when(commit).commitState(snapshot);

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    verify(commit, never()).abort(snapshot.getId());
    verify(commit, never()).rollbackRecords(snapshot);
  }

  @Test
  public void rollback_CalledWithParticipant_ShouldRollbackRecordsOnly() throws RollbackException {
    // Arrange
    boolean isCoordinator = false; // means it's a participant process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    transaction.status = Status.PREPARED;
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);

    // Act
    transaction.rollback();

    // Assert
    verify(commit).rollbackRecords(snapshot);
    verify(manager).removeTransaction(ANY_TX_ID);
  }

  @Test
  public void rollback_CalledWithParticipantAfterPrepareFails_ShouldRollbackRecords()
      throws CommitException, UnknownTransactionStatusException, RollbackException {
    // Arrange
    boolean isCoordinator = false; // means it's a participant process
    TwoPhaseConsensusCommit transaction =
        new TwoPhaseConsensusCommit(crud, commit, recovery, isCoordinator, manager);
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);
    doThrow(CommitException.class).when(commit).prepare(snapshot, false);

    // Act
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    verify(commit).rollbackRecords(snapshot);
    verify(manager).removeTransaction(ANY_TX_ID);
  }
}
