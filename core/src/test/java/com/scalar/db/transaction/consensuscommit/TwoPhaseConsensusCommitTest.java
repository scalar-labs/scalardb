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
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TwoPhaseConsensusCommitTest {

  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TX_ID = "any_id";

  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private RecoveryHandler recovery;
  @Mock private ConsensusCommitMutationOperationChecker mutationOperationChecker;
  @InjectMocks private TwoPhaseConsensusCommit transaction;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_NAMESPACE).forTable(ANY_TABLE_NAME);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException {
    // Arrange
    Get get = prepareGet();
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
    Get get = prepareGet();
    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    when(crud.get(get)).thenThrow(toThrow);
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(get, result);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
    Scan scan = prepareScan();
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
  public void scan_ScanForUncommittedRecordGiven_ShouldRecoverRecord() throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    when(crud.scan(scan)).thenThrow(toThrow);
    when(toThrow.getSelection()).thenReturn(scan);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(scan, result);
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.put(put);

    // Assert
    verify(crud).put(put);
    verify(mutationOperationChecker).check(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put);
    verify(mutationOperationChecker, times(2)).check(put);
  }

  @Test
  public void put_PutGivenAndUncommittedRecordExceptionThrown_ShouldRecoverRecord()
      throws CrudException {
    // Arrange
    Put put = preparePut();
    Get get = prepareGet();

    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    doThrow(toThrow).when(crud).put(put);
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.delete(delete);

    // Assert
    verify(crud).delete(delete);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete);
    verify(mutationOperationChecker, times(2)).check(delete);
  }

  @Test
  public void delete_DeleteGivenAndUncommittedRecordExceptionThrown_ShouldRecoverRecord()
      throws CrudException {
    // Arrange
    Delete delete = prepareDelete();
    Get get = prepareGet();

    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    doThrow(toThrow).when(crud).delete(delete);
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(delete))
        .isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldCallCrudHandlerPutAndDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Put put = preparePut();
    Delete delete = prepareDelete();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.mutate(Arrays.asList(put, delete));

    // Assert
    verify(crud).put(put);
    verify(crud).delete(delete);
    verify(mutationOperationChecker).check(put);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void prepare_ProcessedCrudGiven_ShouldPrepareWithSnapshot()
      throws PreparationException, CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.prepare();

    // Assert
    verify(crud).readIfImplicitPreReadEnabled();
    verify(commit).prepare(snapshot);
  }

  @Test
  public void
      prepare_ProcessedCrudGiven_CrudConflictExceptionThrownWhileImplicitPreRead_ShouldThrowPreparationConflictException()
          throws CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CrudConflictException.class).when(crud).readIfImplicitPreReadEnabled();

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationConflictException.class);
  }

  @Test
  public void
      prepare_ProcessedCrudGiven_UncommittedRecordExceptionThrownWhileImplicitPreRead_ShouldPerformLazyRecoveryAndThrowPreparationConflictException()
          throws CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);

    Get get = mock(Get.class);
    TransactionResult result = mock(TransactionResult.class);

    UncommittedRecordException uncommittedRecordException = mock(UncommittedRecordException.class);
    when(uncommittedRecordException.getSelection()).thenReturn(get);
    when(uncommittedRecordException.getResults()).thenReturn(Collections.singletonList(result));

    doThrow(uncommittedRecordException).when(crud).readIfImplicitPreReadEnabled();

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationConflictException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void
      prepare_ProcessedCrudGiven_CrudExceptionThrownWhileImplicitPreRead_ShouldThrowPreparationException()
          throws CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CrudException.class).when(crud).readIfImplicitPreReadEnabled();

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
  }

  @Test
  public void validate_ProcessedCrudGiven_ShouldPerformValidationWithSnapshot()
      throws ValidationException, PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.validate();

    // Assert
    verify(commit).validate(snapshot);
  }

  @Test
  public void commit_ShouldCommitStateAndRecords()
      throws CommitException, UnknownTransactionStatusException, PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(snapshot);
    verify(commit).commitRecords(snapshot);
  }

  @Test
  public void commit_ExtraReadUsedAndValidatedState_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, PreparationException,
          ValidationException {
    // Arrange
    transaction.prepare();
    transaction.validate();
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);
    when(snapshot.isValidationRequired()).thenReturn(true);

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(snapshot);
    verify(commit).commitRecords(snapshot);
  }

  @Test
  public void commit_ExtraReadUsedAndPreparedState_ShouldThrowIllegalStateException()
      throws PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn(ANY_TX_ID);
    when(snapshot.isValidationRequired()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void rollback_ShouldAbortStateAndRollbackRecords()
      throws RollbackException, UnknownTransactionStatusException, PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    transaction.rollback();

    // Assert
    verify(commit).abortState(snapshot.getId());
    verify(commit).rollbackRecords(snapshot);
  }

  @Test
  public void rollback_CalledAfterPrepareFails_ShouldAbortStateAndRollbackRecords()
      throws PreparationException, UnknownTransactionStatusException, RollbackException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(PreparationException.class).when(commit).prepare(snapshot);

    // Act
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    verify(commit).abortState(snapshot.getId());
    verify(commit).rollbackRecords(snapshot);
  }

  @Test
  public void rollback_CalledAfterCommitFails_ShouldNeverAbortStateAndRollbackRecords()
      throws CommitException, UnknownTransactionStatusException, RollbackException,
          PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CommitConflictException.class).when(commit).commitState(snapshot);

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    verify(commit, never()).abortState(snapshot.getId());
    verify(commit, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      rollback_UnknownTransactionStatusExceptionThrownByAbortState_ShouldThrowRollbackException()
          throws UnknownTransactionStatusException, PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(commit.abortState(snapshot.getId())).thenThrow(UnknownTransactionStatusException.class);

    // Act Assert
    assertThatThrownBy(transaction::rollback).isInstanceOf(RollbackException.class);

    verify(commit, never()).rollbackRecords(snapshot);
  }

  @Test
  public void rollback_CommittedStateReturnedByAbortState_ShouldThrowRollbackException()
      throws UnknownTransactionStatusException, PreparationException {
    // Arrange
    transaction.prepare();
    when(crud.getSnapshot()).thenReturn(snapshot);
    when(commit.abortState(snapshot.getId())).thenReturn(TransactionState.COMMITTED);

    // Act Assert
    assertThatThrownBy(transaction::rollback).isInstanceOf(RollbackException.class);

    verify(commit, never()).rollbackRecords(snapshot);
  }
}
