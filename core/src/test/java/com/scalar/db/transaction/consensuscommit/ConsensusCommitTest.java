package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
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
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
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

public class ConsensusCommitTest {
  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";

  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private RecoveryHandler recovery;

  @SuppressWarnings("unused")
  @Mock
  private ConsensusCommitManager manager;

  @Mock private ConsensusCommitMutationOperationChecker mutationOperationChecker;

  @InjectMocks private ConsensusCommit consensus;

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

    // Act Assert
    Optional<Result> actual = consensus.get(get);

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
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> consensus.get(get)).isInstanceOf(UncommittedRecordException.class);

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

    // Act Assert
    List<Result> actual = consensus.scan(scan);

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
    assertThatThrownBy(() -> consensus.scan(scan)).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(scan, result);
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(put);

    // Assert
    verify(crud).put(put);
    verify(mutationOperationChecker).check(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put);
    verify(mutationOperationChecker, times(2)).check(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(delete);

    // Assert
    verify(crud).delete(delete);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete);
    verify(mutationOperationChecker, times(2)).check(delete);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldCallCrudHandlerPutAndDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Put put = preparePut();
    Delete delete = prepareDelete();
    doNothing().when(crud).put(put);
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.mutate(Arrays.asList(put, delete));

    // Assert
    verify(crud).put(put);
    verify(crud).delete(delete);
    verify(mutationOperationChecker).check(put);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void commit_ProcessedCrudGiven_ShouldCommitWithSnapshot()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    doNothing().when(commit).commit(any(Snapshot.class));
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act
    consensus.commit();

    // Assert
    verify(crud).readIfImplicitPreReadEnabled();
    verify(commit).commit(snapshot);
  }

  @Test
  public void
      commit_ProcessedCrudGiven_CrudConflictExceptionThrownWhileImplicitPreRead_ShouldThrowCommitConflictException()
          throws CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CrudConflictException.class).when(crud).readIfImplicitPreReadEnabled();

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_ProcessedCrudGiven_UncommittedRecordExceptionThrownWhileImplicitPreRead_ShouldPerformLazyRecoveryAndThrowCommitConflictException()
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
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitConflictException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void
      commit_ProcessedCrudGiven_CrudExceptionThrownWhileImplicitPreRead_ShouldThrowCommitException()
          throws CrudException {
    // Arrange
    when(crud.getSnapshot()).thenReturn(snapshot);
    doThrow(CrudException.class).when(crud).readIfImplicitPreReadEnabled();

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitException.class);
  }

  @Test
  public void rollback_WithoutGroupCommitter_ShouldDoNothing()
      throws UnknownTransactionStatusException {
    // Arrange

    // Act
    consensus.rollback();

    // Assert
    verify(commit, never()).rollbackRecords(any(Snapshot.class));
    verify(commit, never()).abortState(anyString());
  }

  @Test
  public void rollback_WithGroupCommitter_ShouldRemoveTxFromGroupCommitter()
      throws UnknownTransactionStatusException {
    // Arrange
    String txId = "tx-id";
    Snapshot snapshot = mock(Snapshot.class);
    doReturn(txId).when(snapshot).getId();
    doReturn(snapshot).when(crud).getSnapshot();
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    ConsensusCommit consensusWithGroupCommit =
        new ConsensusCommit(crud, commit, recovery, mutationOperationChecker, groupCommitter);

    // Act
    consensusWithGroupCommit.rollback();

    // Assert
    verify(groupCommitter).remove(txId);
    verify(commit, never()).rollbackRecords(any(Snapshot.class));
    verify(commit, never()).abortState(anyString());
  }
}
