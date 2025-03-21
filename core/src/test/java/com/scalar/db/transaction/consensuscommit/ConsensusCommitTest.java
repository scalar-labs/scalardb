package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
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
  private static final String ANY_TEXT_4 = "text4";

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

    // Arrange
    when(crud.areAllScannersClosed()).thenReturn(true);
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

    // Act
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

    verify(recovery).recover(get, result);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = mock(TransactionResult.class);
    List<Result> results = Collections.singletonList(result);
    when(crud.scan(scan)).thenReturn(results);

    // Act
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
  public void getScannerAndScannerOne_ShouldCallCrudHandlerGetScannerAndScannerOne()
      throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    Result result = mock(Result.class);
    when(scanner.one()).thenReturn(Optional.of(result));
    when(crud.getScanner(scan)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    Optional<Result> actualResult = actualScanner.one();

    // Assert
    assertThat(actualResult).hasValue(result);
    verify(crud).getScanner(scan);
    verify(scanner).one();
  }

  @Test
  public void
      getScannerAndScannerOne_UncommittedRecordExceptionThrownByScannerOne_ShouldRecoverRecord()
          throws CrudException {
    // Arrange
    Scan scan = prepareScan();

    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    TransactionResult result = mock(TransactionResult.class);
    when(toThrow.getSelection()).thenReturn(scan);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(scanner.one()).thenThrow(toThrow);
    when(crud.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    assertThatThrownBy(actualScanner::one).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(scan, result);
  }

  @Test
  public void getScannerAndScannerAll_ShouldCallCrudHandlerGetScannerAndScannerAll()
      throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    when(scanner.all()).thenReturn(Arrays.asList(result1, result2));
    when(crud.getScanner(scan)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    List<Result> actualResults = actualScanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2);
    verify(crud).getScanner(scan);
    verify(scanner).all();
  }

  @Test
  public void
      getScannerAndScannerAll_UncommittedRecordExceptionThrownByScannerAll_ShouldRecoverRecord()
          throws CrudException {
    // Arrange
    Scan scan = prepareScan();

    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    TransactionResult result = mock(TransactionResult.class);
    when(toThrow.getSelection()).thenReturn(scan);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(scanner.all()).thenThrow(toThrow);
    when(crud.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    assertThatThrownBy(actualScanner::all).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(scan, result);
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put);

    // Act
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

    // Act
    consensus.put(Arrays.asList(put, put));

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
    assertThatThrownBy(() -> consensus.put(put)).isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete);

    // Act
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

    // Act
    consensus.delete(Arrays.asList(delete, delete));

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
    assertThatThrownBy(() -> consensus.delete(delete))
        .isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void insert_InsertGiven_ShouldCallCrudHandlerPut()
      throws CrudException, ExecutionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    // Act
    consensus.insert(insert);

    // Assert
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableInsertMode()
            .build();
    verify(crud).put(expectedPut);
    verify(mutationOperationChecker).check(expectedPut);
  }

  @Test
  public void upsert_UpsertGiven_ShouldCallCrudHandlerPut()
      throws CrudException, ExecutionException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    // Act
    consensus.upsert(upsert);

    // Assert
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableImplicitPreRead()
            .build();
    verify(crud).put(expectedPut);
    verify(mutationOperationChecker).check(expectedPut);
  }

  @Test
  public void upsert_UpsertForUncommittedRecordGiven_ShouldRecoverRecord() throws CrudException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableImplicitPreRead()
            .build();
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();

    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    doThrow(toThrow).when(crud).put(put);
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> consensus.upsert(upsert))
        .isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
  }

  @Test
  public void update_UpdateWithoutConditionGiven_ShouldCallCrudHandlerPut()
      throws CrudException, ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    // Act
    consensus.update(update);

    // Assert
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();
    verify(crud).put(expectedPut);
    verify(mutationOperationChecker).check(expectedPut);
  }

  @Test
  public void update_UpdateWithConditionGiven_ShouldCallCrudHandlerPut()
      throws CrudException, ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();

    // Act
    consensus.update(update);

    // Assert
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .enableImplicitPreRead()
            .build();
    verify(crud).put(expectedPut);
    verify(mutationOperationChecker).check(expectedPut);
  }

  @Test
  public void
      update_UpdateWithoutConditionGivenAndUnsatisfiedConditionExceptionThrownByCrudHandler_ShouldDoNothing()
          throws CrudException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn("id");

    doThrow(UnsatisfiedConditionException.class).when(crud).put(put);

    // Act Assert
    assertThatCode(() -> consensus.update(update)).doesNotThrowAnyException();
  }

  @Test
  public void
      update_UpdateWithUpdateIfConditionGivenAndUnsatisfiedConditionExceptionThrownByCrudHandler_ShouldThrowUnsatisfiedConditionException()
          throws CrudException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .enableImplicitPreRead()
            .build();

    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn("id");

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIf");
    doThrow(unsatisfiedConditionException).when(crud).put(put);

    // Act Assert
    assertThatThrownBy(() -> consensus.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("UpdateIf")
        .hasMessageNotContaining("PutIf");
  }

  @Test
  public void
      update_UpdateWithUpdateIfExistsConditionGivenAndUnsatisfiedConditionExceptionThrownByCrudHandler_ShouldThrowUnsatisfiedConditionException()
          throws CrudException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.updateIfExists())
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    when(crud.getSnapshot()).thenReturn(snapshot);
    when(snapshot.getId()).thenReturn("id");

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIfExists");
    doThrow(unsatisfiedConditionException).when(crud).put(put);

    // Act Assert
    assertThatThrownBy(() -> consensus.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("UpdateIfExists")
        .hasMessageNotContaining("PutIfExists");
  }

  @Test
  public void update_UpdateForUncommittedRecordGiven_ShouldRecoverRecord() throws CrudException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();

    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    doThrow(toThrow).when(crud).put(put);
    when(toThrow.getSelection()).thenReturn(get);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> consensus.update(update))
        .isInstanceOf(UncommittedRecordException.class);

    verify(recovery).recover(get, result);
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
  public void commit_ScannerNotClosed_ShouldThrowIllegalStateException() {
    // Arrange
    when(crud.areAllScannersClosed()).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void rollback_ShouldDoNothing() throws CrudException, UnknownTransactionStatusException {
    // Arrange

    // Act
    consensus.rollback();

    // Assert
    verify(crud).closeScanners();
    verify(commit, never()).rollbackRecords(any(Snapshot.class));
    verify(commit, never()).abortState(anyString());
  }

  @Test
  public void rollback_WithGroupCommitter_ShouldRemoveTxFromGroupCommitter()
      throws CrudException, UnknownTransactionStatusException {
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
    verify(crud).closeScanners();
    verify(groupCommitter).remove(txId);
    verify(commit, never()).rollbackRecords(any(Snapshot.class));
    verify(commit, never()).abortState(anyString());
  }
}
