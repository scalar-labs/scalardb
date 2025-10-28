package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TX_ID = "any_id";

  private TransactionContext context;
  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private ConsensusCommitMutationOperationChecker mutationOperationChecker;

  private TwoPhaseConsensusCommit transaction;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    context = spy(new TransactionContext(ANY_TX_ID, snapshot, Isolation.SNAPSHOT, false, false));
    transaction = new TwoPhaseConsensusCommit(context, crud, commit, mutationOperationChecker);
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Scan prepareScan() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .textValue(ANY_NAME_3, ANY_TEXT_3)
        .build();
  }

  private Delete prepareDelete() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  @Test
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = mock(TransactionResult.class);
    when(crud.get(get, context)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = transaction.get(get);

    // Assert
    assertThat(actual).isPresent();
    verify(crud).get(get, context);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = mock(TransactionResult.class);
    List<Result> results = Collections.singletonList(result);
    when(crud.scan(scan, context)).thenReturn(results);

    // Act
    List<Result> actual = transaction.scan(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    verify(crud).scan(scan, context);
  }

  @Test
  public void getScannerAndScannerOne_ShouldCallCrudHandlerGetScannerAndScannerOne()
      throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    Result result = mock(Result.class);
    when(scanner.one()).thenReturn(Optional.of(result));
    when(crud.getScanner(scan, context)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = transaction.getScanner(scan);
    Optional<Result> actualResult = actualScanner.one();

    // Assert
    assertThat(actualResult).hasValue(result);
    verify(crud).getScanner(scan, context);
    verify(scanner).one();
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
    when(crud.getScanner(scan, context)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = transaction.getScanner(scan);
    List<Result> actualResults = actualScanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2);
    verify(crud).getScanner(scan, context);
    verify(scanner).all();
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();

    // Act
    transaction.put(put);

    // Assert
    verify(crud).put(put, context);
    verify(mutationOperationChecker).check(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();

    // Act
    transaction.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put, context);
    verify(mutationOperationChecker, times(2)).check(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();

    // Act
    transaction.delete(delete);

    // Assert
    verify(crud).delete(delete, context);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();

    // Act
    transaction.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete, context);
    verify(mutationOperationChecker, times(2)).check(delete);
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
    transaction.insert(insert);

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
    verify(crud).put(expectedPut, context);
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
    transaction.upsert(upsert);

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
    verify(crud).put(expectedPut, context);
    verify(mutationOperationChecker).check(expectedPut);
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
    transaction.update(update);

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
    verify(crud).put(expectedPut, context);
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
    transaction.update(update);

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
    verify(crud).put(expectedPut, context);
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

    doThrow(UnsatisfiedConditionException.class).when(crud).put(put, context);

    // Act Assert
    assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();
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

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIf");
    doThrow(unsatisfiedConditionException).when(crud).put(put, context);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
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

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIfExists");
    doThrow(unsatisfiedConditionException).when(crud).put(put, context);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("UpdateIfExists")
        .hasMessageNotContaining("PutIfExists");
  }

  @Test
  public void mutate_MutationsGiven_ShouldCallCrudHandlerPutAndDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    // Act
    transaction.mutate(Arrays.asList(put, insert, upsert, update, delete));

    // Assert
    Put expectedPutFromInsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableInsertMode()
            .build();
    Put expectedPutFromUpsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableImplicitPreRead()
            .build();
    Put expectedPutFromUpdate =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    verify(crud).put(put, context);
    verify(crud).put(expectedPutFromInsert, context);
    verify(crud).put(expectedPutFromUpsert, context);
    verify(crud).put(expectedPutFromUpdate, context);
    verify(crud).delete(delete, context);
    verify(mutationOperationChecker).check(put);
    verify(mutationOperationChecker).check(expectedPutFromInsert);
    verify(mutationOperationChecker).check(expectedPutFromUpsert);
    verify(mutationOperationChecker).check(expectedPutFromUpdate);
    verify(mutationOperationChecker).check(delete);
  }

  @Test
  public void mutate_EmptyMutationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transaction.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void batch_OperationsGiven_ShouldCallCrudHandlerProperly()
      throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Scan scan = prepareScan();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    TransactionResult result1 = mock(TransactionResult.class);
    TransactionResult result2 = mock(TransactionResult.class);
    TransactionResult result3 = mock(TransactionResult.class);

    when(crud.get(get, context)).thenReturn(Optional.of(result1));
    when(crud.scan(scan, context)).thenReturn(Arrays.asList(result2, result3));

    // Act
    List<CrudOperable.BatchResult> results =
        transaction.batch(Arrays.asList(get, scan, put, insert, upsert, update, delete));

    // Assert
    Put expectedPutFromInsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableInsertMode()
            .build();
    Put expectedPutFromUpsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableImplicitPreRead()
            .build();
    Put expectedPutFromUpdate =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .enableImplicitPreRead()
            .build();

    verify(crud).get(get, context);
    verify(crud).scan(scan, context);
    verify(crud).put(put, context);
    verify(crud).put(expectedPutFromInsert, context);
    verify(crud).put(expectedPutFromUpsert, context);
    verify(crud).put(expectedPutFromUpdate, context);
    verify(crud).delete(delete, context);
    verify(mutationOperationChecker).check(put);
    verify(mutationOperationChecker).check(expectedPutFromInsert);
    verify(mutationOperationChecker).check(expectedPutFromUpsert);
    verify(mutationOperationChecker).check(expectedPutFromUpdate);
    verify(mutationOperationChecker).check(delete);
    assertThat(results).hasSize(7);
    assertThat(results.get(0).getType()).isEqualTo(CrudOperable.BatchResult.Type.GET);
    assertThat(results.get(0).getGetResult()).hasValue(result1);
    assertThat(results.get(1).getType()).isEqualTo(CrudOperable.BatchResult.Type.SCAN);
    assertThat(results.get(1).getScanResult()).containsExactly(result2, result3);
    assertThat(results.get(2).getType()).isEqualTo(CrudOperable.BatchResult.Type.PUT);
    assertThat(results.get(3).getType()).isEqualTo(CrudOperable.BatchResult.Type.INSERT);
    assertThat(results.get(4).getType()).isEqualTo(CrudOperable.BatchResult.Type.UPSERT);
    assertThat(results.get(5).getType()).isEqualTo(CrudOperable.BatchResult.Type.UPDATE);
    assertThat(results.get(6).getType()).isEqualTo(CrudOperable.BatchResult.Type.DELETE);
  }

  @Test
  public void batch_EmptyOperationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transaction.batch(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void prepare_ProcessedCrudGiven_ShouldPrepareRecordsWithSnapshot()
      throws PreparationException, CrudException {
    // Arrange

    // Act
    transaction.prepare();

    // Assert
    verify(context).areAllScannersClosed();
    verify(crud).readIfImplicitPreReadEnabled(context);
    verify(crud).waitForRecoveryCompletionIfNecessary(context);
    verify(commit).prepareRecords(context);
  }

  @Test
  public void
      prepare_ProcessedCrudGiven_CrudConflictExceptionThrownWhileImplicitPreRead_ShouldThrowPreparationConflictException()
          throws CrudException {
    // Arrange
    doThrow(CrudConflictException.class).when(crud).readIfImplicitPreReadEnabled(context);

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationConflictException.class);
  }

  @Test
  public void
      prepare_ProcessedCrudGiven_CrudExceptionThrownWhileImplicitPreRead_ShouldThrowPreparationException()
          throws CrudException {
    // Arrange
    doThrow(CrudException.class).when(crud).readIfImplicitPreReadEnabled(context);

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
  }

  @Test
  public void prepare_ScannerNotClosed_ShouldThrowIllegalStateException() {
    // Arrange
    when(context.areAllScannersClosed()).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void
      prepare_CrudConflictExceptionThrownByCrudHandlerWaitForRecoveryCompletionIfNecessary_ShouldThrowPreparationConflictException()
          throws CrudException {
    // Arrange
    CrudConflictException crudConflictException = mock(CrudConflictException.class);
    when(crudConflictException.getMessage()).thenReturn("error");
    doThrow(crudConflictException).when(crud).waitForRecoveryCompletionIfNecessary(context);

    // Act Assert
    assertThatThrownBy(() -> transaction.prepare())
        .isInstanceOf(PreparationConflictException.class);
  }

  @Test
  public void
      prepare_CrudExceptionThrownByCrudHandlerWaitForRecoveryCompletionIfNecessary_ShouldThrowPreparationException()
          throws CrudException {
    // Arrange
    CrudException crudException = mock(CrudException.class);
    when(crudException.getMessage()).thenReturn("error");
    doThrow(crudException).when(crud).waitForRecoveryCompletionIfNecessary(context);

    // Act Assert
    assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(PreparationException.class);
  }

  @Test
  public void validate_ProcessedCrudGiven_ShouldValidateRecordsWithSnapshot()
      throws ValidationException, PreparationException {
    // Arrange
    transaction.prepare();

    // Act
    transaction.validate();

    // Assert
    verify(commit).validateRecords(context);
  }

  @Test
  public void commit_ShouldCommitStateAndRecords()
      throws CommitException, UnknownTransactionStatusException, PreparationException {
    // Arrange
    transaction.prepare();

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(context);
    verify(commit).commitRecords(context);
  }

  @Test
  public void commit_SerializableUsedAndValidatedState_ShouldCommitProperly()
      throws CommitException, UnknownTransactionStatusException, PreparationException,
          ValidationException {
    // Arrange
    transaction.prepare();
    transaction.validate();
    when(context.isValidationRequired()).thenReturn(true);

    // Act
    transaction.commit();

    // Assert
    verify(commit).commitState(context);
    verify(commit).commitRecords(context);
  }

  @Test
  public void commit_SerializableUsedAndPreparedState_ShouldThrowIllegalStateException()
      throws PreparationException {
    // Arrange
    transaction.prepare();
    when(context.isValidationRequired()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(transaction::commit).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void rollback_ShouldAbortStateAndRollbackRecords() throws TransactionException {
    // Arrange
    transaction.prepare();

    // Act
    transaction.rollback();

    // Assert
    verify(context).closeScanners();
    verify(commit).abortState(ANY_TX_ID);
    verify(commit).rollbackRecords(context);
  }

  @Test
  public void rollback_CalledAfterPrepareFails_ShouldAbortStateAndRollbackRecords()
      throws TransactionException {
    // Arrange
    doThrow(PreparationException.class).when(commit).prepareRecords(context);

    // Act
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    verify(context).closeScanners();
    verify(commit).abortState(ANY_TX_ID);
    verify(commit).rollbackRecords(context);
  }

  @Test
  public void rollback_CalledAfterCommitFails_ShouldNeverAbortStateAndRollbackRecords()
      throws TransactionException {
    // Arrange
    transaction.prepare();
    doThrow(CommitConflictException.class).when(commit).commitState(context);

    // Act
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    verify(context).closeScanners();
    verify(commit, never()).abortState(ANY_TX_ID);
    verify(commit, never()).rollbackRecords(context);
  }

  @Test
  public void
      rollback_UnknownTransactionStatusExceptionThrownByAbortState_ShouldThrowRollbackException()
          throws TransactionException {
    // Arrange
    transaction.prepare();
    when(commit.abortState(ANY_TX_ID)).thenThrow(UnknownTransactionStatusException.class);

    // Act Assert
    assertThatThrownBy(transaction::rollback).isInstanceOf(RollbackException.class);

    verify(context).closeScanners();
    verify(commit, never()).rollbackRecords(context);
  }

  @Test
  public void rollback_CommittedStateReturnedByAbortState_ShouldThrowRollbackException()
      throws TransactionException {
    // Arrange
    transaction.prepare();
    when(commit.abortState(ANY_TX_ID)).thenReturn(TransactionState.COMMITTED);

    // Act Assert
    assertThatThrownBy(transaction::rollback).isInstanceOf(RollbackException.class);

    verify(context).closeScanners();
    verify(commit, never()).rollbackRecords(context);
  }
}
