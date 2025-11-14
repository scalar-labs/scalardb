package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
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
  private static final String ANY_ID = "id";

  private TransactionContext context;
  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private ConsensusCommitOperationChecker operationChecker;

  private ConsensusCommit consensus;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    context = spy(new TransactionContext(ANY_ID, snapshot, Isolation.SNAPSHOT, false, false));
    consensus = new ConsensusCommit(context, crud, commit, operationChecker, null);
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
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    doNothing().when(operationChecker).check(get, context);
    TransactionResult result = mock(TransactionResult.class);
    when(crud.get(get, context)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = consensus.get(get);

    // Assert
    assertThat(actual).isPresent();
    verify(operationChecker).check(get, context);
    verify(crud).get(get, context);
  }

  @Test
  public void get_OperationCheckerThrowsExecutionException_ShouldThrowCrudException()
      throws ExecutionException, CrudException {
    // Arrange
    Get get = prepareGet();
    ExecutionException exception = new ExecutionException("operation check failed");
    doThrow(exception).when(operationChecker).check(get, context);

    // Act Assert
    assertThatThrownBy(() -> consensus.get(get))
        .isInstanceOf(CrudException.class)
        .hasMessage("operation check failed. Transaction ID: " + ANY_ID)
        .hasCause(exception);
    verify(operationChecker).check(get, context);
    verify(crud, never()).get(any(), any());
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException, ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    doNothing().when(operationChecker).check(scan, context);
    TransactionResult result = mock(TransactionResult.class);
    List<Result> results = Collections.singletonList(result);
    when(crud.scan(scan, context)).thenReturn(results);

    // Act
    List<Result> actual = consensus.scan(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    verify(operationChecker).check(scan, context);
    verify(crud).scan(scan, context);
  }

  @Test
  public void scan_OperationCheckerThrowsExecutionException_ShouldThrowCrudException()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    ExecutionException exception = new ExecutionException("operation check failed");
    doThrow(exception).when(operationChecker).check(scan, context);

    // Act Assert
    assertThatThrownBy(() -> consensus.scan(scan))
        .isInstanceOf(CrudException.class)
        .hasMessage("operation check failed. Transaction ID: " + ANY_ID)
        .hasCause(exception);
    verify(operationChecker).check(scan, context);
    verify(crud, never()).scan(any(), any());
  }

  @Test
  public void getScannerAndScannerOne_ShouldCallCrudHandlerGetScannerAndScannerOne()
      throws CrudException, ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    doNothing().when(operationChecker).check(scan, context);
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    Result result = mock(Result.class);
    when(scanner.one()).thenReturn(Optional.of(result));
    when(crud.getScanner(scan, context)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    Optional<Result> actualResult = actualScanner.one();

    // Assert
    assertThat(actualResult).hasValue(result);
    verify(operationChecker).check(scan, context);
    verify(crud).getScanner(scan, context);
    verify(scanner).one();
  }

  @Test
  public void getScannerAndScannerAll_ShouldCallCrudHandlerGetScannerAndScannerAll()
      throws CrudException, ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    doNothing().when(operationChecker).check(scan, context);
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    when(scanner.all()).thenReturn(Arrays.asList(result1, result2));
    when(crud.getScanner(scan, context)).thenReturn(scanner);

    // Act
    TransactionCrudOperable.Scanner actualScanner = consensus.getScanner(scan);
    List<Result> actualResults = actualScanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2);
    verify(operationChecker).check(scan, context);
    verify(crud).getScanner(scan, context);
    verify(scanner).all();
  }

  @Test
  public void getScanner_OperationCheckerThrowsExecutionException_ShouldThrowCrudException()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    ExecutionException exception = new ExecutionException("operation check failed");
    doThrow(exception).when(operationChecker).check(scan, context);

    // Act Assert
    assertThatThrownBy(() -> consensus.getScanner(scan))
        .isInstanceOf(CrudException.class)
        .hasMessage("operation check failed. Transaction ID: " + ANY_ID)
        .hasCause(exception);
    verify(operationChecker).check(scan, context);
    verify(crud, never()).getScanner(any(), any());
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put, context);

    // Act
    consensus.put(put);

    // Assert
    verify(crud).put(put, context);
    verify(operationChecker).check(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put, context);

    // Act
    consensus.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put, context);
    verify(operationChecker, times(2)).check(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete()
      throws CrudException, ExecutionException {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete, context);

    // Act
    consensus.delete(delete);

    // Assert
    verify(crud).delete(delete, context);
    verify(operationChecker).check(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice()
      throws ExecutionException, CrudException {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete, context);

    // Act
    consensus.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete, context);
    verify(operationChecker, times(2)).check(delete);
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
    verify(crud).put(expectedPut, context);
    verify(operationChecker).check(expectedPut);
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
    verify(crud).put(expectedPut, context);
    verify(operationChecker).check(expectedPut);
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
    verify(crud).put(expectedPut, context);
    verify(operationChecker).check(expectedPut);
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
    verify(crud).put(expectedPut, context);
    verify(operationChecker).check(expectedPut);
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

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIf");
    doThrow(unsatisfiedConditionException).when(crud).put(put, context);

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

    UnsatisfiedConditionException unsatisfiedConditionException =
        mock(UnsatisfiedConditionException.class);
    when(unsatisfiedConditionException.getMessage()).thenReturn("PutIfExists");
    doThrow(unsatisfiedConditionException).when(crud).put(put, context);

    // Act Assert
    assertThatThrownBy(() -> consensus.update(update))
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
    consensus.mutate(Arrays.asList(put, insert, upsert, update, delete));

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
    verify(operationChecker).check(put);
    verify(operationChecker).check(expectedPutFromInsert);
    verify(operationChecker).check(expectedPutFromUpsert);
    verify(operationChecker).check(expectedPutFromUpdate);
    verify(operationChecker).check(delete);
  }

  @Test
  public void mutate_EmptyMutationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> consensus.mutate(Collections.emptyList()))
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
        consensus.batch(Arrays.asList(get, scan, put, insert, upsert, update, delete));

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
    verify(operationChecker).check(put);
    verify(operationChecker).check(expectedPutFromInsert);
    verify(operationChecker).check(expectedPutFromUpsert);
    verify(operationChecker).check(expectedPutFromUpdate);
    verify(operationChecker).check(delete);
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
    assertThatThrownBy(() -> consensus.batch(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void commit_ProcessedCrudGiven_ShouldCommitWithSnapshot()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    doNothing().when(commit).commit(any(TransactionContext.class));

    // Act
    consensus.commit();

    // Assert
    verify(context).areAllScannersClosed();
    verify(crud).readIfImplicitPreReadEnabled(context);
    verify(crud).waitForRecoveryCompletionIfNecessary(context);
    verify(commit).commit(context);
  }

  @Test
  public void commit_ProcessedCrudGiven_InReadOnlyMode_ShouldCommitWithSnapshot()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    doNothing().when(commit).commit(any(TransactionContext.class));
    context = spy(new TransactionContext(ANY_ID, snapshot, Isolation.SNAPSHOT, true, false));
    consensus = new ConsensusCommit(context, crud, commit, operationChecker, null);

    // Act
    consensus.commit();

    // Assert
    verify(context).areAllScannersClosed();
    verify(crud).readIfImplicitPreReadEnabled(context);
    verify(crud).waitForRecoveryCompletionIfNecessary(context);
    verify(commit).commit(context);
  }

  @Test
  public void
      commit_ProcessedCrudGiven_CrudConflictExceptionThrownWhileImplicitPreRead_ShouldThrowCommitConflictException()
          throws CrudException {
    // Arrange
    doThrow(CrudConflictException.class).when(crud).readIfImplicitPreReadEnabled(context);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_ProcessedCrudGiven_CrudExceptionThrownWhileImplicitPreRead_ShouldThrowCommitException()
          throws CrudException {
    // Arrange
    doThrow(CrudException.class).when(crud).readIfImplicitPreReadEnabled(context);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitException.class);
  }

  @Test
  public void commit_ScannerNotClosed_ShouldThrowIllegalStateException() {
    // Arrange
    when(context.areAllScannersClosed()).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void
      commit_CrudConflictExceptionThrownByCrudHandlerWaitForRecoveryCompletionIfNecessary_ShouldThrowCommitConflictException()
          throws CrudException {
    // Arrange
    CrudConflictException crudConflictException = mock(CrudConflictException.class);
    when(crudConflictException.getMessage()).thenReturn("error");
    doThrow(crudConflictException).when(crud).waitForRecoveryCompletionIfNecessary(context);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      commit_CrudExceptionThrownByCrudHandlerWaitForRecoveryCompletionIfNecessary_ShouldThrowCommitException()
          throws CrudException {
    // Arrange
    CrudException crudException = mock(CrudException.class);
    when(crudException.getMessage()).thenReturn("error");
    doThrow(crudException).when(crud).waitForRecoveryCompletionIfNecessary(context);

    // Act Assert
    assertThatThrownBy(() -> consensus.commit()).isInstanceOf(CommitException.class);
  }

  @Test
  public void rollback_ShouldDoNothing() throws CrudException, UnknownTransactionStatusException {
    // Arrange

    // Act
    consensus.rollback();

    // Assert
    verify(context).closeScanners();
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    verify(commit, never()).abortState(anyString());
  }

  @Test
  public void rollback_WithGroupCommitter_ShouldRemoveTxFromGroupCommitter()
      throws CrudException, UnknownTransactionStatusException {
    // Arrange
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    ConsensusCommit consensusWithGroupCommit =
        new ConsensusCommit(context, crud, commit, operationChecker, groupCommitter);

    // Act
    consensusWithGroupCommit.rollback();

    // Assert
    verify(context).closeScanners();
    verify(groupCommitter).remove(ANY_ID);
    verify(commit, never()).rollbackRecords(context);
    verify(commit, never()).abortState(anyString());
  }

  @Test
  public void rollback_WithGroupCommitter_InReadOnlyMode_ShouldNotRemoveTxFromGroupCommitter()
      throws CrudException, UnknownTransactionStatusException {
    // Arrange
    context = spy(new TransactionContext(ANY_ID, snapshot, Isolation.SNAPSHOT, true, false));
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    ConsensusCommit consensusWithGroupCommit =
        new ConsensusCommit(context, crud, commit, operationChecker, groupCommitter);

    // Act
    consensusWithGroupCommit.rollback();

    // Assert
    verify(context).closeScanners();
    verify(groupCommitter, never()).remove(anyString());
    verify(commit, never()).rollbackRecords(any(TransactionContext.class));
    verify(commit, never()).abortState(anyString());
  }
}
