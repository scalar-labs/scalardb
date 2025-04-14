package com.scalar.db.transaction.singlecrudoperation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SingleCrudOperationTransactionManagerTest {

  @Mock private DistributedStorage storage;
  @Mock private DatabaseConfig databaseConfig;

  private SingleCrudOperationTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    transactionManager = new SingleCrudOperationTransactionManager(databaseConfig, storage);
  }

  @Test
  public void begin_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionManager.begin())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> transactionManager.begin("id"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> transactionManager.start())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> transactionManager.start("id"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void resume_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionManager.resume("id"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void get_ShouldReturnResult() throws ExecutionException, TransactionException {
    // Arrange
    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Get getWithLinearizableConsistency =
        Get.newBuilder(get).consistency(com.scalar.db.api.Consistency.LINEARIZABLE).build();

    Result result = mock(Result.class);
    when(storage.get(getWithLinearizableConsistency)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = transactionManager.get(get);

    // Assert
    verify(storage).get(getWithLinearizableConsistency);
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void get_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Get getWithLinearizableConsistency =
        Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build();

    ExecutionException exception = new ExecutionException("error");
    when(storage.get(getWithLinearizableConsistency)).thenThrow(exception);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.get(get))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void scan_ShouldReturnResults() throws ExecutionException, TransactionException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Scan scanWithLinearizableConsistency =
        Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build();

    List<Result> results = Arrays.asList(mock(Result.class), mock(Result.class));
    Scanner scanner = mock(Scanner.class);
    when(scanner.all()).thenReturn(results);
    when(storage.scan(scanWithLinearizableConsistency)).thenReturn(scanner);

    // Act
    List<Result> actual = transactionManager.scan(scan);

    // Assert
    verify(storage).scan(scanWithLinearizableConsistency);
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void scan_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Scan scanWithLinearizableConsistency =
        Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build();

    ExecutionException exception = new ExecutionException("error");
    when(storage.scan(scanWithLinearizableConsistency)).thenThrow(exception);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.scan(scan))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void getScannerAndScannerOne_ShouldReturnScannerAndShouldReturnProperResult()
      throws ExecutionException, TransactionException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);
    assertThat(actual.one()).hasValue(result1);
    assertThat(actual.one()).hasValue(result2);
    assertThat(actual.one()).hasValue(result3);
    assertThat(actual.one()).isEmpty();
    actual.close();

    verify(storage).scan(scan);
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerAll_ShouldReturnScannerAndShouldReturnProperResults()
      throws ExecutionException, TransactionException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.all()).thenReturn(Arrays.asList(result1, result2, result3));

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);
    assertThat(actual.all()).containsExactly(result1, result2, result3);
    actual.close();

    verify(storage).scan(scan);
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerIterator_ShouldReturnScannerAndShouldReturnProperResults()
      throws ExecutionException, TransactionException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);

    Iterator<Result> iterator = actual.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result1);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    actual.close();

    verify(storage).scan(scan);
    verify(scanner).close();
  }

  @Test
  public void getScanner_WhenExecutionExceptionThrownByJdbcService_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(storage.scan(scan)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.getScanner(scan)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerOne_WhenExecutionExceptionThrownByScannerOne_ShouldThrowCrudException()
          throws ExecutionException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Scanner scanner = mock(Scanner.class);

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(scanner.one()).thenThrow(executionException);

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);
    assertThatThrownBy(actual::one).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerAll_WhenExecutionExceptionThrownByScannerAll_ShouldThrowCrudException()
          throws ExecutionException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Scanner scanner = mock(Scanner.class);

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(scanner.all()).thenThrow(executionException);

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);
    assertThatThrownBy(actual::all).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerClose_WhenIOExceptionThrownByScannerClose_ShouldThrowCrudException()
          throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();

    Scanner scanner = mock(Scanner.class);

    IOException ioException = mock(IOException.class);
    when(ioException.getMessage()).thenReturn("error");
    doThrow(ioException).when(scanner).close();

    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = transactionManager.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);
  }

  @Test
  public void put_ShouldCallStorageProperly() throws ExecutionException, TransactionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();
    Put putWithLinearizableConsistency =
        Put.newBuilder(put).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transactionManager.put(put);

    // Verify
    verify(storage).put(putWithLinearizableConsistency);
  }

  @Test
  public void put_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();
    Put putWithLinearizableConsistency =
        Put.newBuilder(put).consistency(Consistency.LINEARIZABLE).build();

    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(storage).put(putWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.put(put))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void
      put_WithCondition_NoMutationExceptionThrownByStorage_ShouldThrowUnsatisfiedConditionException()
          throws ExecutionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    Put putWithLinearizableConsistency =
        Put.newBuilder(put).consistency(Consistency.LINEARIZABLE).build();

    doThrow(NoMutationException.class).when(storage).put(putWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.put(put))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void insert_ShouldCallStorageProperly() throws ExecutionException, TransactionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    // Act
    transactionManager.insert(insert);

    // Verify
    verify(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void insert_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    ExecutionException exception = new ExecutionException("error");
    doThrow(exception)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatThrownBy(() -> transactionManager.insert(insert))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void insert_NoMutationExceptionThrownByStorage_ShouldThrowCrudConflictException()
      throws ExecutionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    doThrow(NoMutationException.class)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatThrownBy(() -> transactionManager.insert(insert))
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void upsert_ShouldCallStorageProperly() throws ExecutionException, TransactionException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    // Act
    transactionManager.upsert(upsert);

    // Verify
    verify(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void upsert_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    ExecutionException exception = new ExecutionException("error");
    doThrow(exception)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatThrownBy(() -> transactionManager.upsert(upsert))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void update_WithoutCondition_ShouldCallStorageProperly()
      throws ExecutionException, TransactionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    // Act
    transactionManager.update(update);

    // Verify
    verify(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void update_WithCondition_ShouldCallStorageProperly()
      throws ExecutionException, TransactionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column("col").isEqualToInt(1)).build())
            .build();

    // Act
    transactionManager.update(update);

    // Verify
    verify(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(
                    ConditionBuilder.putIf(ConditionBuilder.column("col").isEqualToInt(1)).build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void update_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    ExecutionException exception = new ExecutionException("error");
    doThrow(exception)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatThrownBy(() -> transactionManager.update(update))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void update_WithoutCondition_NoMutationExceptionThrownByStorage_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    doThrow(NoMutationException.class)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(ConditionBuilder.putIfExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatCode(() -> transactionManager.update(update)).doesNotThrowAnyException();
  }

  @Test
  public void
      update_WithCondition_NoMutationExceptionThrownByStorage_ShouldThrowUnsatisfiedConditionException()
          throws ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column("col").isEqualToInt(1)).build())
            .build();

    doThrow(NoMutationException.class)
        .when(storage)
        .put(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("id", 0))
                .intValue("col", 0)
                .condition(
                    ConditionBuilder.putIf(ConditionBuilder.column("col").isEqualToInt(1)).build())
                .consistency(Consistency.LINEARIZABLE)
                .build());

    // Act Assert
    assertThatThrownBy(() -> transactionManager.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }

  @Test
  public void delete_ShouldCallStorageProperly() throws ExecutionException, TransactionException {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transactionManager.delete(delete);

    // Verify
    verify(storage).delete(deleteWithLinearizableConsistency);
  }

  @Test
  public void delete_ExecutionExceptionThrownByStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(storage).delete(deleteWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.delete(delete))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void
      delete_WithCondition_NoMutationExceptionThrownByStorage_ShouldThrowUnsatisfiedConditionException()
          throws ExecutionException {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .condition(ConditionBuilder.deleteIfExists())
            .build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    doThrow(NoMutationException.class).when(storage).delete(deleteWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.delete(delete))
        .isInstanceOf(UnsatisfiedConditionException.class);
  }
}
