package com.scalar.db.transaction.autocommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AutoCommitTransactionTest {

  private static final String TRANSACTION_ID = "id";

  @Mock private DistributedStorage storage;

  private AutoCommitTransaction transaction;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    transaction = new AutoCommitTransaction(TRANSACTION_ID, storage);
  }

  @Test
  public void getId_ShouldReturnTransactionId() {
    // Arrange

    // Act
    String id = transaction.getId();

    // Assert
    assertThat(id).isEqualTo(TRANSACTION_ID);
  }

  @Test
  public void get_ShouldReturnResult() throws ExecutionException, CrudException {
    // Arrange
    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Get getWithLinearizableConsistency =
        Get.newBuilder(get).consistency(com.scalar.db.api.Consistency.LINEARIZABLE).build();

    Result result = mock(Result.class);
    when(storage.get(getWithLinearizableConsistency)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = transaction.get(get);

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
    assertThatThrownBy(() -> transaction.get(get))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void scan_ShouldReturnResults() throws ExecutionException, CrudException {
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
    List<Result> actual = transaction.scan(scan);

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
    assertThatThrownBy(() -> transaction.scan(scan))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void put_ShouldCallStorageProperly() throws ExecutionException, CrudException {
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
    transaction.put(put);

    // Assert
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
    assertThatThrownBy(() -> transaction.put(put))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void put_NoMutationExceptionThrownByStorage_ShouldThrowUnsatisfiedConditionException()
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

    NoMutationException exception = new NoMutationException("error");
    doThrow(exception).when(storage).put(putWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transaction.put(put))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void put_MultiplePutGiven_ShouldCallStorageProperly()
      throws ExecutionException, CrudException {
    // Arrange
    Put put1 =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 1))
            .intValue("col", 1)
            .build();
    Put put1WithLinearizableConsistency =
        Put.newBuilder(put1).consistency(Consistency.LINEARIZABLE).build();
    Put put2WithLinearizableConsistency =
        Put.newBuilder(put2).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transaction.put(Arrays.asList(put1, put2));

    // Assert
    verify(storage).put(put1WithLinearizableConsistency);
    verify(storage).put(put2WithLinearizableConsistency);
  }

  @Test
  public void delete_ShouldCallStorageProperly() throws ExecutionException, CrudException {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transaction.delete(delete);

    // Assert
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
    assertThatThrownBy(() -> transaction.delete(delete))
        .isInstanceOf(CrudException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void delete_NoMutationExceptionThrownByStorage_ShouldThrowUnsatisfiedConditionException()
      throws ExecutionException {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    NoMutationException exception = new NoMutationException("error");
    doThrow(exception).when(storage).delete(deleteWithLinearizableConsistency);

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(delete))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining("error")
        .hasCause(exception);
  }

  @Test
  public void delete_MultipleDeleteGiven_ShouldCallStorageProperly()
      throws ExecutionException, CrudException {
    // Arrange
    Delete delete1 =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete delete2 =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 1)).build();
    Delete delete1WithLinearizableConsistency =
        Delete.newBuilder(delete1).consistency(Consistency.LINEARIZABLE).build();
    Delete delete2WithLinearizableConsistency =
        Delete.newBuilder(delete2).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transaction.delete(Arrays.asList(delete1, delete2));

    // Assert
    verify(storage).delete(delete1WithLinearizableConsistency);
    verify(storage).delete(delete2WithLinearizableConsistency);
  }

  @Test
  public void mutate_ShouldCallStorageProperly() throws ExecutionException, CrudException {
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
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("id", 0)).build();
    Delete deleteWithLinearizableConsistency =
        Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build();

    // Act
    transaction.mutate(Arrays.asList(put, delete));

    // Assert
    verify(storage).put(putWithLinearizableConsistency);
    verify(storage).delete(deleteWithLinearizableConsistency);
  }
}
