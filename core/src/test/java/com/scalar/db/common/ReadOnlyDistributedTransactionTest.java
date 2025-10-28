package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.CrudOperable.BatchResult;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ReadOnlyDistributedTransactionTest {

  private static final String TRANSACTION_ID = "test-tx-id";

  @Mock private DistributedTransaction transaction;

  private ReadOnlyDistributedTransaction readOnlyTransaction;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(transaction.getId()).thenReturn(TRANSACTION_ID);
    readOnlyTransaction = new ReadOnlyDistributedTransaction(transaction);
  }

  @Test
  public void get_ShouldDelegateToUnderlyingTransaction() throws CrudException {
    // Arrange
    Get get = mock(Get.class);
    Optional<Result> expected = Optional.of(mock(Result.class));
    when(transaction.get(get)).thenReturn(expected);

    // Act
    Optional<Result> actual = readOnlyTransaction.get(get);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(transaction).get(get);
  }

  @Test
  public void scan_ShouldDelegateToUnderlyingTransaction() throws CrudException {
    // Arrange
    Scan scan = mock(Scan.class);
    List<Result> expected = Arrays.asList(mock(Result.class), mock(Result.class));
    when(transaction.scan(scan)).thenReturn(expected);

    // Act
    List<Result> actual = readOnlyTransaction.scan(scan);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(transaction).scan(scan);
  }

  @Test
  public void getScanner_ShouldDelegateToUnderlyingTransaction() throws CrudException {
    // Arrange
    Scan scan = mock(Scan.class);
    Scanner expected = mock(Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(expected);

    // Act
    Scanner actual = readOnlyTransaction.getScanner(scan);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(transaction).getScanner(scan);
  }

  @Test
  public void put_ShouldThrowIllegalStateException() {
    // Arrange
    Put put = mock(Put.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.put(put))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void put_WithList_ShouldThrowIllegalStateException() {
    // Arrange
    @SuppressWarnings("unchecked")
    List<Put> puts = (List<Put>) mock(List.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.put(puts))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void insert_ShouldThrowIllegalStateException() {
    // Arrange
    Insert insert = mock(Insert.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.insert(insert))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void upsert_ShouldThrowIllegalStateException() {
    // Arrange
    Upsert upsert = mock(Upsert.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.upsert(upsert))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void update_ShouldThrowIllegalStateException() {
    // Arrange
    Update update = mock(Update.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.update(update))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void delete_ShouldThrowIllegalStateException() {
    // Arrange
    Delete delete = mock(Delete.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.delete(delete))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void delete_WithList_ShouldThrowIllegalStateException() {
    // Arrange
    @SuppressWarnings("unchecked")
    List<Delete> deletes = (List<Delete>) mock(List.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.delete(deletes))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void mutate_ShouldThrowIllegalStateException() {
    // Arrange
    @SuppressWarnings("unchecked")
    List<Mutation> mutations = (List<Mutation>) mock(List.class);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.mutate(mutations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithPut_ShouldThrowIllegalStateException() {
    // Arrange
    Put put = mock(Put.class);
    List<Operation> operations = Collections.singletonList(put);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithInsert_ShouldThrowIllegalStateException() {
    // Arrange
    Insert insert = mock(Insert.class);
    List<Operation> operations = Collections.singletonList(insert);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithUpsert_ShouldThrowIllegalStateException() {
    // Arrange
    Upsert upsert = mock(Upsert.class);
    List<Operation> operations = Collections.singletonList(upsert);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithUpdate_ShouldThrowIllegalStateException() {
    // Arrange
    Update update = mock(Update.class);
    List<Operation> operations = Collections.singletonList(update);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithDelete_ShouldThrowIllegalStateException() {
    // Arrange
    Delete delete = mock(Delete.class);
    List<Operation> operations = Collections.singletonList(delete);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithMixedOperationsIncludingMutation_ShouldThrowIllegalStateException() {
    // Arrange
    Get get = mock(Get.class);
    Put put = mock(Put.class);
    List<Operation> operations = Arrays.asList(get, put);

    // Act Assert
    assertThatThrownBy(() -> readOnlyTransaction.batch(operations))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mutations are not allowed in read-only transactions")
        .hasMessageContaining(TRANSACTION_ID);
  }

  @Test
  public void batch_WithOnlyReadOperations_ShouldDelegateToUnderlyingTransaction()
      throws CrudException {
    // Arrange
    Get get = mock(Get.class);
    Scan scan = mock(Scan.class);
    List<Operation> operations = Arrays.asList(get, scan);
    @SuppressWarnings("unchecked")
    List<BatchResult> expected = (List<BatchResult>) mock(List.class);
    when(transaction.batch(any())).thenReturn(expected);

    // Act
    List<BatchResult> actual = readOnlyTransaction.batch(operations);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(transaction).batch(operations);
  }

  @Test
  public void batch_WithEmptyOperations_ShouldDelegateToUnderlyingTransaction()
      throws CrudException {
    // Arrange
    List<Operation> operations = Collections.emptyList();
    @SuppressWarnings("unchecked")
    List<BatchResult> expected = (List<BatchResult>) mock(List.class);
    when(transaction.batch(any())).thenReturn(expected);

    // Act
    List<BatchResult> actual = readOnlyTransaction.batch(operations);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(transaction).batch(operations);
  }

  @Test
  public void commit_ShouldDelegateToUnderlyingTransaction() {
    // Act Assert
    assertThatCode(() -> readOnlyTransaction.commit()).doesNotThrowAnyException();
    assertThatCode(() -> verify(transaction).commit()).doesNotThrowAnyException();
  }

  @Test
  public void rollback_ShouldDelegateToUnderlyingTransaction() {
    // Act Assert
    assertThatCode(() -> readOnlyTransaction.rollback()).doesNotThrowAnyException();
    assertThatCode(() -> verify(transaction).rollback()).doesNotThrowAnyException();
  }

  @Test
  public void abort_ShouldDelegateToUnderlyingTransaction() {
    // Act Assert
    assertThatCode(() -> readOnlyTransaction.abort()).doesNotThrowAnyException();
    assertThatCode(() -> verify(transaction).abort()).doesNotThrowAnyException();
  }

  @Test
  public void getId_ShouldReturnTransactionId() {
    // Act
    String actual = readOnlyTransaction.getId();

    // Assert
    assertThat(actual).isEqualTo(TRANSACTION_ID);
  }
}
