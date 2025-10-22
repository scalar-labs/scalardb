package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ActiveTransactionManagedTwoPhaseCommitTransactionManagerTest {

  @Mock private TwoPhaseCommitTransactionManager wrappedTransactionManager;

  private ActiveTransactionManagedTwoPhaseCommitTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(wrappedTransactionManager, -1);
  }

  @Test
  public void begin_ShouldReturnActiveTransaction() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1").thenReturn("txId2");

    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.begin(anyString())).thenReturn(wrappedTransaction);

    // Act
    TwoPhaseCommitTransaction transaction1 = transactionManager.begin();
    TwoPhaseCommitTransaction transaction2 = transactionManager.begin("txId2");

    // Assert
    assertThat(transaction1)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);
  }

  @Test
  public void start_ShouldReturnActiveTransaction() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1").thenReturn("txId2");

    when(wrappedTransactionManager.start()).thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(anyString())).thenReturn(wrappedTransaction);

    // Act
    TwoPhaseCommitTransaction transaction1 = transactionManager.start();
    TwoPhaseCommitTransaction transaction2 = transactionManager.start("txId2");

    // Assert
    assertThat(transaction1)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);
  }

  @Test
  public void join_ActiveTransactionExists_ShouldReturnBegunActiveTransaction()
      throws TransactionException {
    // Arrange
    String txId = "txId1";

    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(wrappedTransaction);

    TwoPhaseCommitTransaction expected = transactionManager.begin(txId);

    // Act
    TwoPhaseCommitTransaction actual = transactionManager.join(txId);

    // Assert
    assertThat(actual)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);

    assertThat(((DecoratedTwoPhaseCommitTransaction) actual).getOriginalTransaction())
        .isEqualTo(((DecoratedTwoPhaseCommitTransaction) expected).getOriginalTransaction());
  }

  @Test
  public void join_ActiveTransactionNotExists_ShouldReturnActiveTransaction()
      throws TransactionException {
    // Arrange
    String txId = "txId1";

    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.join(txId)).thenReturn(wrappedTransaction);

    // Act
    TwoPhaseCommitTransaction actual = transactionManager.join(txId);

    // Assert
    assertThat(actual)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);
  }

  @Test
  public void resume_ShouldReturnBegunActiveTransaction() throws TransactionException {
    // Arrange
    String txId = "txId1";

    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);
    when(transaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(transaction);

    TwoPhaseCommitTransaction expected = transactionManager.begin(txId);

    // Act
    TwoPhaseCommitTransaction actual = transactionManager.resume(txId);

    // Assert
    assertThat(actual)
        .isInstanceOf(
            ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveTransaction.class);

    assertThat(((DecoratedTwoPhaseCommitTransaction) actual).getOriginalTransaction())
        .isEqualTo(((DecoratedTwoPhaseCommitTransaction) expected).getOriginalTransaction());
  }

  @Test
  public void resume_ActiveTransactionAlreadyRolledBack_ShouldThrowTransactionNotFoundException()
      throws TransactionException {
    String txId = "txId1";

    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);
    when(transaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(transaction);

    TwoPhaseCommitTransaction activeTransaction = transactionManager.begin(txId);
    activeTransaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> transactionManager.resume(txId))
        .isInstanceOf(TransactionNotFoundException.class);
  }
}
