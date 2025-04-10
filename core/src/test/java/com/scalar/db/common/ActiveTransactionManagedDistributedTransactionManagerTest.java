package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ActiveTransactionManagedDistributedTransactionManagerTest {

  @Mock private DistributedTransactionManager wrappedTransactionManager;

  private ActiveTransactionManagedDistributedTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new ActiveTransactionManagedDistributedTransactionManager(wrappedTransactionManager, -1);
  }

  @Test
  public void begin_ShouldReturnActiveTransaction() throws TransactionException {
    // Arrange
    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1").thenReturn("txId2");

    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.begin(anyString())).thenReturn(wrappedTransaction);

    // Act
    DistributedTransaction transaction1 = transactionManager.begin();
    DistributedTransaction transaction2 = transactionManager.begin("txId2");

    // Assert
    assertThat(transaction1)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
  }

  @Test
  public void start_ShouldReturnActiveTransaction() throws TransactionException {
    // Arrange
    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId())
        .thenReturn("txId1")
        .thenReturn("txId2")
        .thenReturn("txId3")
        .thenReturn("txId4")
        .thenReturn("txId5")
        .thenReturn("txId6")
        .thenReturn("txId7")
        .thenReturn("txId8");

    when(wrappedTransactionManager.start()).thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(anyString())).thenReturn(wrappedTransaction);

    when(wrappedTransactionManager.start(any(Isolation.class))).thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(anyString(), any(Isolation.class)))
        .thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(any(Isolation.class), any(SerializableStrategy.class)))
        .thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(any(SerializableStrategy.class)))
        .thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(anyString(), any(SerializableStrategy.class)))
        .thenReturn(wrappedTransaction);
    when(wrappedTransactionManager.start(
            anyString(), any(Isolation.class), any(SerializableStrategy.class)))
        .thenReturn(wrappedTransaction);

    // Act
    DistributedTransaction transaction1 = transactionManager.start();
    DistributedTransaction transaction2 = transactionManager.start("txId2");
    DistributedTransaction transaction3 = transactionManager.start(Isolation.SERIALIZABLE);
    DistributedTransaction transaction4 = transactionManager.start("txId4", Isolation.SERIALIZABLE);
    DistributedTransaction transaction5 =
        transactionManager.start(
            Isolation.SERIALIZABLE,
            com.scalar.db.transaction.consensuscommit.SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction6 =
        transactionManager.start(
            com.scalar.db.transaction.consensuscommit.SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction7 =
        transactionManager.start(
            "txId7", com.scalar.db.transaction.consensuscommit.SerializableStrategy.EXTRA_READ);
    DistributedTransaction transaction8 =
        transactionManager.start(
            "txId8",
            Isolation.SERIALIZABLE,
            com.scalar.db.transaction.consensuscommit.SerializableStrategy.EXTRA_READ);

    // Assert
    assertThat(transaction1)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction3)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction4)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction5)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction6)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction7)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
    assertThat(transaction8)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);
  }

  @Test
  public void join_ShouldReturnBegunActiveTransaction() throws TransactionException {
    // Arrange
    String txId = "txId1";

    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(wrappedTransaction);

    DistributedTransaction expected = transactionManager.begin(txId);

    // Act
    DistributedTransaction actual = transactionManager.join(txId);

    // Assert
    assertThat(actual)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);

    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isEqualTo(((DecoratedDistributedTransaction) expected).getOriginalTransaction());
  }

  @Test
  public void join_ActiveTransactionAlreadyCommitted_ShouldThrowTransactionNotFoundException()
      throws TransactionException {
    String txId = "txId1";

    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(wrappedTransaction);

    DistributedTransaction transaction = transactionManager.begin(txId);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> transactionManager.join(txId))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_ShouldReturnBegunActiveTransaction() throws TransactionException {
    // Arrange
    String txId = "txId1";

    DistributedTransaction transaction = mock(DistributedTransaction.class);
    when(transaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(transaction);

    DistributedTransaction expected = transactionManager.begin(txId);

    // Act
    DistributedTransaction actual = transactionManager.resume(txId);

    // Assert
    assertThat(actual)
        .isInstanceOf(
            ActiveTransactionManagedDistributedTransactionManager.ActiveTransaction.class);

    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isEqualTo(((DecoratedDistributedTransaction) expected).getOriginalTransaction());
  }

  @Test
  public void resume_ActiveTransactionAlreadyRolledBack_ShouldThrowTransactionNotFoundException()
      throws TransactionException {
    String txId = "txId1";

    DistributedTransaction transaction = mock(DistributedTransaction.class);
    when(transaction.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId)).thenReturn(transaction);

    DistributedTransaction activeTransaction = transactionManager.begin(txId);
    activeTransaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> transactionManager.resume(txId))
        .isInstanceOf(TransactionNotFoundException.class);
  }
}
