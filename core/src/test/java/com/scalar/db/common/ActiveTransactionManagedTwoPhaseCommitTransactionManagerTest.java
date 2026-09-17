package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Collections;
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
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, -1, -1);
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
  @SuppressWarnings({"unchecked", "deprecation"})
  public void crudOperations_OnHeldTransaction_ShouldRefreshIdleTimerViaRegistryTouch()
      throws Exception {
    // Arrange — inject a mock registry so we can observe the idle-timer refresh directly.
    ActiveTransactionRegistry<TwoPhaseCommitTransaction> registry =
        mock(ActiveTransactionRegistry.class);
    when(registry.add(anyString(), any())).thenReturn(true);
    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1");
    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, registry);

    // Act: every CRUD op on a held transaction reference must refresh the idle timer, so a
    // long-running begin-and-hold transaction is neither idle-reaped nor LRU-evicted mid-work.
    TwoPhaseCommitTransaction transaction = manager.begin();
    transaction.get(mock(Get.class));
    transaction.scan(mock(Scan.class));
    transaction.put(mock(Put.class));
    transaction.put(Collections.singletonList(mock(Put.class)));
    transaction.delete(mock(Delete.class));
    transaction.delete(Collections.singletonList(mock(Delete.class)));
    transaction.insert(mock(Insert.class));
    transaction.upsert(mock(Upsert.class));
    transaction.update(mock(Update.class));
    transaction.mutate(Collections.singletonList(mock(Put.class)));
    transaction.batch(Collections.singletonList(mock(Get.class)));

    // Assert — one refresh per CRUD operation (begin() registers but does not touch).
    verify(registry, times(11)).touch("txId1");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void prepareAndValidate_OnHeldTransaction_ShouldRefreshIdleTimerButTerminalOpsShouldNot()
      throws Exception {
    // Arrange — inject a mock registry so we can observe the idle-timer refresh directly.
    ActiveTransactionRegistry<TwoPhaseCommitTransaction> registry =
        mock(ActiveTransactionRegistry.class);
    when(registry.add(anyString(), any())).thenReturn(true);
    TwoPhaseCommitTransaction wrappedTransaction = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1");
    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, registry);

    // Act: prepare()/validate() are non-terminal operations on a held transaction, so they must
    // refresh the idle timer; commit() is terminal and removes the entry, so it must not touch.
    TwoPhaseCommitTransaction transaction = manager.begin();
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert — one refresh each for prepare()/validate() only; commit() removes instead of
    // touching.
    verify(registry, times(2)).touch("txId1");
    verify(registry).remove("txId1");
  }

  @Test
  public void begin_TransactionAlreadyExists_ShouldRollbackAndThrowTransactionException()
      throws TransactionException {
    // Arrange
    String txId = "txId1";

    TwoPhaseCommitTransaction wrappedTransaction1 = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction1.getId()).thenReturn(txId);

    TwoPhaseCommitTransaction wrappedTransaction2 = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction2.getId()).thenReturn(txId);

    when(wrappedTransactionManager.begin(txId))
        .thenReturn(wrappedTransaction1)
        .thenReturn(wrappedTransaction2);

    transactionManager.begin(txId);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.begin(txId))
        .isInstanceOf(TransactionException.class);
    verify(wrappedTransaction2).rollback();
  }

  @Test
  public void begin_TransactionAlreadyExistsAndRollbackFails_ShouldThrowTransactionException()
      throws TransactionException {
    // Arrange
    String txId = "txId1";

    TwoPhaseCommitTransaction wrappedTransaction1 = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction1.getId()).thenReturn(txId);

    TwoPhaseCommitTransaction wrappedTransaction2 = mock(TwoPhaseCommitTransaction.class);
    when(wrappedTransaction2.getId()).thenReturn(txId);
    doThrow(new RollbackException("rollback failed", txId)).when(wrappedTransaction2).rollback();

    when(wrappedTransactionManager.begin(txId))
        .thenReturn(wrappedTransaction1)
        .thenReturn(wrappedTransaction2);

    transactionManager.begin(txId);

    // Act Assert
    assertThatThrownBy(() -> transactionManager.begin(txId))
        .isInstanceOf(TransactionException.class);
    verify(wrappedTransaction2).rollback();
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
