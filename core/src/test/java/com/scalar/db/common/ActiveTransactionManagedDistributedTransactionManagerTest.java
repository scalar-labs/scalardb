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
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, -1, -1);
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
  public void getScanner_ReturnedScanner_ShouldDriveUnderlyingOneWhenIterated() throws Exception {
    // Arrange
    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1");
    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    TransactionCrudOperable.Scanner rawScanner = mock(TransactionCrudOperable.Scanner.class);
    Result result = mock(Result.class);
    when(rawScanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    Scan scan = Scan.newBuilder().namespace("ns").table("tbl").all().build();
    when(wrappedTransaction.getScanner(scan)).thenReturn(rawScanner);

    // Act
    DistributedTransaction transaction = transactionManager.begin();
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    List<Result> results = new ArrayList<>();
    scanner.iterator().forEachRemaining(results::add);

    // Assert — iterating drives the underlying scanner's one() per element, so iteration flows
    // through the idle-timer-refreshing (and synchronizing) wrapper rather than bypassing it via
    // the
    // raw scanner's own iterator.
    assertThat(results).containsExactly(result);
    verify(rawScanner, times(2)).one();
  }

  @Test
  @SuppressWarnings({"unchecked", "deprecation"})
  public void crudOperations_OnHeldTransaction_ShouldRefreshIdleTimerViaRegistryTouch()
      throws Exception {
    // Arrange — inject a mock registry so we can observe the idle-timer refresh directly.
    ActiveTransactionRegistry<DistributedTransaction> registry =
        mock(ActiveTransactionRegistry.class);
    when(registry.add(anyString(), any())).thenReturn(true);
    DistributedTransaction wrappedTransaction = mock(DistributedTransaction.class);
    when(wrappedTransaction.getId()).thenReturn("txId1");
    when(wrappedTransactionManager.begin()).thenReturn(wrappedTransaction);
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, registry);

    // Act: every CRUD op on a held transaction reference must refresh the idle timer, so a
    // long-running begin-and-hold transaction is neither idle-reaped nor LRU-evicted mid-work.
    DistributedTransaction transaction = manager.begin();
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
  public void begin_TransactionAlreadyExists_ShouldRollbackAndThrowTransactionException()
      throws TransactionException {
    // Arrange
    String txId = "txId1";

    DistributedTransaction wrappedTransaction1 = mock(DistributedTransaction.class);
    when(wrappedTransaction1.getId()).thenReturn(txId);

    DistributedTransaction wrappedTransaction2 = mock(DistributedTransaction.class);
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

    DistributedTransaction wrappedTransaction1 = mock(DistributedTransaction.class);
    when(wrappedTransaction1.getId()).thenReturn(txId);

    DistributedTransaction wrappedTransaction2 = mock(DistributedTransaction.class);
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
