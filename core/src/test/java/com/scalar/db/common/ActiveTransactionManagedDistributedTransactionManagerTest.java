package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ActiveExpiringMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ActiveTransactionManagedDistributedTransactionManagerTest {

  @Mock private DistributedTransactionManager wrappedTransactionManager;

  private ActiveTransactionManagedDistributedTransactionManager transactionManager;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, -1, -1);
  }

  @AfterEach
  public void tearDown() {
    executor.shutdownNow();
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

  @Test
  public void getScanner_ShouldReturnActiveScannerThatReadsUnderlyingScanner() throws Exception {
    // Arrange
    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    when(underlyingScanner.one()).thenReturn(Optional.of(result1)).thenReturn(Optional.empty());
    when(underlyingScanner.all()).thenReturn(Collections.singletonList(result2));

    // Act
    TransactionManagerCrudOperable.Scanner scanner = transactionManager.getScanner(scan());

    // Assert
    assertThat(scanner)
        .isInstanceOf(ActiveTransactionManagedDistributedTransactionManager.ActiveScanner.class);
    List<Result> results = new ArrayList<>();
    scanner.iterator().forEachRemaining(results::add);
    assertThat(results).containsExactly(result1);
    assertThat(scanner.all()).containsExactly(result2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getScanner_ShouldRegisterScannerAndRefreshItOnEachRead() throws Exception {
    // Arrange
    ActiveExpiringMap<String, ActiveTransactionManagedDistributedTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    prepareUnderlyingScanner();

    // Act
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            manager.getScanner(scan());
    scanner.one();
    scanner.all();

    // Assert
    ArgumentCaptor<String> id = ArgumentCaptor.forClass(String.class);
    verify(activeScanners).put(id.capture(), same(scanner));
    verify(activeScanners, times(2)).updateExpirationTime(id.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void close_ShouldCloseUnderlyingScannerAndUnregisterIt() throws Exception {
    // Arrange
    ActiveExpiringMap<String, ActiveTransactionManagedDistributedTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            manager.getScanner(scan());

    // Act
    scanner.close();

    // Assert
    verify(underlyingScanner).close();
    ArgumentCaptor<String> id = ArgumentCaptor.forClass(String.class);
    verify(activeScanners).put(id.capture(), same(scanner));
    verify(activeScanners).remove(id.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void close_WhenUnderlyingCloseFails_ShouldUnregisterScannerAndThrow() throws Exception {
    // Arrange
    ActiveExpiringMap<String, ActiveTransactionManagedDistributedTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    CrudException failure = new CrudException("close failed", null);
    doThrow(failure).when(underlyingScanner).close();
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            manager.getScanner(scan());

    // Act Assert
    assertThatThrownBy(scanner::close).isSameAs(failure);
    ArgumentCaptor<String> id = ArgumentCaptor.forClass(String.class);
    verify(activeScanners).put(id.capture(), same(scanner));
    verify(activeScanners).remove(id.getValue());
  }

  @Test
  public void dispose_ShouldCloseUnderlyingScanner() throws Exception {
    // Arrange
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            transactionManager.getScanner(scan());

    // Act
    scanner.dispose();

    // Assert
    verify(underlyingScanner).close();
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  public void dispose_WhenUnderlyingCloseFails_ShouldNotThrow(Exception failure) throws Exception {
    // Arrange
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    doThrow(failure).when(underlyingScanner).close();
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            transactionManager.getScanner(scan());

    // Act
    scanner.dispose();

    // Assert
    verify(underlyingScanner).close();
  }

  @Test
  public void dispose_WhileReading_ShouldCloseUnderlyingScannerAfterReadReturns() throws Exception {
    // Arrange
    CountDownLatch readStarted = new CountDownLatch(1);
    CountDownLatch finishRead = new CountDownLatch(1);
    Result result = mock(Result.class);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    doAnswer(
            invocation -> {
              readStarted.countDown();
              if (!finishRead.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("The read was not released");
              }
              return Optional.of(result);
            })
        .when(underlyingScanner)
        .one();
    ActiveTransactionManagedDistributedTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedDistributedTransactionManager.ActiveScanner)
            transactionManager.getScanner(scan());
    Future<Optional<Result>> read = executor.submit(scanner::one);
    assertThat(readStarted.await(10, TimeUnit.SECONDS)).isTrue();

    // Act
    AtomicReference<Thread> expirationThread = new AtomicReference<>();
    Future<?> expiration =
        executor.submit(
            () -> {
              expirationThread.set(Thread.currentThread());
              scanner.dispose();
            });
    waitUntilBlocked(expirationThread);
    verify(underlyingScanner, never()).close();
    finishRead.countDown();

    // Assert
    assertThat(read.get(10, TimeUnit.SECONDS)).hasValue(result);
    expiration.get(10, TimeUnit.SECONDS);
    InOrder inOrder = inOrder(underlyingScanner);
    inOrder.verify(underlyingScanner).one();
    inOrder.verify(underlyingScanner).close();
  }

  @Test
  public void getScanner_ScannerNotUsedForExpirationTime_ShouldCloseUnderlyingScanner()
      throws Exception {
    // Arrange
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(
            wrappedTransactionManager, 200, -1);
    CountDownLatch closed = new CountDownLatch(1);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(underlyingScanner)
        .close();

    // Act
    manager.getScanner(scan());

    // Assert
    assertThat(closed.await(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void getScanner_MoreScannersThanMaximum_ShouldCloseEvictedScanner() throws Exception {
    // Arrange
    ActiveTransactionManagedDistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(wrappedTransactionManager, -1, 1);
    CountDownLatch closed = new CountDownLatch(1);
    TransactionManagerCrudOperable.Scanner underlyingScanner1 =
        mock(TransactionManagerCrudOperable.Scanner.class);
    TransactionManagerCrudOperable.Scanner underlyingScanner2 =
        mock(TransactionManagerCrudOperable.Scanner.class);
    for (TransactionManagerCrudOperable.Scanner underlyingScanner :
        Arrays.asList(underlyingScanner1, underlyingScanner2)) {
      doAnswer(
              invocation -> {
                closed.countDown();
                return null;
              })
          .when(underlyingScanner)
          .close();
    }
    when(wrappedTransactionManager.getScanner(scan()))
        .thenReturn(underlyingScanner1)
        .thenReturn(underlyingScanner2);

    // Act
    manager.getScanner(scan());
    TransactionManagerCrudOperable.Scanner scanner = manager.getScanner(scan());

    // Assert
    // Without an expiration time, the eviction can stay pending until a scanner is accessed again
    for (int i = 0; i < 100 && !closed.await(100, TimeUnit.MILLISECONDS); i++) {
      scanner.one();
    }
    assertThat(closed.getCount()).isZero();
  }

  private static Stream<Exception> closeFailures() {
    return Stream.of(
        new CrudException("close failed", null),
        new UnknownTransactionStatusException("close failed", null),
        new RuntimeException("close failed"));
  }

  private TransactionManagerCrudOperable.Scanner prepareUnderlyingScanner() throws CrudException {
    TransactionManagerCrudOperable.Scanner underlyingScanner =
        mock(TransactionManagerCrudOperable.Scanner.class);
    when(wrappedTransactionManager.getScanner(scan())).thenReturn(underlyingScanner);
    return underlyingScanner;
  }

  private static Scan scan() {
    return Scan.newBuilder().namespace("ns").table("tbl").all().build();
  }

  private static void waitUntilBlocked(AtomicReference<Thread> thread) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10000;
    while (thread.get() == null || thread.get().getState() != Thread.State.BLOCKED) {
      if (System.currentTimeMillis() >= deadline) {
        throw new AssertionError("The thread did not block");
      }
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }
}
