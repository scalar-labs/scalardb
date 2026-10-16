package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ActiveExpiringMap;
import java.util.ArrayList;
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

public class ActiveTransactionManagedTwoPhaseCommitTransactionManagerTest {

  @Mock private TwoPhaseCommitTransactionManager wrappedTransactionManager;

  private ActiveTransactionManagedTwoPhaseCommitTransactionManager transactionManager;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(wrappedTransactionManager, -1);
  }

  @AfterEach
  public void tearDown() {
    executor.shutdownNow();
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
        .isInstanceOf(ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner.class);
    List<Result> results = new ArrayList<>();
    scanner.iterator().forEachRemaining(results::add);
    assertThat(results).containsExactly(result1);
    assertThat(scanner.all()).containsExactly(result2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getScanner_ShouldRegisterScannerAndRefreshItOnEachRead() throws Exception {
    // Arrange
    ActiveExpiringMap<
            String, ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    prepareUnderlyingScanner();

    // Act
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveExpiringMap<
            String, ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveExpiringMap<
            String, ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner>
        activeScanners = mock(ActiveExpiringMap.class);
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, mock(ActiveTransactionRegistry.class), activeScanners);
    TransactionManagerCrudOperable.Scanner underlyingScanner = prepareUnderlyingScanner();
    CrudException failure = new CrudException("close failed", null);
    doThrow(failure).when(underlyingScanner).close();
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner scanner =
        (ActiveTransactionManagedTwoPhaseCommitTransactionManager.ActiveScanner)
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
    ActiveTransactionManagedTwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
            wrappedTransactionManager, 200);
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
