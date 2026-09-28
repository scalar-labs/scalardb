package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class AsyncExecutorTest {

  private static final String TX_ID = "id";
  private static final long TIMEOUT_SECONDS = 10;

  @Mock private ConsensusCommitConfig config;

  private AsyncExecutor asyncExecutor;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @AfterEach
  void tearDown() {
    if (asyncExecutor != null) {
      asyncExecutor.close();
    }
  }

  @Test
  void commitRecords_AsyncCommitNotEnabled_ShouldRunTaskOnCallingThread() {
    // Arrange
    when(config.isAsyncCommitEnabled()).thenReturn(false);
    asyncExecutor = new AsyncExecutor(config);
    AtomicReference<Thread> taskThread = new AtomicReference<>();

    // Act
    asyncExecutor.commitRecords(() -> taskThread.set(Thread.currentThread()), TX_ID);

    // Assert
    assertThat(taskThread.get()).isEqualTo(Thread.currentThread());
  }

  @Test
  void commitRecords_AsyncCommitEnabled_ShouldRunTaskOnAnotherThreadWithoutWaitingForIt()
      throws Exception {
    // Arrange
    when(config.isAsyncCommitEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(2);
    asyncExecutor = new AsyncExecutor(config);
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskReleased = new CountDownLatch(1);
    AtomicReference<Thread> taskThread = new AtomicReference<>();

    // Act
    asyncExecutor.commitRecords(
        () -> {
          taskThread.set(Thread.currentThread());
          taskStarted.countDown();
          // Bounded so that an implementation that runs the task on the calling thread fails
          // instead of hanging
          awaitUninterruptibly(taskReleased);
        },
        TX_ID);

    // Assert

    // This returns while the task is still running, on another thread
    assertThat(taskStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    assertThat(taskThread.get()).isNotEqualTo(Thread.currentThread());

    taskReleased.countDown();
  }

  @Test
  void commitRecords_AsyncCommitEnabled_WhenTaskThrows_ShouldNotPropagateException() {
    // Arrange
    when(config.isAsyncCommitEnabled()).thenReturn(true);
    // Runs the task on the calling thread, so that an exception the executor fails to catch reaches
    // this test instead of being lost on another thread
    asyncExecutor = new AsyncExecutor(config, MoreExecutors.newDirectExecutorService());

    // Act Assert
    assertThatCode(
            () ->
                asyncExecutor.commitRecords(
                    () -> {
                      throw new RuntimeException("Failed");
                    },
                    TX_ID))
        .doesNotThrowAnyException();
  }

  @Test
  void rollbackRecords_AsyncRollbackNotEnabled_ShouldRunTaskOnCallingThread() {
    // Arrange
    when(config.isAsyncRollbackEnabled()).thenReturn(false);
    asyncExecutor = new AsyncExecutor(config);
    AtomicReference<Thread> taskThread = new AtomicReference<>();

    // Act
    asyncExecutor.rollbackRecords(() -> taskThread.set(Thread.currentThread()), TX_ID);

    // Assert
    assertThat(taskThread.get()).isEqualTo(Thread.currentThread());
  }

  @Test
  void rollbackRecords_AsyncRollbackNotEnabled_WhenTaskThrows_ShouldNotPropagateException() {
    // Arrange
    when(config.isAsyncRollbackEnabled()).thenReturn(false);
    asyncExecutor = new AsyncExecutor(config);

    // Act Assert
    assertThatCode(
            () ->
                asyncExecutor.rollbackRecords(
                    () -> {
                      throw new RuntimeException("Failed");
                    },
                    TX_ID))
        .doesNotThrowAnyException();
  }

  @Test
  void rollbackRecords_AsyncRollbackEnabled_ShouldRunTaskOnAnotherThreadWithoutWaitingForIt()
      throws Exception {
    // Arrange
    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(2);
    asyncExecutor = new AsyncExecutor(config);
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskReleased = new CountDownLatch(1);
    AtomicReference<Thread> taskThread = new AtomicReference<>();

    // Act
    asyncExecutor.rollbackRecords(
        () -> {
          taskThread.set(Thread.currentThread());
          taskStarted.countDown();
          // Bounded so that an implementation that runs the task on the calling thread fails
          // instead of hanging
          awaitUninterruptibly(taskReleased);
        },
        TX_ID);

    // Assert

    // This returns while the task is still running, on another thread
    assertThat(taskStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    assertThat(taskThread.get()).isNotEqualTo(Thread.currentThread());

    taskReleased.countDown();
  }

  @Test
  void rollbackRecords_AsyncRollbackEnabled_WhenTaskThrows_ShouldNotPropagateException() {
    // Arrange
    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    // Runs the task on the calling thread, so that an exception the executor fails to catch reaches
    // this test instead of being lost on another thread
    asyncExecutor = new AsyncExecutor(config, MoreExecutors.newDirectExecutorService());

    // Act Assert
    assertThatCode(
            () ->
                asyncExecutor.rollbackRecords(
                    () -> {
                      throw new RuntimeException("Failed");
                    },
                    TX_ID))
        .doesNotThrowAnyException();
  }

  @Test
  void rollbackRecords_AsyncRollbackEnabled_WhenNoThreadAvailable_ShouldRunTaskOnCallingThread()
      throws Exception {
    // Arrange

    // A single thread, which the first task occupies
    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(1);
    asyncExecutor = new AsyncExecutor(config);
    CountDownLatch firstTaskStarted = new CountDownLatch(1);
    CountDownLatch firstTaskReleased = new CountDownLatch(1);
    asyncExecutor.rollbackRecords(
        () -> {
          firstTaskStarted.countDown();
          awaitUninterruptibly(firstTaskReleased);
        },
        TX_ID);
    assertThat(firstTaskStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    AtomicReference<Thread> secondTaskThread = new AtomicReference<>();

    // Act
    asyncExecutor.rollbackRecords(() -> secondTaskThread.set(Thread.currentThread()), TX_ID);

    // Assert
    assertThat(secondTaskThread.get()).isEqualTo(Thread.currentThread());

    firstTaskReleased.countDown();
  }

  @Test
  void rollbackRecords_AsyncRollbackEnabled_AfterClosed_ShouldRunTaskOnCallingThread() {
    // Arrange
    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(2);
    asyncExecutor = new AsyncExecutor(config);
    asyncExecutor.close();
    AtomicReference<Thread> taskThread = new AtomicReference<>();

    // Act
    asyncExecutor.rollbackRecords(() -> taskThread.set(Thread.currentThread()), TX_ID);

    // Assert

    // The task is run instead of being dropped, so the rollback is not lost
    assertThat(taskThread.get()).isEqualTo(Thread.currentThread());
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    try {
      latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
