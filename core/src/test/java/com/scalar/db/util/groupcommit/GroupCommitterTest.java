package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupCommitterTest {
  private static final int TIMEOUT_CHECK_INTERVAL_MILLIS = 10;

  @Mock Emittable<String, Integer> emitter;
  CurrentTime currentTime;

  private class TestableGroupCommitter
      extends GroupCommitter<String, String, String, String, Integer> {
    TestableGroupCommitter(GroupCommitConfig config) {
      super("test", config, new TestableKeyManipulator());
    }

    @Override
    CurrentTime createCurrentTime() {
      return currentTime;
    }
  }

  @BeforeEach
  void setUp() {
    currentTime = spy(new CurrentTime());
  }

  @Test
  void reserve_GivenArbitraryChildKey_ShouldReturnFullKeyProperly() throws Exception {
    // Arrange

    int groupCloseTimeoutMillis = 100;
    int delayedSlotMoveTimeoutMillis = 400;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                2,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(emitter);

      // Act
      // Assert

      assertThat(groupCommitter.reserve("child-key-1")).isEqualTo("0000:child-key-1");
      assertThat(groupCommitter.reserve("child-key-2")).isEqualTo("0000:child-key-2");
      assertThat(groupCommitter.reserve("child-key-3")).isEqualTo("0001:child-key-3");

      verify(emitter, never()).execute(any(), any());
    }
  }

  @Test
  void ready_GivenTwoValuesForTwoSlots_ShouldEmitThem() throws Exception {
    // Arrange

    int groupCloseTimeoutMillis = 100;
    int delayedSlotMoveTimeoutMillis = 400;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                2,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(emitter);
      ExecutorService executorService = Executors.newCachedThreadPool();

      String fullKey1 = groupCommitter.reserve("child-key-1");
      String fullKey2 = groupCommitter.reserve("child-key-2");
      groupCommitter.reserve("child-key-3");

      // Act

      // Move current time forward to let GroupCloseWorker process.
      doReturn(startTimeMillis + groupCloseTimeoutMillis).when(currentTime).currentTimeMillis();
      List<Future<?>> futures = new ArrayList<>();
      futures.add(executorService.submit(() -> groupCommitter.ready(fullKey1, 11)));
      futures.add(executorService.submit(() -> groupCommitter.ready(fullKey2, 22)));
      executorService.shutdown();
      for (Future<?> future : futures) {
        future.get();
      }

      // Assert
      verify(emitter).execute("0000", Arrays.asList(11, 22));
      verify(emitter, never()).execute(eq("0001"), any());
    }
  }

  @Test
  void ready_GivenOnlyOneValueForTwoSlots_ShouldJustWait() throws Exception {
    // Arrange

    int groupCloseTimeoutMillis = 100;
    // Enough long to wait `ready()`.
    int delayedSlotMoveTimeoutMillis = 2000;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                2,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(emitter);
      ExecutorService executorService = Executors.newCachedThreadPool();

      // Reserve 3 slots.
      String fullKey1 = groupCommitter.reserve("child-key-1");
      groupCommitter.reserve("child-key-2");
      groupCommitter.reserve("child-key-3");

      // Act

      // Move current time forward to let GroupCloseWorker process.
      doReturn(startTimeMillis + 1000).when(currentTime).currentTimeMillis();
      // Mark the first slot as ready by putting a value.
      Future<?> future = executorService.submit(() -> groupCommitter.ready(fullKey1, 11));
      executorService.shutdown();

      // Assert
      assertThrows(TimeoutException.class, () -> future.get(1000, TimeUnit.MILLISECONDS));
      verify(emitter, never()).execute(any(), any());
    }
  }

  @Test
  void remove_GivenOpenGroup_ShouldRemoveIt() throws Exception {
    // Arrange

    int groupCloseTimeoutMillis = 100;
    int delayedSlotMoveTimeoutMillis = 400;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                3,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(emitter);

      String fullKey1 = groupCommitter.reserve("child-key-1");
      String fullKey2 = groupCommitter.reserve("child-key-2");

      // Act
      // Assert

      groupCommitter.remove(fullKey1);
      groupCommitter.remove(fullKey2);
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      assertThrows(GroupCommitException.class, () -> groupCommitter.ready(fullKey1, 42));
      assertThrows(GroupCommitException.class, () -> groupCommitter.ready(fullKey2, 42));
      verify(emitter, never()).execute(any(), any());
    }
  }

  @Test
  void remove_GivenClosedGroup_ShouldRemoveIt() throws Exception {
    // Arrange

    int groupCloseTimeoutMillis = 100;
    int delayedSlotMoveTimeoutMillis = 400;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                2,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(emitter);

      String fullKey1 = groupCommitter.reserve("child-key-1");
      String fullKey2 = groupCommitter.reserve("child-key-2");

      // Act
      // Assert

      groupCommitter.remove(fullKey1);
      groupCommitter.remove(fullKey2);
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      assertThrows(GroupCommitException.class, () -> groupCommitter.ready(fullKey1, 42));
      assertThrows(GroupCommitException.class, () -> groupCommitter.ready(fullKey2, 42));
      verify(emitter, never()).execute(any(), any());
    }
  }

  @Test
  void remove_GivenReadyGroup_ShouldFail() throws Exception {
    // Arrange
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Emittable<String, Integer> testableEmitter =
        spy(
            // This should be an anonymous class since `spy()` can't handle a lambda.
            new Emittable<String, Integer>() {
              @Override
              public void execute(String key, List<Integer> values) throws Exception {
                countDownLatch.await();
              }
            });

    int groupCloseTimeoutMillis = 100;
    int delayedSlotMoveTimeoutMillis = 400;

    // Pin the current time to prevent GroupCloseWorker from closing the first group before
    // reserving 2 slots.
    long startTimeMillis = System.currentTimeMillis();
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();

    try (TestableGroupCommitter groupCommitter =
        new TestableGroupCommitter(
            new GroupCommitConfig(
                2,
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                TIMEOUT_CHECK_INTERVAL_MILLIS))) {
      groupCommitter.setEmitter(testableEmitter);
      ExecutorService executorService = Executors.newCachedThreadPool();

      String fullKey1 = groupCommitter.reserve("child-key-1");
      String fullKey2 = groupCommitter.reserve("child-key-2");
      List<Future<?>> futures = new ArrayList<>();
      futures.add(executorService.submit(() -> groupCommitter.ready(fullKey1, 11)));
      futures.add(executorService.submit(() -> groupCommitter.ready(fullKey2, 22)));
      executorService.shutdown();
      // Wait for a while so that the threads can wait.
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

      // Act
      // Assert

      groupCommitter.remove(fullKey1);
      groupCommitter.remove(fullKey2);
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

      countDownLatch.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
      verify(testableEmitter).execute("0000", Arrays.asList(11, 22));
    }
  }
}
