package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DelayedGroupTest {
  private Emittable<String, Integer> emitter;
  private TestableKeyManipulator keyManipulator;

  @BeforeEach
  void setUp() {
    emitter = (s, values) -> {};
    // This generates parent keys which start with "0000" and increment by one for each subsequent
    // key ("0001", "0002"...).
    keyManipulator = new TestableKeyManipulator();
  }

  @Test
  void fullKey_GivenFullKeyViaConstructor_ShouldReturnProperly() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", emitter, keyManipulator);

    // Act
    // Assert
    assertThat(group.fullKey()).isEqualTo("0000:full-key");
  }

  @Test
  void reserveNewSlot_GivenArbitrarySlot_ShouldStoreIt() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(config, emitter, keyManipulator);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", emitter, keyManipulator);

    assertThat(group.size()).isNull();
    assertThat(group.isSizeFixed()).isFalse();

    // Act
    group.reserveNewSlot(slot);

    // Assert
    assertThat(group.size()).isEqualTo(1);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();
  }

  @Test
  void putValueToSlotAndWait_WithSuccessfulEmitTask_ShouldExecuteTaskProperly()
      throws InterruptedException, ExecutionException {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(config, waitableEmitter, keyManipulator);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", waitableEmitter, keyManipulator);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot);

    // Act
    // Assert

    // Put value to the slots.
    // Using different threads since calling putValueToSlotAndWait() will block the client thread
    // until emitting.
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot.key(), 42);
              return null;
            }));
    executorService.shutdown();
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    // The status is READY not DONE.
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isFalse();
    assertThat(emitted.get()).isFalse();

    // Resume the blocked emit task to move forward to DONE.
    wait.countDown();
    for (Future<Void> future : futures) {
      future.get();
    }
    group.updateStatus();
    assertThat(group.isDone()).isTrue();
    assertThat(emitted.get()).isTrue();
  }

  @Test
  void putValueToSlotAndWait_WithFailingEmitTask_ShouldFail() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> failingEmitter =
        (s, values) -> {
          wait.await();
          throw new RuntimeException("Something is wrong");
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(config, failingEmitter, keyManipulator);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", failingEmitter, keyManipulator);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot);

    // Act
    // Assert

    // Put value to the slots.
    // Using different threads since calling putValueToSlotAndWait() will block the client thread
    // until emitting.
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot.key(), 42);
              return null;
            }));
    executorService.shutdown();
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // The status is READY not DONE.
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isFalse();

    // Resume the blocked emit task to move forward to DONE.
    wait.countDown();
    for (Future<Void> future : futures) {
      Throwable cause = assertThrows(ExecutionException.class, future::get).getCause();
      assertThat(cause).isInstanceOf(GroupCommitException.class);
    }
    group.updateStatus();
    assertThat(group.isDone()).isTrue();
  }

  @Test
  void removeSlot_GivenNoReadySlot_ShouldRemoveSlotAndGetDone() {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(config, emitter, keyManipulator);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", emitter, keyManipulator);

    group.reserveNewSlot(slot);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();

    // Act
    // Assert

    assertThat(group.removeSlot("child-key")).isTrue();
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isTrue();
    assertThat(group.removeSlot("child-key")).isFalse();
  }

  @Test
  void removeSlot_GivenReadySlot_ShouldDoNothing() throws InterruptedException, ExecutionException {
    // Arrange
    GroupCommitConfig config = new GroupCommitConfig(2, 100, 1000, 60, 20);
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(config, waitableEmitter, keyManipulator);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>(config, "0000:full-key", waitableEmitter, keyManipulator);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();

    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot.key(), 42);
              return null;
            }));
    executorService.shutdown();

    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    // At the moment,
    // - slot is ready, but not done since it's blocked

    // Act
    // Assert

    assertThat(group.removeSlot("child-key")).isFalse();
    assertThat(group.isReady()).isTrue();

    // This wait is needed to prevent the DelayedGroup from immediately getting done.
    wait.countDown();

    assertThat(futures.size()).isEqualTo(1);
    futures.get(0).get();
    assertThat(group.isDone()).isTrue();
    assertThat(emitted.get()).isTrue();
  }
}
