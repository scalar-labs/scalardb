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

class NormalGroupTest {
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
  void parentKey_WithKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);

    // Act
    // Assert
    assertThat(group.parentKey()).isEqualTo("0000");
  }

  @Test
  void fullKey_WithKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);

    // Act
    // Assert
    assertThat(group.fullKey("child-key")).isEqualTo("0000:child-key");
  }

  @Test
  void reserveNewSlot_GivenArbitrarySlot_ShouldStoreIt() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);

    assertThat(group.size()).isNull();
    assertThat(group.isSizeFixed()).isFalse();

    // Act
    // Assert
    group.reserveNewSlot(slot1);
    assertThat(group.size()).isNull();
    assertThat(group.isSizeFixed()).isFalse();

    group.reserveNewSlot(slot2);
    assertThat(group.size()).isEqualTo(2);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();
  }

  @Test
  void putValueToSlotAndWait_WithSuccessfulEmitTaskWithSingleSlot_ShouldExecuteTaskProperly()
      throws InterruptedException, ExecutionException {
    // Arrange
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot1);
    group.fixSize();

    // Act
    // Assert

    // Put value to the slots.
    // Using different threads since calling putValueToSlotAndWait() will block the client thread
    // until emitting.
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot1.key(), 42);
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
  void putValueToSlotAndWait_WithSuccessfulEmitTask_ShouldExecuteTaskProperly()
      throws InterruptedException, ExecutionException {
    // Arrange
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);

    // Act
    // Assert

    // Put value to the slots.
    // Using different threads since calling putValueToSlotAndWait() will block the client thread
    // until emitting.
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot1.key(), 42);
              return null;
            }));
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot2.key(), 43);
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
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> failingEmitter =
        (s, values) -> {
          wait.await();
          throw new RuntimeException("Something is wrong");
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(failingEmitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", failingEmitter, keyManipulator);

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
  void removeNotReadySlots_WhenBothReadyAndNonReadySlotsExist_ShouldExecuteEmitTaskProperly()
      throws InterruptedException, ExecutionException {
    // Arrange
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2 + 1);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);
    Slot<String, String, String, String, Integer> slot3 = new Slot<>("child-key-3", group);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);
    group.reserveNewSlot(slot3);

    // Put value to the first 2 slots to treat the last slot is delayed.
    // Using different threads since calling putValueToSlotAndWait() will block the client thread
    // until emitting.
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot1.key(), 42);
              return null;
            }));
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot2.key(), 43);
              return null;
            }));
    executorService.shutdown();
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // The status isn't READY yet since slot3 isn't ready.
    assertThat(group.isReady()).isFalse();

    // Act
    // Assert

    // Remove the not-ready slot (slot3).
    List<Slot<String, String, String, String, Integer>> notReadySlots = group.removeNotReadySlots();
    assertThat(notReadySlots).isNotNull().size().isEqualTo(1);
    assertThat(notReadySlots.get(0).isReady()).isFalse();
    assertThat(notReadySlots.get(0).isDone()).isFalse();
    assertThat(notReadySlots.get(0)).isEqualTo(slot3);

    // All the slots in the group is now ready and the group status should be READY.
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isFalse();

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
  void removeNotReadySlots_WhenAllSlotsAreNotReady_ShouldRetainSlots() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);

    // Act
    // Assert

    // The method returns null since all the slots are not ready.
    assertThat(group.removeNotReadySlots()).isNull();
  }

  @Test
  void removeSlot_GivenNotReadySlot_ShouldRemoveSlotAndGetDone() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();

    // Act
    // Assert

    assertThat(group.removeSlot("child-key-1")).isTrue();
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();
    assertThat(group.removeSlot("child-key-1")).isFalse();
    assertThat(group.removeSlot("child-key-2")).isTrue();
    assertThat(group.isDone()).isTrue();
  }

  @Test
  void removeSlot_GivenReadySlot_ShouldDoNothing() throws ExecutionException, InterruptedException {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();

    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot1.key(), 42);
              return null;
            }));
    executorService.shutdown();
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // At the moment,
    // - slot1 is ready
    // - slot2 is not ready

    // Act
    // Assert

    assertThat(group.removeSlot("child-key-1")).isFalse();
    assertThat(group.isSizeFixed()).isTrue();
    assertThat(group.isReady()).isFalse();
    assertThat(group.removeSlot("child-key-2")).isTrue();
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    assertThat(group.isDone()).isTrue();
    assertThat(group.size()).isEqualTo(1);

    assertThat(futures.size()).isEqualTo(1);
    futures.get(0).get();
  }

  @Test
  void groupSizeFixTimeoutMillisAt_GivenArbitraryTimeoutValue_ShouldReturnProperly() {
    // Arrange
    long minOfCurrentTimeMillis = System.currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    long maxOfCurrentTimeMillis = System.currentTimeMillis();

    // Act
    // Assert
    assertThat(group.groupSizeFixTimeoutMillisAt())
        .isGreaterThanOrEqualTo(minOfCurrentTimeMillis + 100)
        .isLessThanOrEqualTo(maxOfCurrentTimeMillis + 100);
  }

  @Test
  void delayedSlotMoveTimeoutMillisAt_GivenArbitraryTimeoutValue_ShouldReturnProperly() {
    // Arrange
    long minOfCurrentTimeMillis = System.currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);
    long maxOfCurrentTimeMillis = System.currentTimeMillis();

    // Act
    // Assert
    assertThat(group.delayedSlotMoveTimeoutMillisAt())
        .isGreaterThanOrEqualTo(minOfCurrentTimeMillis + 1000)
        .isLessThanOrEqualTo(maxOfCurrentTimeMillis + 1000);
  }

  @Test
  void updateDelayedSlotMoveTimeoutMillisAt_GivenArbitraryTimeoutValue_ShouldUpdateProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2);

    // Act
    long minOfCurrentTimeMillis = System.currentTimeMillis();
    group.updateDelayedSlotMoveTimeoutMillisAt();
    long maxOfCurrentTimeMillis = System.currentTimeMillis();

    // Assert
    assertThat(group.delayedSlotMoveTimeoutMillisAt())
        .isGreaterThanOrEqualTo(minOfCurrentTimeMillis + 1000)
        .isLessThanOrEqualTo(maxOfCurrentTimeMillis + 1000);
  }
}
