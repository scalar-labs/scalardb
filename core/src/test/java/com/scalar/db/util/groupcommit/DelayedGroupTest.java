package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;

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
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", emitter, keyManipulator, new CurrentTime());

    // Act
    // Assert
    assertThat(group.fullKey()).isEqualTo("0000:full-key");
  }

  @Test
  void reserveNewSlot_GivenArbitrarySlot_ShouldStoreIt() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, new CurrentTime());
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", emitter, keyManipulator, new CurrentTime());

    assertThat(group.size()).isNull();
    assertThat(group.isClosed()).isFalse();

    // Act
    group.reserveNewSlot(slot);

    // Assert
    assertThat(group.size()).isEqualTo(1);
    assertThat(group.isClosed()).isTrue();
    assertThat(group.isReady()).isFalse();
  }

  @Test
  void putValueToSlotAndWait_GivenValuesIntoSlots_ShouldExecuteEmitTaskProperly()
      throws InterruptedException, ExecutionException {
    // Arrange
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2, new CurrentTime());
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", waitableEmitter, keyManipulator, new CurrentTime());
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

    TimeUnit.MILLISECONDS.sleep(500);
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
  void removeSlot_GivenNoReadySlots_ShouldRemoveSlotAndGetDone() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, new CurrentTime());
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", emitter, keyManipulator, new CurrentTime());

    group.reserveNewSlot(slot);
    assertThat(group.isClosed()).isTrue();
    assertThat(group.isReady()).isFalse();

    // Act
    // Assert

    assertThat(group.removeSlot("child-key")).isTrue();
    assertThat(group.isClosed()).isTrue();
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isTrue();
    assertThat(group.removeSlot("child-key")).isFalse();
  }

  @Test
  void removeSlot_GivenReadySlots_ShouldRemoveSlotAndGetDone()
      throws InterruptedException, ExecutionException {
    // Arrange
    AtomicBoolean emitted = new AtomicBoolean();
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter =
        (s, values) -> {
          wait.await();
          emitted.set(true);
        };
    NormalGroup<String, String, String, String, Integer> oldGroup =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2, new CurrentTime());
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", oldGroup);
    DelayedGroup<String, String, String, String, Integer> group =
        new DelayedGroup<>("0000:full-key", waitableEmitter, keyManipulator, new CurrentTime());
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot);
    assertThat(group.isClosed()).isTrue();
    assertThat(group.isReady()).isFalse();

    List<Future<Void>> futures = new ArrayList<>();
    futures.add(
        executorService.submit(
            () -> {
              group.putValueToSlotAndWait(slot.key(), 42);
              return null;
            }));
    executorService.shutdown();

    TimeUnit.MILLISECONDS.sleep(500);

    // At the moment,
    // - slot is ready, but not done since it's blocked

    // Act
    // Assert

    assertThat(group.removeSlot("child-key")).isTrue();
    assertThat(group.isClosed()).isTrue();
    assertThat(group.isReady()).isTrue();
    assertThat(group.isDone()).isTrue();
    assertThat(group.removeSlot("child-key")).isFalse();
    assertThat(group.size()).isEqualTo(0);

    // This wait is needed to prevent the DelayedGroup from immediately getting done.
    wait.countDown();

    assertThat(futures.size()).isEqualTo(1);
    futures.get(0).get();
    assertThat(emitted.get()).isTrue();
  }
}
