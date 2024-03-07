package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

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
  void parentKey_GivenKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, new CurrentTime());

    // Act
    // Assert
    assertThat(group.parentKey()).isEqualTo("0000");
  }

  @Test
  void fullKey_GivenKeyManipulator_ShouldReturnProperly() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, new CurrentTime());

    // Act
    // Assert
    assertThat(group.fullKey("child-key")).isEqualTo("0000:child-key");
  }

  @Test
  void reserveNewSlot_GivenArbitrarySlot_ShouldStoreIt() {
    // Arrange
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, new CurrentTime());
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);

    // Act
    // Assert
    assertThat(group.size()).isNull();
    assertThat(group.isClosed()).isFalse();

    group.reserveNewSlot(slot1);
    assertThat(group.size()).isNull();
    assertThat(group.isClosed()).isFalse();

    group.reserveNewSlot(slot2);
    assertThat(group.size()).isEqualTo(2);
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
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2, new CurrentTime());
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
  void removeNotReadySlots() throws InterruptedException, ExecutionException {
    // Arrange
    CountDownLatch wait = new CountDownLatch(1);
    Emittable<String, Integer> waitableEmitter = (s, values) -> wait.await();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(waitableEmitter, keyManipulator, 100, 1000, 2 + 1, new CurrentTime());
    Slot<String, String, String, String, Integer> slot1 = new Slot<>("child-key-1", group);
    Slot<String, String, String, String, Integer> slot2 = new Slot<>("child-key-2", group);
    Slot<String, String, String, String, Integer> slot3 = new Slot<>("child-key-3", group);
    ExecutorService executorService = Executors.newCachedThreadPool();

    group.reserveNewSlot(slot1);
    group.reserveNewSlot(slot2);
    group.reserveNewSlot(slot3);

    // Act
    // Assert

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

    TimeUnit.MILLISECONDS.sleep(500);
    // The status isn't READY yet since slot3 isn't ready.
    assertThat(group.isReady()).isFalse();

    // Remove the not-ready slot (slot3).
    List<Slot<String, String, String, String, Integer>> notReadySlots = group.removeNotReadySlots();
    assertThat(notReadySlots).isNotNull().size().isEqualTo(1);
    assertThat(notReadySlots.get(0).isReady()).isFalse();
    assertThat(notReadySlots.get(0).isReady()).isFalse();
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
  }

  @Test
  void groupClosedAt_GivenCurrentTime_ShouldReturnProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, currentTime);

    // Act
    // Assert
    assertThat(group.groupClosedMillisAt()).isEqualTo(startTimeMillis + 100);
  }

  @Test
  void delayedSlotMovedAt_GivenCurrentTime_ShouldReturnProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, currentTime);

    // Act
    // Assert
    assertThat(group.delayedSlotMovedMillisAt()).isEqualTo(startTimeMillis + 1000);
  }

  @Test
  void updateDelayedSlotMovedAt_GivenCurrentTime_ShouldUpdateProperly() {
    // Arrange
    long startTimeMillis = System.currentTimeMillis();
    CurrentTime currentTime = spy(new CurrentTime());
    doReturn(startTimeMillis).when(currentTime).currentTimeMillis();
    NormalGroup<String, String, String, String, Integer> group =
        new NormalGroup<>(emitter, keyManipulator, 100, 1000, 2, currentTime);

    // Act
    long updateTimeMillis = startTimeMillis + 10;
    doReturn(updateTimeMillis).when(currentTime).currentTimeMillis();
    group.updateDelayedSlotMovedAt();

    // Assert
    assertThat(group.delayedSlotMovedMillisAt()).isEqualTo(updateTimeMillis + 1000);
  }
}
