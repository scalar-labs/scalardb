package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupManagerTest {
  private static final int TIMEOUT_CHECK_INTERVAL_MILLIS = 10;
  private TestableKeyManipulator keyManipulator;
  @Mock private Emittable<String, Integer> emittable;
  @Mock private GroupSizeFixWorker<String, String, String, String, Integer> groupSizeFixWorker;
  @Mock private GroupCleanupWorker<String, String, String, String, Integer> groupCleanupWorker;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup1;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup2;
  @Mock private DelayedGroup<String, String, String, String, Integer> delayedGroup1;
  @Mock private DelayedGroup<String, String, String, String, Integer> delayedGroup2;

  @BeforeEach
  void setUp() {
    keyManipulator = new TestableKeyManipulator();
  }

  @AfterEach
  void tearDown() {}

  @Test
  void reserveNewSlot_WhenCurrentGroupDoesNotExist_ShouldCreateNewGroupAndReserveSlotInIt() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Act

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", OPEN, slots:[Slot("child-key-1")])

    // Assert
    assertThat(keys1.parentKey).isEqualTo("0000");
  }

  @Test
  void reserveNewSlot_WhenAvailableSlotsExistInCurrentGroup_ShouldReserveSlotInCurrentGroup() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Act

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])

    // Assert
    assertThat(keys1.parentKey).isEqualTo("0000");
    assertThat(keys2.parentKey).isEqualTo("0000");
  }

  @Test
  void reserveNewSlot_WhenCurrentGroupIsSizeFixed_ShouldCreateNewGroupAndReserveSlotInIt() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Act

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // Add slot-3.
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", OPEN, slots:[Slot("child-key-3")])

    // Assert
    assertThat(keys1.parentKey).isEqualTo("0000");
    assertThat(keys2.parentKey).isEqualTo("0000");
    // The slot capacity is 2 and the 3rd slot must be in a new NormalGroup.
    assertThat(keys3.parentKey).isEqualTo("0001");
  }

  @Test
  void getGroup_GivenKeyForNormalGroup_ShouldReturnProperly() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // Add slot-3.
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", OPEN, slots:[Slot("child-key-3")])

    // Act
    Group<String, String, String, String, Integer> groupForKeys1 = groupManager.getGroup(keys1);
    Group<String, String, String, String, Integer> groupForKeys2 = groupManager.getGroup(keys2);
    Group<String, String, String, String, Integer> groupForKeys3 = groupManager.getGroup(keys3);

    // Assert
    assertThat(groupForKeys1).isInstanceOf(NormalGroup.class);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupForKeys1;

    assertThat(groupForKeys3).isInstanceOf(NormalGroup.class);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey3 =
        (NormalGroup<String, String, String, String, Integer>) groupForKeys3;

    // The first 2 NormalGroups are the same since the capacity is 2.
    assertThat(normalGroupForKey1).isEqualTo(groupForKeys2);
    // The 3rd slot must be in a new NormalGroup.
    assertThat(normalGroupForKey1).isNotEqualTo(normalGroupForKey3);

    assertThat(normalGroupForKey1.parentKey()).isEqualTo("0000");
    assertThat(normalGroupForKey1.isSizeFixed()).isTrue();
    assertThat(normalGroupForKey1.isReady()).isFalse();
    assertThat(normalGroupForKey3.parentKey()).isEqualTo("0001");
    assertThat(normalGroupForKey3.isSizeFixed()).isFalse();
  }

  @Test
  void getGroup_GivenKeyForDelayedGroup_ShouldReturnProperly()
      throws ExecutionException, InterruptedException {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    ExecutorService executorService = Executors.newCachedThreadPool();

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);

    // Put a value to the slot to mark it as ready.
    Future<Boolean> future =
        executorService.submit(() -> normalGroupForKey1.putValueToSlotAndWait("child-key-1", 42));
    executorService.shutdown();
    // Wait until the thread waits on the slot.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])

    assertThat(groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1)).isTrue();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", SIZE-FIXED, slots:[Slot("child-key-2")])

    // Act

    // Assert
    assertThat(future.get()).isTrue();

    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.isDone()).isTrue();

    DelayedGroup<String, String, String, String, Integer> delayedGroupForKey2 =
        (DelayedGroup<String, String, String, String, Integer>) groupManager.getGroup(keys2);
    assertThat(delayedGroupForKey2.isSizeFixed()).isTrue();
    assertThat(delayedGroupForKey2.isReady()).isFalse();
  }

  @Test
  void removeGroupFromMap_GivenKeyForNormalGroup_ShouldRemoveThemProperly() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    groupManager.reserveNewSlot("child-key-2");
    // Add slot-3.
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", OPEN, slots:[Slot("child-key-3")])

    // Act
    // Assert
    assertThat(groupManager.removeGroupFromMap(groupManager.getGroup(keys1))).isTrue();
    assertThrows(GroupCommitException.class, () -> groupManager.getGroup(keys1));
    assertThat(groupManager.removeGroupFromMap(groupManager.getGroup(keys3))).isTrue();
    assertThrows(GroupCommitException.class, () -> groupManager.getGroup(keys3));
  }

  @Test
  void removeGroupFromMap_GivenKeyForDelayedGroup_ShouldRemoveThemProperly()
      throws ExecutionException, InterruptedException {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    ExecutorService executorService = Executors.newCachedThreadPool();

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);

    // Put a value to the slot to mark the slot as ready.
    Future<Boolean> future =
        executorService.submit(() -> normalGroupForKey1.putValueToSlotAndWait("child-key-1", 42));
    executorService.shutdown();
    // Wait until the thread waits on the slot.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])

    assertThat(groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1)).isTrue();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", SIZE-FIXED, slots:[Slot("child-key-2")])

    Group<String, String, String, String, Integer> groupForKey2 = groupManager.getGroup(keys2);
    assertThat(groupForKey2).isInstanceOf(DelayedGroup.class);
    assertThat(groupForKey2.isSizeFixed()).isTrue();
    assertThat(groupForKey2.isReady()).isFalse();
    assertThat(groupForKey2.slots.size()).isEqualTo(1);
    assertThat(groupForKey2.slots.get("child-key-2")).isNotNull();

    // Act
    // Assert
    assertThat(groupManager.removeGroupFromMap(groupForKey2)).isTrue();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])

    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // No groups are supposed to exist at this moment.

    assertThat(future.get()).isTrue();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.isDone()).isTrue();
  }

  @Test
  void removeSlotFromGroup_GivenKeyForSlotInNormalGroup_ShouldRemoveSlotFromThemProperly() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    // Add slot-3.
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", OPEN, slots:[Slot("child-key-3")])

    Group<String, String, String, String, Integer> groupForKeys1 = groupManager.getGroup(keys1);
    Group<String, String, String, String, Integer> groupForKeys3 = groupManager.getGroup(keys3);

    // Act
    // Assert
    assertThat(groupManager.removeSlotFromGroup(keys1)).isTrue();
    assertThat(groupForKeys1.isSizeFixed()).isTrue();
    assertThat(groupForKeys1.isReady()).isFalse();
    assertThat(groupForKeys1.size()).isEqualTo(1);
    assertThat(groupManager.removeSlotFromGroup(keys1)).isFalse();

    assertThat(groupManager.removeSlotFromGroup(keys2)).isTrue();
    assertThat(groupForKeys1.isDone()).isTrue();
    assertThat(groupForKeys1.size()).isEqualTo(0);
    assertThat(groupManager.removeSlotFromGroup(keys2)).isFalse();

    assertThat(groupManager.removeSlotFromGroup(keys3)).isTrue();
    assertThat(groupForKeys3.isDone()).isTrue();
    assertThat(groupForKeys3.size()).isEqualTo(0);
    assertThat(groupManager.removeSlotFromGroup(keys3)).isFalse();
  }

  @Test
  void removeSlotFromGroup_GivenKeyForSlotInDelayedGroup_ShouldRemoveSlotFromThemProperly()
      throws ExecutionException, InterruptedException {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    ExecutorService executorService = Executors.newCachedThreadPool();

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);
    Future<Boolean> future =
        executorService.submit(() -> normalGroupForKey1.putValueToSlotAndWait("child-key-1", 42));
    executorService.shutdown();
    // Wait until the thread waits on the slot.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])

    assertThat(groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1)).isTrue();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.slots.get("child-key-1")).isNotNull();
    assertThat(normalGroupForKey1.slots.get("child-key-2")).isNull();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", SIZE-FIXED, slots:[Slot("child-key-2")])
    Group<String, String, String, String, Integer> groupForKey2 = groupManager.getGroup(keys2);
    assertThat(groupForKey2).isInstanceOf(DelayedGroup.class);
    assertThat(groupForKey2.isSizeFixed()).isTrue();
    assertThat(groupForKey2.isReady()).isFalse();
    assertThat(groupForKey2.slots.size()).isEqualTo(1);
    assertThat(groupForKey2.slots.get("child-key-2")).isNotNull();

    // Act
    // Assert
    assertThat(groupManager.removeSlotFromGroup(keys2)).isTrue();
    assertThat(groupForKey2.isDone()).isTrue();
    assertThat(groupForKey2.slots.size()).isEqualTo(0);
    assertThat(groupForKey2.slots.get("child-key-2")).isNull();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", DONE, slots:[])

    assertThat(future.get()).isTrue();
    assertThat(normalGroupForKey1.isDone()).isTrue();
  }

  @Test
  void moveDelayedSlotToDelayedGroup_GivenKeyForOpenGroup_ShouldKeepThem() {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", OPEN, slots:[Slot("child-key-1")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);

    // Act
    boolean moved = groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1);

    // Assert
    assertThat(moved).isFalse();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.isSizeFixed()).isFalse();
  }

  @Test
  void moveDelayedSlotToDelayedGroup_GivenKeyForOpenGroupWithReadySlot_ShouldKeepThem()
      throws ExecutionException, InterruptedException {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(3, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    ExecutorService executorService = Executors.newCachedThreadPool();

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    groupManager.reserveNewSlot("child-key-2");
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", OPEN, slots:[Slot("child-key-1"), Slot("child-key-2")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);
    Future<Boolean> future =
        executorService.submit(() -> normalGroupForKey1.putValueToSlotAndWait("child-key-1", 42));
    executorService.shutdown();
    // Wait until the thread waits on the slot.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", OPEN, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])

    // Act
    // Assert

    boolean moved = groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1);
    assertThat(moved).isFalse();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(2);
    assertThat(normalGroupForKey1.isSizeFixed()).isFalse();

    // Size-fix the first group.
    normalGroupForKey1.fixSize();
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])
    assertThat(normalGroupForKey1.isSizeFixed()).isTrue();

    moved = groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", READY, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", SIZE-FIXED, slots:[Slot("child-key-2")])
    assertThat(moved).isTrue();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.isReady()).isTrue();
    assertThat(future.get()).isTrue();
  }

  @Test
  void
      moveDelayedSlotToDelayedGroup_GivenKeyForSizeFixedGroupWithAllNotReadySlots_ShouldKeepThem() {
    // Arrange
    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    groupManager.reserveNewSlot("child-key-2");
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot("child-key-1"), Slot("child-key-2")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);

    // Act
    boolean moved = groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1);

    // Assert
    assertThat(moved).isFalse();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(2);
    assertThat(normalGroupForKey1.isSizeFixed()).isTrue();
  }

  @Test
  void
      moveDelayedSlotToDelayedGroup_GivenKeyForSizeFixedGroupWithReadySlot_ShouldRemoveNotReadySlot()
          throws InterruptedException, ExecutionException {
    // Arrange

    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            new GroupCommitConfig(2, 100, 400, 60, TIMEOUT_CHECK_INTERVAL_MILLIS), keyManipulator);
    groupManager.setEmitter(emittable);
    groupManager.setGroupSizeFixWorker(groupSizeFixWorker);
    groupManager.setGroupCleanupWorker(groupCleanupWorker);

    ExecutorService executorService = Executors.newCachedThreadPool();

    // Add slot-1.
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    // Add slot-2.
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);
    Future<Boolean> future =
        executorService.submit(() -> normalGroupForKey1.putValueToSlotAndWait("child-key-1", 42));
    executorService.shutdown();
    // Wait until the thread waits on the slot.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1"), Slot("child-key-2")])

    // Act
    boolean moved = groupManager.moveDelayedSlotToDelayedGroup(normalGroupForKey1);
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", SIZE-FIXED, slots:[Slot(READY, "child-key-1")])
    // - DelayedGroup("0000:child-key-2", slots:[Slot("child-key-2")])

    // Assert
    assertThat(future.get()).isTrue();

    assertThat(moved).isTrue();
    assertThat(normalGroupForKey1.slots.size()).isEqualTo(1);
    assertThat(normalGroupForKey1.isDone()).isTrue();

    DelayedGroup<String, String, String, String, Integer> delayedGroupForKey2 =
        (DelayedGroup<String, String, String, String, Integer>) groupManager.getGroup(keys2);
    assertThat(delayedGroupForKey2.isSizeFixed()).isTrue();
    assertThat(delayedGroupForKey2.isReady()).isFalse();
  }
}
