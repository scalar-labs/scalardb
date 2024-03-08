package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GroupManagerTest {
  private CurrentTime currentTime;
  private TestableKeyManipulator keyManipulator;

  @BeforeEach
  void setUp() {
    currentTime = spy(new CurrentTime());
    keyManipulator = new TestableKeyManipulator();
  }

  @AfterEach
  void tearDown() {}

  @Test
  void reserveNewSlot_GivenMoreSlotsThanCapacity_ShouldCreateNewNormalGroup() {
    // Arrange
    doReturn(System.currentTimeMillis()).when(currentTime).currentTimeMillis();
    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            "test", new GroupCommitConfig(2, 100, 1000, 10), keyManipulator, currentTime);

    // Act
    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", slots:[Slot("child-key-3")])

    // Assert
    assertThat(keys1.parentKey).isEqualTo("0000");
    assertThat(keys2.parentKey).isEqualTo("0000");
    // The slot capacity is 2 and the 3rd slot must be in a new NormalGroup.
    assertThat(keys3.parentKey).isEqualTo("0001");
  }

  @Test
  void reserveNewSlot_GivenCurrentGroupClosed_ShouldCreateNewNormalGroup() {
    // TODO
  }

  @Test
  void getGroup_GivenNormalGroups_ShouldReturnProperly() {
    // Arrange
    doReturn(System.currentTimeMillis()).when(currentTime).currentTimeMillis();
    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            "test", new GroupCommitConfig(2, 100, 1000, 10), keyManipulator, currentTime);

    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", slots:[Slot("child-key-3")])

    // Act
    Group<String, String, String, String, Integer> groupForKeys1 = groupManager.getGroup(keys1);
    Group<String, String, String, String, Integer> groupForKeys2 = groupManager.getGroup(keys2);
    Group<String, String, String, String, Integer> groupForKeys3 = groupManager.getGroup(keys3);

    // Assert
    assertThat(groupForKeys1).isInstanceOf(NormalGroup.class);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupForKeys1;

    assertThat(groupForKeys2).isInstanceOf(NormalGroup.class);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey2 =
        (NormalGroup<String, String, String, String, Integer>) groupForKeys2;

    assertThat(groupForKeys3).isInstanceOf(NormalGroup.class);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey3 =
        (NormalGroup<String, String, String, String, Integer>) groupForKeys3;

    // The first 2 NormalGroups are the same since the capacity is 2.
    assertThat(normalGroupForKey1).isEqualTo(normalGroupForKey2);
    // The 3rd slot must be in a new NormalGroup.
    assertThat(normalGroupForKey1).isNotEqualTo(normalGroupForKey3);

    assertThat(normalGroupForKey1.parentKey()).isEqualTo("0000");
    assertThat(normalGroupForKey1.isClosed()).isTrue();
    assertThat(normalGroupForKey1.isReady()).isFalse();
    assertThat(normalGroupForKey3.parentKey()).isEqualTo("0001");
    assertThat(normalGroupForKey3.isClosed()).isFalse();
  }

  @Test
  void getGroup_GivenDelayedGroups_ShouldReturnProperly() {
    // TODO
  }

  @Test
  void removeGroupFromMap_GivenNormalGroups_ShouldRemoveThemProperly() {
    // Arrange
    doReturn(System.currentTimeMillis()).when(currentTime).currentTimeMillis();
    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            "test", new GroupCommitConfig(2, 100, 1000, 10), keyManipulator, currentTime);

    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", slots:[Slot("child-key-3")])

    // Act
    // Assert
    assertThat(groupManager.removeGroupFromMap(groupManager.getGroup(keys1))).isTrue();
    assertThrows(GroupCommitException.class, () -> groupManager.getGroup(keys1));
    assertThat(groupManager.removeGroupFromMap(groupManager.getGroup(keys3))).isTrue();
    assertThrows(GroupCommitException.class, () -> groupManager.getGroup(keys3));
  }

  @Test
  void removeGroupFromMap_GivenDelayedGroups_ShouldRemoveThemProperly() {
    // TODO
  }

  @Test
  void removeSlotFromGroup_GivenNormalGroups_ShouldRemoveSlotFromThemProperly() {
    // Arrange
    doReturn(System.currentTimeMillis()).when(currentTime).currentTimeMillis();
    GroupManager<String, String, String, String, Integer> groupManager =
        new GroupManager<>(
            "test", new GroupCommitConfig(2, 100, 1000, 10), keyManipulator, currentTime);

    Keys<String, String, String> keys1 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-1"));
    Keys<String, String, String> keys2 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-2"));
    Keys<String, String, String> keys3 =
        keyManipulator.keysFromFullKey(groupManager.reserveNewSlot("child-key-3"));
    // These groups are supposed to exist at this moment.
    // - NormalGroup("0000", slots:[Slot("child-key-1"), Slot("child-key-2")])
    // - NormalGroup("0001", slots:[Slot("child-key-3")])

    NormalGroup<String, String, String, String, Integer> normalGroupForKey1 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys1);
    NormalGroup<String, String, String, String, Integer> normalGroupForKey3 =
        (NormalGroup<String, String, String, String, Integer>) groupManager.getGroup(keys3);

    // Act
    // Assert
    assertThat(groupManager.removeSlotFromGroup(keys1)).isTrue();
    assertThat(normalGroupForKey1.isClosed()).isTrue();
    assertThat(normalGroupForKey1.isReady()).isFalse();
    assertThat(normalGroupForKey1.size()).isEqualTo(1);
    assertThat(groupManager.removeSlotFromGroup(keys1)).isFalse();

    assertThat(groupManager.removeSlotFromGroup(keys2)).isTrue();
    assertThat(normalGroupForKey1.isDone()).isTrue();
    assertThat(normalGroupForKey1.size()).isEqualTo(0);
    assertThat(groupManager.removeSlotFromGroup(keys2)).isFalse();

    assertThat(groupManager.removeSlotFromGroup(keys3)).isTrue();
    assertThat(normalGroupForKey3.isDone()).isTrue();
    assertThat(normalGroupForKey3.size()).isEqualTo(0);
    assertThat(groupManager.removeSlotFromGroup(keys3)).isFalse();
  }

  @Test
  void removeSlotFromGroup_GivenDelayedGroups_ShouldRemoveSlotFromThemProperly() {
    // TODO
  }

  @Test
  void moveDelayedSlotToDelayedGroup() {
    // TODO
  }
}
