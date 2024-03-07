package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DelayedSlotMoveWorkerTest {
  @Mock private GroupCleanupWorker<String, String, String, String, Integer> groupCleanupWorker;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup;
  @Mock private GroupManager<String, String, String, String, Integer> groupManager;
  private CurrentTime currentTime;
  private DelayedSlotMoveWorker<String, String, String, String, Integer> worker;

  @BeforeEach
  void setUp() {
    currentTime = spy(new CurrentTime());
    worker = new DelayedSlotMoveWorker<>("test", 10, groupManager, groupCleanupWorker, currentTime);
  }

  @AfterEach
  void tearDown() {
    worker.close();
  }

  @Test
  void processItem_GivenReadyGroup_ShouldPassItToGroupCleanupWorker() {
    // Arrange
    doReturn(true).when(normalGroup).isReady();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(groupManager, never()).moveDelayedSlotToDelayedGroup(normalGroup);
    verify(groupCleanupWorker).add(normalGroup);
  }

  @Test
  void processItem_GivenNotReadyGroupNotTimedOut_ShouldKeepIt() {
    // Arrange
    long now = System.currentTimeMillis();
    doReturn(now).when(currentTime).currentTimeMillis();

    doReturn(false).when(normalGroup).isReady();
    doReturn(now + 5).when(normalGroup).delayedSlotMovedMillisAt();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isFalse();
    verify(groupManager, never()).moveDelayedSlotToDelayedGroup(normalGroup);
    verify(groupCleanupWorker, never()).add(normalGroup);
  }

  @Test
  void processItem_GivenNotReadyGroupTimedOut_ShouldMoveDelayedSlotsToDelayedGroup() {
    // Arrange
    long now = System.currentTimeMillis();
    doReturn(now).when(currentTime).currentTimeMillis();

    // The group is supposed to be ready after removing delayed slots.
    doReturn(false, true).when(normalGroup).isReady();
    doReturn(now - 5).when(normalGroup).delayedSlotMovedMillisAt();
    doReturn(true).when(groupManager).moveDelayedSlotToDelayedGroup(normalGroup);

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(groupManager).moveDelayedSlotToDelayedGroup(normalGroup);
    verify(groupCleanupWorker).add(normalGroup);
  }
}
