package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupCleanupWorkerTest {
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup;
  @Mock private DelayedGroup<String, String, String, String, Integer> delayedGroup;
  @Mock private GroupManager<String, String, String, String, Integer> groupManager;
  private GroupCleanupWorker<String, String, String, String, Integer> worker;

  @BeforeEach
  void setUp() {
    worker = new GroupCleanupWorker<>("test", 10, groupManager, new CurrentTime());
  }

  @AfterEach
  void tearDown() {
    worker.close();
  }

  @Test
  void processItem_GivenDoneNormalGroup_ShouldRemoveItFromGroupManager() {
    // Arrange
    doReturn(true).when(normalGroup).isDone();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(groupManager).removeGroupFromMap(normalGroup);
  }

  @Test
  void processItem_GivenNotDoneNormalGroup_ShouldKeepIt() {
    // Arrange
    doReturn(false).when(normalGroup).isDone();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isFalse();
    verify(groupManager, never()).removeGroupFromMap(normalGroup);
  }

  @Test
  void processItem_GivenDoneDelayedGroup_ShouldRemoveItFromGroupManager() {
    // Arrange
    doReturn(true).when(delayedGroup).isDone();

    // Act
    // Assert
    assertThat(worker.processItem(delayedGroup)).isTrue();
    verify(groupManager).removeGroupFromMap(delayedGroup);
  }

  @Test
  void processItem_GivenNotDoneDelayedGroup_ShouldKeepIt() {
    // Arrange
    doReturn(false).when(delayedGroup).isDone();

    // Act
    // Assert
    assertThat(worker.processItem(delayedGroup)).isFalse();
    verify(groupManager, never()).removeGroupFromMap(delayedGroup);
  }
}
