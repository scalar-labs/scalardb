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
class GroupCloseWorkerTest {
  @Mock
  private DelayedSlotMoveWorker<String, String, String, String, Integer> delayedSlotMoveWorker;

  @Mock private GroupCleanupWorker<String, String, String, String, Integer> groupCleanupWorker;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup;
  private CurrentTime currentTime;
  private GroupCloseWorker<String, String, String, String, Integer> worker;

  @BeforeEach
  void setUp() {
    currentTime = spy(new CurrentTime());
    worker =
        new GroupCloseWorker<>("test", 10, delayedSlotMoveWorker, groupCleanupWorker, currentTime);
  }

  @AfterEach
  void tearDown() {
    worker.close();
  }

  @Test
  void processItem_GivenClosedGroup_ShouldPassItToDelayedSlotMoveWorker() {
    // Arrange
    doReturn(true).when(normalGroup).isClosed();
    doReturn(false).when(normalGroup).isReady();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(delayedSlotMoveWorker).add(normalGroup);
    verify(groupCleanupWorker, never()).add(normalGroup);
  }

  @Test
  void processItem_GivenReadyGroup_ShouldPassItToGroupCleanupWorker() {
    // Arrange
    doReturn(true).when(normalGroup).isClosed();
    doReturn(true).when(normalGroup).isReady();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(delayedSlotMoveWorker, never()).add(normalGroup);
    verify(groupCleanupWorker).add(normalGroup);
  }

  @Test
  void processItem_GivenOpenGroupNotTimedOut_ShouldKeepIt() {
    // Arrange
    long now = System.currentTimeMillis();
    doReturn(now).when(currentTime).currentTimeMillis();

    doReturn(false).when(normalGroup).isClosed();
    doReturn(now + 5).when(normalGroup).groupClosedMillisAt();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isFalse();
    verify(normalGroup, never()).close();
    verify(delayedSlotMoveWorker, never()).add(normalGroup);
    verify(groupCleanupWorker, never()).add(normalGroup);
  }

  @Test
  void processItem_GivenOpenGroupTimedOut_ShouldCloseItAndPassItToDelayedSlotMoveWorker() {
    // Arrange
    long now = System.currentTimeMillis();
    doReturn(now).when(currentTime).currentTimeMillis();

    doReturn(false).when(normalGroup).isClosed();
    doReturn(now - 5).when(normalGroup).groupClosedMillisAt();
    doReturn(false).when(normalGroup).isReady();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(normalGroup).close();
    verify(delayedSlotMoveWorker).add(normalGroup);
    verify(groupCleanupWorker, never()).add(normalGroup);
  }

  @Test
  void
      processItem_GivenOpenGroupTimedOut_WhichWillBeReadyAfterClosed_ShouldCloseItAndPassItToGroupCleanupWorker() {
    // Arrange
    long now = System.currentTimeMillis();
    doReturn(now).when(currentTime).currentTimeMillis();

    doReturn(false).when(normalGroup).isClosed();
    doReturn(now - 5).when(normalGroup).groupClosedMillisAt();
    doReturn(true).when(normalGroup).isReady();

    // Act
    // Assert
    assertThat(worker.processItem(normalGroup)).isTrue();
    verify(normalGroup).close();
    verify(delayedSlotMoveWorker, never()).add(normalGroup);
    verify(groupCleanupWorker).add(normalGroup);
  }
}
