package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupCloseWorkerTest {
  private static final int LONG_WAIT_MILLIS = 500;

  @Mock
  private DelayedSlotMoveWorker<String, String, String, String, Integer> delayedSlotMoveWorker;

  @Mock private GroupCleanupWorker<String, String, String, String, Integer> groupCleanupWorker;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup1;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup2;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup3;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup4;
  private GroupCloseWorker<String, String, String, String, Integer> worker;
  private GroupCloseWorker<String, String, String, String, Integer> workerWithWait;

  @BeforeEach
  void setUp() {
    worker = new GroupCloseWorker<>("test", 10, delayedSlotMoveWorker, groupCleanupWorker);
    workerWithWait =
        spy(
            new GroupCloseWorker<>(
                "long-wait-test", LONG_WAIT_MILLIS, delayedSlotMoveWorker, groupCleanupWorker));
  }

  @AfterEach
  void tearDown() {
    worker.close();
    workerWithWait.close();
  }

  @Test
  void add_GivenClosedGroup_ShouldPassItToDelayedSlotMoveWorker() {
    // Arrange
    doReturn(true).when(normalGroup1).isClosed();
    doReturn(false).when(normalGroup1).isReady();

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    verify(normalGroup1, never()).close();
    assertThat(worker.size()).isEqualTo(0);
    verify(delayedSlotMoveWorker).add(normalGroup1);
    verify(groupCleanupWorker, never()).add(normalGroup1);
  }

  @Test
  void add_GivenReadyGroup_ShouldPassItToGroupCleanupWorker() {
    // Arrange
    doReturn(true).when(normalGroup1).isClosed();
    doReturn(true).when(normalGroup1).isReady();

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    verify(normalGroup1, never()).close();
    assertThat(worker.size()).isEqualTo(0);
    verify(delayedSlotMoveWorker, never()).add(normalGroup1);
    verify(groupCleanupWorker).add(normalGroup1);
  }

  @Test
  void add_GivenOpenGroupNotTimedOut_ShouldKeepItWithWait() {
    // Arrange
    doReturn(false).when(normalGroup1).isClosed();
    doReturn(System.currentTimeMillis() + LONG_WAIT_MILLIS * 10)
        .when(normalGroup1)
        .groupClosedMillisAt();

    // Act
    workerWithWait.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(LONG_WAIT_MILLIS * 2, TimeUnit.MILLISECONDS);

    // Assert
    verify(workerWithWait, atMost(2)).processItem(any());
    verify(normalGroup1, never()).close();
    assertThat(workerWithWait.size()).isEqualTo(1);
    verify(delayedSlotMoveWorker, never()).add(normalGroup1);
    verify(groupCleanupWorker, never()).add(normalGroup1);
  }

  @Test
  void add_GivenOpenGroupTimedOut_ShouldCloseItAndPassItToDelayedSlotMoveWorker() {
    // Arrange
    doReturn(false).when(normalGroup1).isClosed();
    doReturn(false).when(normalGroup1).isReady();
    doReturn(System.currentTimeMillis() - 5).when(normalGroup1).groupClosedMillisAt();

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    verify(normalGroup1).close();
    assertThat(worker.size()).isEqualTo(0);
    verify(delayedSlotMoveWorker).add(normalGroup1);
    verify(groupCleanupWorker, never()).add(normalGroup1);
  }

  @Test
  void
      add_GivenOpenGroupTimedOut_WhichWillBeReadyAfterClosed_ShouldCloseItAndPassItToGroupCleanupWorker() {
    // Arrange
    doReturn(false).when(normalGroup1).isClosed();
    doReturn(true).when(normalGroup1).isReady();
    doReturn(System.currentTimeMillis() - 5).when(normalGroup1).groupClosedMillisAt();

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    verify(normalGroup1).close();
    assertThat(worker.size()).isEqualTo(0);
    verify(delayedSlotMoveWorker, never()).add(normalGroup1);
    verify(groupCleanupWorker).add(normalGroup1);
  }

  @Test
  void
      add_GivenMultipleOpenGroupsTimedOut_ShouldCloseThemAndPassThemToDelayedSlotMoveWorkerWithoutWait() {
    // Arrange
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(
            g -> {
              doReturn(false).when(g).isClosed();
              doReturn(false).when(g).isReady();
              doReturn(System.currentTimeMillis() - 5).when(g).groupClosedMillisAt();
            });

    // Act
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(g -> workerWithWait.add(g));
    Uninterruptibles.sleepUninterruptibly(LONG_WAIT_MILLIS * 2, TimeUnit.MILLISECONDS);

    // Assert
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(
            g -> {
              verify(g).close();
              verify(delayedSlotMoveWorker).add(g);
            });
    assertThat(workerWithWait.size()).isEqualTo(0);
    verify(groupCleanupWorker, never()).add(any());
  }
}
