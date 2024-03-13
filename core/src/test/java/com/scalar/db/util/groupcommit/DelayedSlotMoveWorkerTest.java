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
class DelayedSlotMoveWorkerTest {
  private static final int LONG_WAIT_MILLIS = 500;
  @Mock private GroupCleanupWorker<String, String, String, String, Integer> groupCleanupWorker;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup1;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup2;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup3;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup4;
  @Mock private GroupManager<String, String, String, String, Integer> groupManager;
  private DelayedSlotMoveWorker<String, String, String, String, Integer> worker;
  private DelayedSlotMoveWorker<String, String, String, String, Integer> workerWithWait;

  @BeforeEach
  void setUp() {
    worker = new DelayedSlotMoveWorker<>("test", 10, groupManager, groupCleanupWorker);
    workerWithWait =
        spy(
            new DelayedSlotMoveWorker<>(
                "long-wait-test", LONG_WAIT_MILLIS, groupManager, groupCleanupWorker));
  }

  @AfterEach
  void tearDown() {
    worker.close();
  }

  @Test
  void add_GivenReadyGroup_ShouldPassItToGroupCleanupWorker() {
    // Arrange
    doReturn(true).when(normalGroup1).isReady();

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    assertThat(worker.size()).isEqualTo(0);
    verify(groupManager, never()).moveDelayedSlotToDelayedGroup(normalGroup1);
    verify(groupCleanupWorker).add(normalGroup1);
  }

  @Test
  void add_GivenNotReadyGroupNotTimedOut_ShouldKeepItWithWait() {
    // Arrange
    doReturn(false).when(normalGroup1).isReady();
    doReturn(System.currentTimeMillis() + LONG_WAIT_MILLIS * 10)
        .when(normalGroup1)
        .delayedSlotMovedMillisAt();

    // Act
    workerWithWait.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(LONG_WAIT_MILLIS * 2, TimeUnit.MILLISECONDS);

    // Assert
    verify(workerWithWait, atMost(2)).processItem(any());
    assertThat(workerWithWait.size()).isEqualTo(1);
    verify(groupManager, never()).moveDelayedSlotToDelayedGroup(normalGroup1);
    verify(groupCleanupWorker, never()).add(normalGroup1);
  }

  @Test
  void add_GivenNotReadyGroupTimedOut_ShouldMoveDelayedSlotsToDelayedGroup() {
    // Arrange

    // The group is supposed to be ready after removing delayed slots.
    doReturn(false, true).when(normalGroup1).isReady();
    doReturn(System.currentTimeMillis() - 5).when(normalGroup1).delayedSlotMovedMillisAt();
    doReturn(true).when(groupManager).moveDelayedSlotToDelayedGroup(normalGroup1);

    // Act
    worker.add(normalGroup1);
    Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // Assert
    assertThat(worker.size()).isEqualTo(0);
    verify(groupManager).moveDelayedSlotToDelayedGroup(normalGroup1);
    verify(groupCleanupWorker).add(normalGroup1);
  }

  @Test
  void add_GivenMultipleNotReadyGroupsTimedOut_ShouldMoveDelayedSlotsToDelayedGroupWithoutWait() {
    // Arrange
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(
            g -> {
              doReturn(false, true).when(g).isReady();
              doReturn(System.currentTimeMillis() - 5).when(g).delayedSlotMovedMillisAt();
              doReturn(true).when(groupManager).moveDelayedSlotToDelayedGroup(g);
            });

    // Act
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(g -> workerWithWait.add(g));
    Uninterruptibles.sleepUninterruptibly(LONG_WAIT_MILLIS * 2, TimeUnit.MILLISECONDS);

    // Assert
    Arrays.asList(normalGroup1, normalGroup2, normalGroup3, normalGroup4)
        .forEach(
            g -> {
              verify(groupManager).moveDelayedSlotToDelayedGroup(g);
              verify(groupCleanupWorker).add(g);
            });
    assertThat(workerWithWait.size()).isEqualTo(0);
  }
}
