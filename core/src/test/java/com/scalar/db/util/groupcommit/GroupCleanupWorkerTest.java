package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupCleanupWorkerTest {
  private static final int LONG_WAIT_MILLIS = 500;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup1;
  @Mock private NormalGroup<String, String, String, String, Integer> normalGroup2;
  @Mock private DelayedGroup<String, String, String, String, Integer> delayedGroup1;
  @Mock private DelayedGroup<String, String, String, String, Integer> delayedGroup2;
  @Mock private GroupManager<String, String, String, String, Integer> groupManager;
  private GroupCleanupWorker<String, String, String, String, Integer> worker;
  private GroupCleanupWorker<String, String, String, String, Integer> workerWithWait;

  @BeforeEach
  void setUp() {
    worker = new GroupCleanupWorker<>("test", 10, groupManager, new CurrentTime());
    workerWithWait =
        spy(
            new GroupCleanupWorker<>(
                "long-wait-test", LONG_WAIT_MILLIS, groupManager, new CurrentTime()));
  }

  @AfterEach
  void tearDown() {
    worker.close();
  }

  @Test
  void add_GivenDoneNormalGroup_ShouldRemoveItFromGroupManager() throws InterruptedException {
    // Arrange
    doReturn(true).when(normalGroup1).isDone();
    doReturn(true).when(groupManager).removeGroupFromMap(normalGroup1);

    // Act
    worker.add(normalGroup1);
    TimeUnit.MILLISECONDS.sleep(200);

    // Assert
    assertThat(worker.size()).isEqualTo(0);
    verify(groupManager).removeGroupFromMap(normalGroup1);
  }

  @Test
  void add_GivenNotDoneNormalGroup_ShouldKeepItWithWait() throws InterruptedException {
    // Arrange
    doReturn(false).when(normalGroup1).isDone();

    // Act
    workerWithWait.add(normalGroup1);
    TimeUnit.MILLISECONDS.sleep(LONG_WAIT_MILLIS * 2);

    // Assert
    verify(workerWithWait, atMost(2)).processItem(any());
    assertThat(workerWithWait.size()).isEqualTo(1);
    verify(groupManager, never()).removeGroupFromMap(normalGroup1);
  }

  @Test
  void add_GivenDoneDelayedGroup_ShouldRemoveItFromGroupManager() throws InterruptedException {
    // Arrange
    doReturn(true).when(delayedGroup1).isDone();
    doReturn(true).when(groupManager).removeGroupFromMap(delayedGroup1);

    // Act
    worker.add(delayedGroup1);
    TimeUnit.MILLISECONDS.sleep(200);

    // Assert
    assertThat(worker.size()).isEqualTo(0);
    verify(groupManager).removeGroupFromMap(delayedGroup1);
  }

  @Test
  void add_GivenNotDoneDelayedGroup_ShouldKeepItWithWait() throws InterruptedException {
    // Arrange
    doReturn(false).when(delayedGroup1).isDone();

    // Act
    workerWithWait.add(delayedGroup1);
    TimeUnit.MILLISECONDS.sleep(LONG_WAIT_MILLIS);

    // Assert
    verify(workerWithWait, atMost(2)).processItem(any());
    assertThat(workerWithWait.size()).isEqualTo(1);
    verify(groupManager, never()).removeGroupFromMap(delayedGroup1);
  }

  @Test
  void add_GivenMultipleDoneGroups_ShouldRemoveThemFromGroupManager() throws InterruptedException {
    // Arrange
    Arrays.asList(normalGroup1, normalGroup2, delayedGroup1, delayedGroup2)
        .forEach(
            g -> {
              doReturn(true).when(g).isDone();
              doReturn(true).when(groupManager).removeGroupFromMap(g);
            });

    // Act
    Arrays.asList(normalGroup1, normalGroup2, delayedGroup1, delayedGroup2)
        .forEach(g -> workerWithWait.add(g));
    TimeUnit.MILLISECONDS.sleep(LONG_WAIT_MILLIS * 2);

    // Assert
    Arrays.asList(normalGroup1, normalGroup2, delayedGroup1, delayedGroup2)
        .forEach(g -> verify(groupManager).removeGroupFromMap(g));
    assertThat(workerWithWait.size()).isEqualTo(0);
  }
}
