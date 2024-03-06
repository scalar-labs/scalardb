package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SlotTest {
  @Mock NormalGroup<String, String, String, String, Integer> parentGroup;
  @Mock DelayedGroup<String, String, String, String, Integer> newParentGroup;

  @Test
  void key_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);

    // Act
    // Assert
    assertThat(slot.key()).isEqualTo("child-key");
  }

  @Test
  void fullKey_GivenArbitraryParentGroup_ShouldReturnParentFullKey() {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);
    doReturn("full-key").when(parentGroup).fullKey(eq("child-key"));

    // Act
    // Assert
    assertThat(slot.fullKey()).isEqualTo("full-key");
  }

  @Test
  void setValue_GivenArbitraryValue_ShouldReturnIt() {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);

    // Act
    slot.setValue(42);

    // Assert
    assertThat(slot.value()).isEqualTo(42);
  }

  @Test
  void waitUntilEmit_GivenNoCompletion_ShouldWait() {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AtomicReference<Exception> exception = new AtomicReference<>();

    // Act
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                slot.waitUntilEmit();
              } catch (GroupCommitException e) {
                exception.set(e);
              }
            });
    executorService.shutdown();

    // Assert
    try {
      future.get(1, TimeUnit.SECONDS);
      fail();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      // Expected
      assertThat(exception.get()).isNull();
    } finally {
      slot.markAsSuccess();
    }
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallMarkAsSuccess_ShouldFinish()
      throws ExecutionException, InterruptedException {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AtomicReference<Exception> exception = new AtomicReference<>();

    // Act
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                slot.waitUntilEmit();
              } catch (GroupCommitException e) {
                exception.set(e);
              }
            });
    executorService.shutdown();

    slot.markAsSuccess();

    // Assert
    future.get();
    assertThat(exception.get()).isNull();
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallMarkAsFail_ShouldFinishAndThrowException()
      throws ExecutionException, InterruptedException {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AtomicReference<Exception> exception = new AtomicReference<>();

    // Act
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                slot.waitUntilEmit();
              } catch (GroupCommitException e) {
                exception.set(e);
              }
            });
    executorService.shutdown();

    slot.markAsFail(new RuntimeException("Hello, world!"));

    // Assert
    future.get();
    assertThat(exception.get()).isInstanceOf(GroupCommitException.class);
    assertThat(exception.get().getCause())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Hello, world!");
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallDelegateTaskToWaiter_ShouldTakeOverTask()
      throws ExecutionException, InterruptedException {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AtomicReference<Exception> exception = new AtomicReference<>();
    AtomicReference<Integer> result = new AtomicReference<>();

    // Act
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                slot.waitUntilEmit();
              } catch (GroupCommitException e) {
                exception.set(e);
              }
            });
    executorService.shutdown();

    slot.delegateTaskToWaiter(() -> result.set(42));

    // Assert
    future.get();
    assertThat(exception.get()).isNull();
    assertThat(result.get()).isEqualTo(42);
  }
}
