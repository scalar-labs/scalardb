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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SlotTest {
  private Slot<String, String, String, String, Integer> slot;
  @Mock NormalGroup<String, String, String, String, Integer> parentGroup;
  @Mock DelayedGroup<String, String, String, String, Integer> newParentGroup;

  @BeforeEach
  void setUp() {
    slot = new Slot<>("child-key", parentGroup);
  }

  @Test
  void key_GivenArbitraryValue_ShouldReturnProperly() {
    // Arrange
    Slot<String, String, String, String, Integer> slot = new Slot<>("child-key", parentGroup);

    // Act
    // Assert
    assertThat(slot.key()).isEqualTo("child-key");
  }

  @Test
  void fullKey_GivenArbitraryParentGroup_ShouldReturnProperly() {
    // Arrange
    doReturn("full-key").when(parentGroup).fullKey(eq("child-key"));

    // Act
    // Assert
    assertThat(slot.fullKey()).isEqualTo("full-key");
  }

  @Test
  void changeParentGroupToDelayedGroup_GivenArbitraryParentGroup_ShouldUseIt() {
    // Arrange
    doReturn("new-full-key").when(newParentGroup).fullKey(eq("child-key"));

    // Act
    slot.changeParentGroupToDelayedGroup(newParentGroup);

    // Assert
    assertThat(slot.fullKey()).isEqualTo("new-full-key");
  }

  @Test
  void setValue_GivenArbitraryValue_ShouldUseIt() {
    // Arrange

    // Act
    slot.setValue(42);

    // Assert
    assertThat(slot.value()).isEqualTo(42);
  }

  @Test
  void isReady_GivenNoValueSet_ShouldReturnFalse() {
    // Arrange

    // Act
    // Assert
    assertThat(slot.isReady()).isFalse();
  }

  @Test
  void isReady_GivenAnyValueSet_ShouldReturnTrue() {
    // Arrange
    slot.setValue(42);

    // Act
    // Assert
    assertThat(slot.isReady()).isTrue();
  }

  @Test
  void waitUntilEmit_GivenNoCompletion_ShouldWait() {
    // Arrange
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
      assertThat(slot.isDone()).isFalse();
    } finally {
      slot.markAsSuccess();
    }
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallMarkAsSuccess_ShouldFinish()
      throws ExecutionException, InterruptedException {
    // Arrange
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
    assertThat(slot.isDone()).isTrue();
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallMarkAsFail_ShouldFinishAndThrowException()
      throws ExecutionException, InterruptedException {
    // Arrange
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
    assertThat(slot.isDone()).isTrue();
  }

  @Timeout(1)
  @Test
  void waitUntilEmit_GivenCallDelegateTaskToWaiter_ShouldTakeOverTask()
      throws ExecutionException, InterruptedException {
    // Arrange
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
    assertThat(slot.isDone()).isTrue();
  }
}
