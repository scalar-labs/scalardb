package com.scalar.db.util.groupcommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.util.groupcommit.BackgroundWorker.RetryMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BackgroundWorkerTest {
  private static class TestableBackgroundWorker extends BackgroundWorker<String> {
    private final Function<String, Boolean> func;

    TestableBackgroundWorker(
        int timeoutCheckIntervalMillis, RetryMode retryMode, Function<String, Boolean> func) {
      super("test", timeoutCheckIntervalMillis, retryMode);
      this.func = func;
    }

    @Override
    BlockingQueue<String> createQueue() {
      return new LinkedBlockingQueue<>();
    }

    @Override
    boolean processItem(String item) {
      return func.apply(item);
    }
  }

  @Test
  void add_GivenReadyItem_ShouldAddAndProcessAndRemoveItWithoutWait() {
    // Arrange
    List<String> processedItems = new ArrayList<>();
    try (TestableBackgroundWorker worker =
        new TestableBackgroundWorker(
            200,
            RetryMode.KEEP_AT_HEAD,
            item -> {
              processedItems.add(item);
              return true;
            })) {

      // Act
      worker.add("item-1");
      worker.add("item-2");
      worker.add("item-3");
      worker.add("item-4");
      worker.add("item-5");
      Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

      // Assert

      // If the worker waited between each item, all the items shouldn't have been handled.
      assertThat(processedItems.size()).isEqualTo(5);
      assertThat(processedItems.get(0)).isEqualTo("item-1");
      assertThat(processedItems.get(1)).isEqualTo("item-2");
      assertThat(processedItems.get(2)).isEqualTo("item-3");
      assertThat(processedItems.get(3)).isEqualTo("item-4");
      assertThat(processedItems.get(4)).isEqualTo("item-5");
    }
  }

  @Test
  void add_GivenNotReadyItemWithKeepAtHead_ShouldAddAndKeepItWithWait() {
    // Arrange
    List<String> processedItems = new ArrayList<>();
    try (TestableBackgroundWorker worker =
        new TestableBackgroundWorker(
            200,
            RetryMode.KEEP_AT_HEAD,
            item -> {
              processedItems.add(item);
              return false;
            })) {
      // Act
      worker.add("item-1");
      worker.add("item-2");
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

      // Assert
      assertThat(processedItems.size()).isGreaterThan(2);
      // If the worker didn't wait between each item, too many retries should have occurred.
      assertThat(processedItems.size()).isLessThan(10);
      assertThat(processedItems.get(0)).isEqualTo("item-1");
      assertThat(processedItems.get(1)).isEqualTo("item-1");
    }
  }

  @Test
  void add_GivenNotReadyItemWithMoveToTail_ShouldAddAndKeepIt() {
    // Arrange
    List<String> processedItems = new ArrayList<>();
    try (TestableBackgroundWorker worker =
        new TestableBackgroundWorker(
            200,
            RetryMode.RE_ENQUEUE,
            item -> {
              processedItems.add(item);
              return false;
            })) {
      // Act
      worker.add("item-1");
      worker.add("item-2");
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

      // Assert
      assertThat(processedItems.size()).isGreaterThan(2);
      // If the worker didn't wait between each item, too many retries should have occurred.
      assertThat(processedItems.size()).isLessThan(10);
      assertThat(processedItems.get(0)).isEqualTo("item-1");
      assertThat(processedItems.get(1)).isEqualTo("item-2");
    }
  }

  @Test
  void add_GivenFailingItem_ShouldRetryItWithWait() {
    // Arrange
    List<String> processedItems = new ArrayList<>();
    try (TestableBackgroundWorker worker =
        new TestableBackgroundWorker(
            200,
            RetryMode.KEEP_AT_HEAD,
            item -> {
              processedItems.add(item);
              throw new RuntimeException("Something is wrong");
            })) {
      // Act
      worker.add("item-1");
      worker.add("item-2");
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

      // Assert
      assertThat(processedItems.size()).isGreaterThan(2);
      // If the worker didn't wait after the failure, too many retries should have occurred.
      assertThat(processedItems.size()).isLessThan(10);
      assertThat(processedItems.get(0)).isEqualTo("item-1");
      assertThat(processedItems.get(1)).isEqualTo("item-1");
    }
  }
}
