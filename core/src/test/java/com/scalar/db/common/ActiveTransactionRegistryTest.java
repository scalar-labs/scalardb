package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ActiveTransactionRegistryTest {

  // Size-based eviction and its disposal handler run asynchronously, so assertions poll up to a
  // generous timeout rather than reading immediately.
  private static void awaitUntil(BooleanSupplier condition) {
    long deadline = System.currentTimeMillis() + 5000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) {
        throw new AssertionError("Condition was not met within the timeout");
      }
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  void add_WhenEvictionDisposalHandlerThrows_ShouldSwallowExceptionAndStillEvict() {
    // Arrange: cap of 1 and a disposal handler that always throws. Non-positive expiration time
    // keeps time-based expiration off so only size-based eviction can fire.
    AtomicInteger disposed = new AtomicInteger();
    ActiveTransactionRegistry<String> registry =
        new ActiveTransactionRegistry<>(
            -1,
            1,
            t -> {
              disposed.incrementAndGet();
              throw new RuntimeException("boom");
            });

    assertThat(registry.add("a", "a")).isTrue();

    // Act: inserting past the cap evicts an entry to make room. Its disposal handler throws, but
    // the
    // registry must swallow it so the admitting caller is unaffected.
    assertThatCode(() -> assertThat(registry.add("b", "b")).isTrue()).doesNotThrowAnyException();

    // Assert: the throwing handler ran for the evicted entry and the registry is back within the
    // cap (exactly one of the two entries remains).
    awaitUntil(() -> disposed.get() == 1);
    long present = 0;
    if (registry.get("a").isPresent()) {
      present++;
    }
    if (registry.get("b").isPresent()) {
      present++;
    }
    assertThat(present).isEqualTo(1);
  }
}
