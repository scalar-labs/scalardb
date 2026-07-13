package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

public class ActiveExpiringMapTest {

  // A same-thread executor so Caffeine runs maintenance and removal handlers synchronously within
  // the calling operation (e.g. cleanUp()); combined with a fake ticker this makes
  // expiration/eviction assertions deterministic instead of racing an async removal executor.
  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  // Handlers run asynchronously on the cache's removal executor, so assertions on them poll up to a
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
  public void getAndPut_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap = new ActiveExpiringMap<>(0, (k, v) -> {});

    // Act Assert
    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    assertThat(activeExpiringMap.get("k1")).isEqualTo(Optional.of("v1"));
    assertThat(activeExpiringMap.get("k2")).isEqualTo(Optional.of("v2"));
    assertThat(activeExpiringMap.get("k3")).isEqualTo(Optional.of("v3"));
    assertThat(activeExpiringMap.get("k4")).isEqualTo(Optional.empty());
  }

  @Test
  public void put_WhenReplacingExistingKey_ShouldReturnPreviousValue() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap = new ActiveExpiringMap<>(0, (k, v) -> {});
    activeExpiringMap.put("k1", "v1");

    // Act
    Optional<String> previous = activeExpiringMap.put("k1", "v1-new");

    // Assert
    assertThat(previous).hasValue("v1");
    assertThat(activeExpiringMap.get("k1")).hasValue("v1-new");
  }

  @Test
  public void putIfAbsent_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap = new ActiveExpiringMap<>(0, (k, v) -> {});
    activeExpiringMap.put("k1", "v1");

    // Act
    Optional<String> actual1 = activeExpiringMap.putIfAbsent("k1", "v2");
    Optional<String> actual2 = activeExpiringMap.putIfAbsent("k2", "v2");

    // Assert
    assertThat(actual1).hasValue("v1");
    assertThat(actual2).isEmpty();
    assertThat(activeExpiringMap.get("k1")).isEqualTo(Optional.of("v1"));
    assertThat(activeExpiringMap.get("k2")).isEqualTo(Optional.of("v2"));
  }

  @Test
  public void remove_ShouldBehaveCorrectlyAndNotFireHandlers() {
    // Arrange: remove() is an explicit removal (e.g. commit/rollback deregistering), so it must not
    // fire either the expiration or the eviction handler.
    Set<String> disposed = ConcurrentHashMap.newKeySet();
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> disposed.add(k), (k, v) -> disposed.add(k));
    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    // Act
    Optional<String> actual1 = activeExpiringMap.remove("k2");
    Optional<String> actual2 = activeExpiringMap.remove("k4");

    // Assert
    assertThat(actual1).hasValue("v2");
    assertThat(actual2).isEmpty();
    assertThat(activeExpiringMap.get("k1")).hasValue("v1");
    assertThat(activeExpiringMap.get("k2")).isEmpty();
    assertThat(activeExpiringMap.get("k3")).hasValue("v3");
    // An explicit remove is not a reclamation event.
    assertThat(disposed).isEmpty();
  }

  @Test
  public void put_WhenReplacingExistingKey_ShouldNotFireHandlers() {
    // Arrange: a replacement is not a reclamation event, so neither handler runs.
    Set<String> disposed = ConcurrentHashMap.newKeySet();
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 2, (k, v) -> disposed.add(k), (k, v) -> disposed.add(k));
    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");

    // Act
    activeExpiringMap.put("k1", "v1-new");
    activeExpiringMap.cleanUp();

    // Assert
    assertThat(disposed).isEmpty();
    assertThat(activeExpiringMap.get("k1")).hasValue("v1-new");
    assertThat(activeExpiringMap.get("k2")).hasValue("v2");
  }

  @Test
  public void idleEntry_ShouldBeProactivelyExpired_WithoutAnyAccess() {
    // Arrange: a 200ms idle lifetime. This asserts the reason ActiveExpiringMap exists over a plain
    // lazy cache: an idle entry is reaped (and its expiration handler fired) proactively, without
    // any subsequent access to trigger maintenance.
    Set<String> expired = ConcurrentHashMap.newKeySet();
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(200, (k, v) -> expired.add(k));
    activeExpiringMap.put("idle", "v");

    // Act: do not touch or access the entry at all.

    // Assert: it is expired proactively.
    awaitUntil(() -> expired.contains("idle"));
    assertThat(activeExpiringMap.get("idle")).isEmpty();
  }

  @Test
  public void updateExpirationTime_ShouldPreventIdleExpiration_ButIdleEntryStillExpires() {
    // Arrange: a 300ms idle lifetime driven by a fake ticker, so the expiration decision is
    // deterministic and does not depend on wall-clock sleeps. Refreshing an entry within the
    // lifetime keeps it alive; an untouched entry expires.
    FakeTicker ticker = new FakeTicker();
    Set<String> expired = ConcurrentHashMap.newKeySet();
    // A fake ticker plus a same-thread executor makes this fully deterministic: cleanUp() below
    // both decides expiration (fake clock) and runs the handler inline (direct executor), so no
    // wall-clock polling is needed.
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(
            300, 0, (k, v) -> expired.add(k), (k, v) -> {}, ticker, DIRECT_EXECUTOR);
    activeExpiringMap.put("touched", "v");
    activeExpiringMap.put("idle", "v");

    // Act: advance within the lifetime and refresh "touched"; never touch "idle". After a second
    // advance, "idle" has been idle 400ms (> 300, expired) while "touched" has been idle 200ms
    // (< 300, alive).
    ticker.advance(200, TimeUnit.MILLISECONDS);
    activeExpiringMap.updateExpirationTime("touched");
    ticker.advance(200, TimeUnit.MILLISECONDS);
    activeExpiringMap.cleanUp();

    // Assert: the idle value expired and fired its handler; the refreshed value survives.
    assertThat(expired).containsExactly("idle");
    assertThat(activeExpiringMap.get("touched")).hasValue("v");
  }

  @Test
  public void put_WhenOverMaxSize_ShouldEvictViaEvictionHandler_NotExpirationHandler() {
    // Arrange: capacity 2, no idle expiration (lifetime 0). Eviction and expiration use distinct
    // handlers so the test can tell a cap-eviction apart from a timeout.
    Set<String> expired = ConcurrentHashMap.newKeySet();
    Set<String> evicted = ConcurrentHashMap.newKeySet();
    // A same-thread executor runs the eviction inline during cleanUp(), so the assertion is
    // deterministic rather than polling an async removal executor.
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(
            0, 2, (k, v) -> expired.add(k), (k, v) -> evicted.add(k), null, DIRECT_EXECUTOR);

    // Act: insert three distinct keys, exceeding the cap.
    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");
    activeExpiringMap.cleanUp();

    // Assert: exactly one entry was evicted via the eviction handler (not the expiration handler),
    // and the map settled back to its cap of two.
    assertThat(evicted).hasSize(1);
    assertThat(expired).isEmpty();
    long present = 0;
    if (activeExpiringMap.get("k1").isPresent()) {
      present++;
    }
    if (activeExpiringMap.get("k2").isPresent()) {
      present++;
    }
    if (activeExpiringMap.get("k3").isPresent()) {
      present++;
    }
    assertThat(present).isEqualTo(2);
  }

  @Test
  public void put_WhenMaxSizeNonPositive_ShouldNotEvict() {
    // Arrange: a non-positive maxSize means unbounded.
    Set<String> evicted = ConcurrentHashMap.newKeySet();
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {}, (k, v) -> evicted.add(k));

    // Act
    for (int i = 0; i < 1000; i++) {
      activeExpiringMap.put("k" + i, "v" + i);
    }
    activeExpiringMap.cleanUp();

    // Assert: nothing evicted; every entry is still present.
    assertThat(evicted).isEmpty();
    for (int i = 0; i < 1000; i++) {
      assertThat(activeExpiringMap.get("k" + i)).hasValue("v" + i);
    }
  }

  /** A manually-advanced time source so time-based expiration can be tested deterministically. */
  private static final class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
      return nanos.get();
    }

    void advance(long duration, TimeUnit unit) {
      nanos.addAndGet(unit.toNanos(duration));
    }
  }
}
