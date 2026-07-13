package com.scalar.db.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A thread-safe map whose entries are kept alive only while they stay "active" and are removed
 * automatically when they go stale or when the map grows too large. It is a thin adapter over a <a
 * href="https://github.com/ben-manes/caffeine">Caffeine</a> cache.
 *
 * <p>Entries are reclaimed by two independent mechanisms:
 *
 * <ul>
 *   <li><b>Time-based (idle) expiration.</b> When {@code valueLifetimeMillis} is positive, an entry
 *       that is not accessed (via {@link #get(Object)}, {@link #updateExpirationTime(Object)}, or a
 *       write) for that long is removed and the expiration handler is invoked. Expiration is
 *       proactive: a scheduler removes idle entries promptly even when the map is otherwise
 *       untouched, so an abandoned entry does not linger until the next access. A non-positive
 *       {@code valueLifetimeMillis} disables idle expiration.
 *   <li><b>Size-based eviction.</b> When {@code maxSize} is positive and an insertion pushes the
 *       map beyond it, Caffeine evicts an entry (recency- and frequency-aware, Window-TinyLFU) and
 *       invokes the eviction handler. A frequently-touched entry is preferentially retained over a
 *       cold/abandoned one. A non-positive {@code maxSize} leaves the map unbounded.
 * </ul>
 *
 * <p><b>A successful insert is not a retention guarantee.</b> Inserts report success synchronously,
 * but size-based eviction is decided asynchronously afterward, so under sustained pressure at
 * {@code maxSize} a freshly-inserted entry can be evicted moments later. Callers must not assume an
 * inserted entry is, or will remain, present.
 *
 * <p>Reclamation handlers run on a dedicated fixed-size daemon executor, outside any internal lock,
 * so a slow or blocking handler does not block concurrent admissions. Explicit {@link
 * #remove(Object)} calls and replacements do not invoke either handler.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@ThreadSafe
public class ActiveExpiringMap<K, V> {
  // Lets Caffeine proactively expire idle entries even when the cache is not being accessed. On
  // Java 8, Scheduler.systemScheduler() is unavailable, so we back the scheduler with our own
  // daemon executor. A single shared thread suffices because it only triggers short maintenance
  // tasks; the actual removal/handler work runs on the cache's removal executor.
  private static final Scheduler PROACTIVE_SCHEDULER =
      Scheduler.forScheduledExecutorService(newDaemonScheduler());

  // Runs the reclamation handlers (idle-expiry and size-eviction). Disposal work can block on
  // network/storage I/O (e.g. rolling a transaction back or releasing its context), so it must not
  // run on ForkJoinPool.commonPool() — Caffeine's default removal executor — where a burst of
  // blocking disposals could starve unrelated common-pool work. Threads are daemon. The fixed
  // thread
  // count bounds how many disposals run concurrently, throttling a mass reclamation to at most that
  // many concurrent backend calls rather than flooding the backend; any excess queues (the queue is
  // unbounded, so no disposal is dropped) and drains as threads free up.
  private static final Executor DISPOSAL_EXECUTOR = newDaemonDisposalExecutor();

  private final Cache<K, V> cache;

  private static ScheduledExecutorService newDaemonScheduler() {
    ThreadFactory threadFactory =
        r -> {
          Thread thread = new Thread(r, "active-expiring-map-scheduler");
          thread.setDaemon(true);
          return thread;
        };
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  private static ExecutorService newDaemonDisposalExecutor() {
    ThreadFactory threadFactory =
        r -> {
          Thread thread = new Thread(r, "active-expiring-map-disposal");
          thread.setDaemon(true);
          return thread;
        };
    int poolSize = Math.max(1, Runtime.getRuntime().availableProcessors());
    return Executors.newFixedThreadPool(poolSize, threadFactory);
  }

  /**
   * Creates a time-expiring map with no size bound.
   *
   * @param valueLifetimeMillis the idle time-to-live; an entry not accessed for this long is
   *     removed. Non-positive disables idle expiration.
   * @param valueExpirationHandler invoked for each entry removed by idle expiration
   */
  public ActiveExpiringMap(long valueLifetimeMillis, BiConsumer<K, V> valueExpirationHandler) {
    this(valueLifetimeMillis, 0, valueExpirationHandler, (k, v) -> {});
  }

  /**
   * Creates a time-expiring map.
   *
   * @param valueLifetimeMillis the idle time-to-live; an entry not accessed for this long is
   *     removed. Non-positive disables idle expiration.
   * @param maxSize the maximum number of entries; when exceeded on insertion an entry is evicted to
   *     make room. Non-positive means unbounded.
   * @param valueExpirationHandler invoked for each entry removed by idle expiration
   * @param valueEvictionHandler invoked for each entry evicted due to {@code maxSize}
   */
  public ActiveExpiringMap(
      long valueLifetimeMillis,
      int maxSize,
      BiConsumer<K, V> valueExpirationHandler,
      BiConsumer<K, V> valueEvictionHandler) {
    this(valueLifetimeMillis, maxSize, valueExpirationHandler, valueEvictionHandler, null, null);
  }

  @VisibleForTesting
  ActiveExpiringMap(
      long valueLifetimeMillis,
      int maxSize,
      BiConsumer<K, V> valueExpirationHandler,
      BiConsumer<K, V> valueEvictionHandler,
      @Nullable Ticker ticker,
      @Nullable Executor executor) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    // Run reclamation handlers off the dedicated disposal pool by default; tests inject a
    // same-thread executor so removals (and their handlers) settle synchronously.
    builder = builder.executor(executor != null ? executor : DISPOSAL_EXECUTOR);
    if (ticker != null) {
      builder = builder.ticker(ticker);
    }
    if (valueLifetimeMillis > 0) {
      builder = builder.expireAfterAccess(valueLifetimeMillis, TimeUnit.MILLISECONDS);
      // The proactive scheduler reaps idle entries against the real clock. An injected (fake)
      // ticker drives time manually, so attaching the scheduler would only add nondeterministic
      // interference; attach it only under the real clock.
      if (ticker == null) {
        builder = builder.scheduler(PROACTIVE_SCHEDULER);
      }
    }
    if (maxSize > 0) {
      builder = builder.maximumSize(maxSize);
    }
    this.cache =
        builder
            .<K, V>removalListener(
                (key, value, cause) -> {
                  if (key == null || value == null) {
                    return;
                  }
                  if (cause == RemovalCause.EXPIRED) {
                    valueExpirationHandler.accept(key, value);
                  } else if (cause == RemovalCause.SIZE) {
                    valueEvictionHandler.accept(key, value);
                  }
                  // EXPLICIT (remove) and REPLACED (overwrite) are not reclamation events, so no
                  // handler runs for them.
                })
            .build();
  }

  public Optional<V> get(K key) {
    // A read counts as access under expireAfterAccess, so it refreshes the idle timer (and marks
    // the entry as recently used, making it less likely to be size-evicted).
    return Optional.ofNullable(cache.getIfPresent(key));
  }

  public Optional<V> put(K key, V value) {
    return Optional.ofNullable(cache.asMap().put(key, value));
  }

  public Optional<V> putIfAbsent(K key, V value) {
    return Optional.ofNullable(cache.asMap().putIfAbsent(key, value));
  }

  public Optional<V> remove(K key) {
    return Optional.ofNullable(cache.asMap().remove(key));
  }

  /**
   * Refreshes the idle timer of an entry, treating it as recently used. Does nothing if no entry
   * with the given key is present.
   *
   * @param key the key
   */
  public void updateExpirationTime(K key) {
    cache.getIfPresent(key);
  }

  /** Performs any pending maintenance (expiration/eviction) synchronously. For tests only. */
  @VisibleForTesting
  void cleanUp() {
    cache.cleanUp();
  }
}
