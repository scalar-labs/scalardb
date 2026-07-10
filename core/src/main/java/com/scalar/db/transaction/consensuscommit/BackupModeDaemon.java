package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-{@link ConsensusCommitManager} daemon that keeps a local cache of the currently open CBRL
 * backup window. Every {@code checkIntervalMillis} it does a single-row {@code LINEARIZABLE} point
 * get of the {@code backup} coordinator table and refreshes the cached label. Because embedded Core
 * cannot push a flag to every process, each process reads the flag from this cache; {@code begin()}
 * consults it to decide the transaction's backup label. The {@code backup} table holds at most one
 * row (its presence is the open window), so the cache is a single nullable label.
 *
 * <p><b>Fail-closed on staleness.</b> A failing read does <b>not</b> advance the cache's last
 * <i>successful</i> read time, so {@link #activeBackupLabel(long)} can tell when the flag can no
 * longer be confirmed. Once the daemon has read the flag at least once, if the last successful read
 * is older than the staleness bound (e.g. the coordinator is unreachable), {@code
 * activeBackupLabel} throws so {@code begin()} refuses rather than start a transaction that could
 * commit inside a backup window without logging redo. Before the first successful read — when the
 * {@code backup} table may simply not exist (CBRL not in use) — it returns {@code null}: there is
 * no window it could miss, and a transaction cannot commit without the coordinator anyway.
 */
@ThreadSafe
final class BackupModeDaemon {
  private final Coordinator coordinator;
  private final long checkIntervalMillis;
  private final Logger logger;
  private final LongSupplier clockMillis;
  private final ScheduledExecutorService scheduler;
  private final AtomicReference<Cache> cache = new AtomicReference<>(Cache.empty());
  @Nullable private ScheduledFuture<?> scheduledTask;

  // Bound on how long close() waits for an in-flight read to drain before returning.
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

  BackupModeDaemon(Coordinator coordinator, long checkIntervalMillis) {
    this(
        coordinator,
        checkIntervalMillis,
        LoggerFactory.getLogger(BackupModeDaemon.class),
        System::currentTimeMillis);
  }

  @VisibleForTesting
  BackupModeDaemon(
      Coordinator coordinator, long checkIntervalMillis, Logger logger, LongSupplier clockMillis) {
    this.coordinator = coordinator;
    this.checkIntervalMillis = checkIntervalMillis;
    this.logger = logger;
    this.clockMillis = clockMillis;
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "cbrl-backup-mode-daemon");
              thread.setDaemon(true);
              return thread;
            });
  }

  /**
   * Runs one synchronous read so the cache is populated before the manager serves transactions,
   * then schedules the periodic refresh.
   */
  void start() {
    readAndUpdate();
    scheduledTask =
        scheduler.scheduleWithFixedDelay(
            this::readAndUpdate, checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Returns the active backup label for a transaction beginning now, or {@code null} when no window
   * is open. Forces a synchronous refresh first if the cache is older than {@code
   * stalenessBoundMillis}, and <b>fails closed</b> (throws) if the flag still cannot be confirmed
   * once the daemon has succeeded at least once — see the class comment.
   */
  @Nullable
  String activeBackupLabel(long stalenessBoundMillis) {
    Cache current = cache.get();
    if (current.lastSuccessfulReadAtMillis == 0L) {
      // Never confirmed the flag (the backup table may not exist / CBRL is not in use, or the
      // coordinator has been unreachable since startup). There is no window to miss, and a
      // transaction cannot commit without the coordinator anyway, so proceed with no label.
      return null;
    }
    // A non-positive staleness bound disables freshness enforcement (as the transaction timeout
    // does
    // for <= 0): trust the daemon's periodically refreshed cache without forcing a read or failing
    // closed.
    if (stalenessBoundMillis > 0) {
      refreshIfStale(stalenessBoundMillis);
      current = cache.get();
      if (clockMillis.getAsLong() - current.lastSuccessfulReadAtMillis >= stalenessBoundMillis) {
        throw new IllegalStateException(
            "Cannot confirm CBRL backup mode: the last successful read of the backup table is older "
                + "than the staleness bound ("
                + stalenessBoundMillis
                + " ms). Failing closed and refusing to begin a transaction that might commit inside "
                + "a backup window without logging redo. Retry once the coordinator is reachable "
                + "again.");
      }
    }
    return current.label;
  }

  /** Forces a synchronous refresh of the cache (used right after a local enter/quit transition). */
  void refreshNow() {
    readAndUpdate();
  }

  @VisibleForTesting
  @Nullable
  String activeLabel() {
    return cache.get().label;
  }

  @VisibleForTesting
  long lastSuccessfulReadAtMillis() {
    return cache.get().lastSuccessfulReadAtMillis;
  }

  private synchronized void refreshIfStale(long stalenessBoundMillis) {
    if (clockMillis.getAsLong() - cache.get().lastSuccessfulReadAtMillis >= stalenessBoundMillis) {
      readAndUpdate();
    }
  }

  @VisibleForTesting
  void readAndUpdate() {
    try {
      String label = coordinator.getBackupLabel().orElse(null);
      cache.set(new Cache(label, clockMillis.getAsLong()));
    } catch (CoordinatorException | RuntimeException e) {
      // Keep the last-known label AND the last SUCCESSFUL read time — do NOT advance freshness on
      // failure. That way begin()'s staleness check keeps firing and fails closed once the last
      // successful read is too old, instead of treating a persistently failing read as fresh.
      logger.debug(
          "Failed to read the backup table; keeping the last successful backup-label cache", e);
    }
  }

  void close() {
    if (scheduledTask != null) {
      scheduledTask.cancel(true);
    }
    scheduler.shutdownNow();
    try {
      // An in-flight read does not honor interruption (storage calls don't), so wait for it to
      // finish before the caller tears storage down.
      if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        logger.warn("BackupModeDaemon read did not terminate within the shutdown timeout.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class Cache {
    // The label of the open backup window, or null if no window is open.
    @Nullable private final String label;
    // The clock time of the last SUCCESSFUL read (0 = never succeeded). Only a successful read
    // advances it, so a run of failing reads lets begin() detect staleness and fail closed.
    private final long lastSuccessfulReadAtMillis;

    private Cache(@Nullable String label, long lastSuccessfulReadAtMillis) {
      this.label = label;
      this.lastSuccessfulReadAtMillis = lastSuccessfulReadAtMillis;
    }

    private static Cache empty() {
      return new Cache(null, 0L);
    }
  }
}
