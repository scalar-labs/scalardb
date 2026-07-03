package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
 * backup windows. Every {@code checkIntervalMillis} it runs a single-partition {@code LINEARIZABLE}
 * scan of the {@code backup} coordinator table and refreshes the cache of {@code BACKING_UP}
 * labels. Because embedded Core cannot push a flag to every process, each process reads the flag
 * from this cache; {@code begin()} consults it to decide the transaction's backup label.
 *
 * <p>Under the single-window assumption a scan returns at most one {@code BACKING_UP} label; if it
 * ever returns more than one, the daemon logs a warning because the single-window invariant is
 * broken.
 *
 * <p><b>Fail-closed on staleness.</b> A failing scan does <b>not</b> advance the cache's last
 * <i>successful</i> read time, so {@link #activeBackupLabel(long)} can tell when the flag can no
 * longer be confirmed. Once the daemon has read the flag at least once, if the last successful scan
 * is older than the staleness bound (e.g. the coordinator is unreachable), {@code
 * activeBackupLabel} throws so {@code begin()} refuses rather than start a transaction that could
 * commit inside a backup window without logging redo. Before the first successful scan — when the
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
   * Runs one synchronous scan so the cache is populated before the manager serves transactions,
   * then schedules the periodic refresh.
   */
  void start() {
    scanAndUpdate();
    scheduledTask =
        scheduler.scheduleWithFixedDelay(
            this::scanAndUpdate, checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);
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
    // for <= 0): trust the daemon's periodically refreshed cache without forcing a scan or failing
    // closed.
    if (stalenessBoundMillis > 0) {
      refreshIfStale(stalenessBoundMillis);
      current = cache.get();
      if (clockMillis.getAsLong() - current.lastSuccessfulReadAtMillis >= stalenessBoundMillis) {
        throw new IllegalStateException(
            "Cannot confirm CBRL backup mode: the last successful scan of the backup table is older "
                + "than the staleness bound ("
                + stalenessBoundMillis
                + " ms). Failing closed and refusing to begin a transaction that might commit inside "
                + "a backup window without logging redo. Retry once the coordinator is reachable "
                + "again.");
      }
    }
    Set<String> labels = current.labels;
    return labels.isEmpty() ? null : labels.iterator().next();
  }

  /** Forces a synchronous refresh of the cache (used right after a local enter/quit transition). */
  void refreshNow() {
    scanAndUpdate();
  }

  @VisibleForTesting
  Set<String> backingUpLabels() {
    return cache.get().labels;
  }

  @VisibleForTesting
  long lastSuccessfulReadAtMillis() {
    return cache.get().lastSuccessfulReadAtMillis;
  }

  private synchronized void refreshIfStale(long stalenessBoundMillis) {
    if (clockMillis.getAsLong() - cache.get().lastSuccessfulReadAtMillis >= stalenessBoundMillis) {
      scanAndUpdate();
    }
  }

  @VisibleForTesting
  void scanAndUpdate() {
    try {
      List<String> labels = coordinator.scanBackingUpLabels();
      if (labels.size() > 1) {
        logger.warn(
            "The backup table has more than one BACKING_UP label, which breaks the single-window "
                + "invariant. Labels: {}",
            labels);
      }
      cache.set(new Cache(new LinkedHashSet<>(labels), clockMillis.getAsLong()));
    } catch (CoordinatorException | RuntimeException e) {
      // Keep the last-known labels AND the last SUCCESSFUL read time — do NOT advance freshness on
      // failure. That way begin()'s staleness check keeps firing and fails closed once the last
      // successful read is too old, instead of treating a persistently failing scan as fresh.
      logger.debug(
          "Failed to scan the backup table; keeping the last successful backup-label cache", e);
    }
  }

  void close() {
    if (scheduledTask != null) {
      scheduledTask.cancel(true);
    }
    scheduler.shutdownNow();
  }

  private static final class Cache {
    private final Set<String> labels;
    // The clock time of the last SUCCESSFUL scan (0 = never succeeded). Only a successful scan
    // advances it, so a run of failing scans lets begin() detect staleness and fail closed.
    private final long lastSuccessfulReadAtMillis;

    private Cache(Set<String> labels, long lastSuccessfulReadAtMillis) {
      this.labels = Collections.unmodifiableSet(labels);
      this.lastSuccessfulReadAtMillis = lastSuccessfulReadAtMillis;
    }

    private static Cache empty() {
      return new Cache(Collections.emptySet(), 0L);
    }
  }
}
