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
 * Per-{@link ConsensusCommitManager} daemon that caches whether a CBRL backup window is open. Every
 * {@code checkIntervalMillis} it does a {@code LINEARIZABLE} point get of the single fixed-key
 * {@code backup} record and refreshes the cached label. Because each process only has the ScalarDB
 * library (there is no control plane to push a flag to every process), each reads the record
 * through this cache, and {@code begin()} consults it to decide the transaction's backup label.
 * There is at most one {@code backup} record (a fixed partition key, no clustering key), and its
 * presence is the open window, so the cache is a single nullable label.
 *
 * <p>The decision comes only from reading that one record: a successful get with the record present
 * means a window is open, and with it absent means none. If the record cannot be read (any lookup
 * failure, including a missing {@code backup} table), the reading is unconfirmed and {@link
 * #activeBackupLabel(long)} refuses to start the transaction (throws) rather than start one that
 * could commit inside a window without logging redo. The {@code backup} table must therefore exist
 * before ScalarDB is used. A failing read does not advance the last successful read time, so a
 * cache older than the staleness bound is likewise treated as unconfirmed.
 */
@ThreadSafe
final class BackupModeDaemon {
  private final Coordinator coordinator;
  private final long checkIntervalMillis;
  private final Logger logger;
  private final LongSupplier clockMillis;
  private final ScheduledExecutorService scheduler;
  private final AtomicReference<BackupFlagReading> lastReading = new AtomicReference<>();
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
   * Returns the label of the open backup window for a transaction beginning now, or {@code null}
   * when no window is open. Forces a synchronous read first when the cache has never been populated
   * or is older than {@code stalenessBoundMillis}, and <b>refuses</b> (throws) if the flag still
   * cannot be confirmed, so {@code begin()} never starts a transaction that might commit inside a
   * window without logging redo.
   */
  @Nullable
  String activeBackupLabel(long stalenessBoundMillis) {
    BackupFlagReading current = lastReading.get();
    if (current == null) {
      // The flag has never been read successfully (the coordinator was unreachable at startup, or
      // the backup table is missing). Try once now; if it still cannot be read, refuse to start the
      // transaction: a window might be open, and committing without logging redo would lose it.
      readAndUpdate();
      current = lastReading.get();
      if (current == null) {
        throw new IllegalStateException(
            "Cannot confirm CBRL backup mode: the backup flag could not be read. Refusing to start a "
                + "transaction that might commit inside a backup window without logging redo. The "
                + "backup table must exist before ScalarDB is used; retry once the coordinator is "
                + "reachable again.");
      }
    }
    // A non-positive staleness bound disables freshness enforcement: trust the periodically
    // refreshed reading without forcing a fresh read.
    if (stalenessBoundMillis > 0) {
      refreshIfStale(stalenessBoundMillis);
      current = lastReading.get();
      if (current == null
          || clockMillis.getAsLong() - current.readAtMillis >= stalenessBoundMillis) {
        throw new IllegalStateException(
            "Cannot confirm CBRL backup mode: the last successful read of the backup flag is older "
                + "than the staleness bound ("
                + stalenessBoundMillis
                + " ms). Refusing to start a transaction that might commit inside a backup window "
                + "without logging redo. Retry once the coordinator is reachable again.");
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
    BackupFlagReading current = lastReading.get();
    return current == null ? null : current.label;
  }

  @VisibleForTesting
  long lastSuccessfulReadAtMillis() {
    BackupFlagReading current = lastReading.get();
    return current == null ? 0L : current.readAtMillis;
  }

  private synchronized void refreshIfStale(long stalenessBoundMillis) {
    BackupFlagReading current = lastReading.get();
    if (current == null || clockMillis.getAsLong() - current.readAtMillis >= stalenessBoundMillis) {
      readAndUpdate();
    }
  }

  // Synchronized so all reads (the scheduled poll, refreshNow, and begin()'s stale/first-read
  // refresh) serialize: each does its LINEARIZABLE get and publishes the result inside the lock, so
  // a slow read cannot publish a stale observation over a newer one that already completed.
  // Reentrant
  // with refreshIfStale, which is also synchronized on this and calls into here.
  @VisibleForTesting
  synchronized void readAndUpdate() {
    try {
      String label = coordinator.getBackupLabel().orElse(null);
      lastReading.set(new BackupFlagReading(label, clockMillis.getAsLong()));
    } catch (CoordinatorException | RuntimeException e) {
      // Keep the last successful reading and its time so activeBackupLabel's staleness check keeps
      // firing, instead of treating a failing read as fresh. The next poll retries.
      logger.debug("Failed to read the backup flag; keeping the last successful reading", e);
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

  /**
   * One reading of the {@code backup} flag: the open window's label (null if no window is open) and
   * the clock time the read succeeded. The {@link #lastReading} holder is null until the first
   * successful read, and only a successful read replaces it, so a run of failing reads leaves the
   * last reading (and its time) in place for the staleness check.
   */
  private static final class BackupFlagReading {
    @Nullable private final String label;
    private final long readAtMillis;

    private BackupFlagReading(@Nullable String label, long readAtMillis) {
      this.label = label;
      this.readAtMillis = readAtMillis;
    }
  }
}
