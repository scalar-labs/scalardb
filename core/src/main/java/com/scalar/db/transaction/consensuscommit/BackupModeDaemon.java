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
 * <p><b>Armed vs. not.</b> The daemon arms only when the {@code backup} table exists at startup.
 * When it does not exist, CBRL is not in use — no window can ever open (opening one writes that
 * table) — so {@link #activeBackupLabel(long)} returns {@code null} and {@code begin()} proceeds
 * normally.
 *
 * <p><b>Fail-closed when armed.</b> A failing read does <b>not</b> advance the last
 * <i>successful</i> read time, so {@code activeBackupLabel} can tell when the flag can no longer be
 * confirmed. When armed, a reading that cannot be confirmed — never successfully read (after one
 * on-demand retry), or older than the staleness bound (e.g. the coordinator is unreachable) — makes
 * {@code activeBackupLabel} throw so {@code begin()} refuses rather than start a transaction that
 * could commit inside a backup window without logging redo.
 */
@ThreadSafe
final class BackupModeDaemon {
  private final Coordinator coordinator;
  private final long checkIntervalMillis;
  private final Logger logger;
  private final LongSupplier clockMillis;
  private final ScheduledExecutorService scheduler;
  private final AtomicReference<BackupFlagReading> lastReading = new AtomicReference<>();
  // Whether CBRL is provisioned here: true iff the backup table existed at startup. When false, no
  // window can ever open (opening one writes that table), so the flag is not consulted and begin()
  // proceeds normally; when true, a reading that cannot be confirmed fails closed.
  private final boolean armed;
  @Nullable private ScheduledFuture<?> scheduledTask;

  // Bound on how long close() waits for an in-flight read to drain before returning.
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

  BackupModeDaemon(Coordinator coordinator, long checkIntervalMillis, boolean armed) {
    this(
        coordinator,
        checkIntervalMillis,
        armed,
        LoggerFactory.getLogger(BackupModeDaemon.class),
        System::currentTimeMillis);
  }

  @VisibleForTesting
  BackupModeDaemon(
      Coordinator coordinator,
      long checkIntervalMillis,
      boolean armed,
      Logger logger,
      LongSupplier clockMillis) {
    this.coordinator = coordinator;
    this.checkIntervalMillis = checkIntervalMillis;
    this.armed = armed;
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
    if (!armed) {
      // CBRL is not provisioned here (no backup table); there is no flag to poll or confirm.
      return;
    }
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
    if (!armed) {
      // CBRL is not in use here (the backup table does not exist), so no window can be open and
      // there
      // is no redo to miss. Proceed with no label.
      return null;
    }
    BackupFlagReading current = lastReading.get();
    if (current == null) {
      // Armed, but the flag has never been confirmed (e.g. the coordinator was unreachable at
      // startup). Try once now; if it still cannot be read, fail closed — a window might be open,
      // and
      // committing without logging redo would be a silent capture loss.
      readAndUpdate();
      current = lastReading.get();
      if (current == null) {
        throw new IllegalStateException(
            "Cannot confirm CBRL backup mode: the backup table could not be read. Failing closed and "
                + "refusing to begin a transaction that might commit inside a backup window without "
                + "logging redo. Retry once the coordinator is reachable again.");
      }
    }
    // A non-positive staleness bound disables freshness enforcement (as the transaction timeout
    // does for <= 0): trust the daemon's periodically refreshed reading without forcing a fresh
    // read.
    if (stalenessBoundMillis > 0) {
      refreshIfStale(stalenessBoundMillis);
      current = lastReading.get();
      if (current == null
          || clockMillis.getAsLong() - current.readAtMillis >= stalenessBoundMillis) {
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

  @VisibleForTesting
  void readAndUpdate() {
    try {
      String label = coordinator.getBackupLabel().orElse(null);
      lastReading.set(new BackupFlagReading(label, clockMillis.getAsLong()));
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

  /**
   * One successful observation of the {@code backup} flag: the open window's label (null if no
   * window is open) and the clock time the read succeeded. The {@link #lastReading} holder is null
   * until the first successful read, so "never read" needs no sentinel value. Only a successful
   * read replaces it, so a run of failing reads lets {@code begin()} detect staleness and fail
   * closed.
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
