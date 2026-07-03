package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.exception.transaction.CrudException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class TransactionContext {

  // The transaction ID
  public final String transactionId;

  // The snapshot of the transaction
  public final Snapshot snapshot;

  // The isolation level of the transaction
  public final Isolation isolation;

  // Whether the transaction is in read-only mode or not.
  public final boolean readOnly;

  // Whether the transaction is in one-operation mode or not. One-operation mode refers to executing
  // a CRUD operation directly through `DistributedTransactionManager` without explicitly beginning
  // a transaction.
  public final boolean oneOperation;

  // Whether a group commit slot was reserved for this transaction at begin time. It is true only
  // when the group commit feature is enabled and a slot was actually reserved (see
  // ConsensusCommitManager.begin()). The rollback and commit paths check this flag to decide
  // whether the reserved slot must be released.
  public final boolean groupCommitSlotReserved;

  // The backup label of the CBRL backup window active for this transaction, or null when none is
  // (redo logging off). Captured at begin from the manager's window state and immutable for the
  // transaction's life, so the whole transaction logs consistently regardless of a mid-flight flip.
  // When set, the commit/abort/group-emit paths record full after-image columns tagged with this
  // label; when null, only primary keys.
  @Nullable public final String backupLabel;

  // A monotonic timestamp (System.nanoTime()) captured when the transaction began, used to enforce
  // the transaction timeout on the commit path. nanoTime, not wall-clock, so a backward clock step
  // cannot make a frozen transaction look un-expired.
  public final long beginAtNanos;

  // The transaction-lifetime bound in millis; a value <= 0 disables the timeout. When enabled, the
  // commit path self-aborts (retryable) a transaction that has run longer than this, bounding how
  // long a pre-backup-flag transaction can drain and killing any transaction frozen across a
  // backup-window transition.
  public final long transactionTimeoutMillis;

  // A list of scanners opened in the transaction
  public final List<ConsensusCommitScanner> scanners = new ArrayList<>();

  // A list of recovery results for asynchronously executed recoveries. These are tracked so that
  // their completion can be awaited before committing the transaction if necessary.
  public final List<RecoveryExecutor.Result> recoveryResults = new CopyOnWriteArrayList<>();

  public TransactionContext(
      String transactionId,
      Snapshot snapshot,
      Isolation isolation,
      boolean readOnly,
      boolean oneOperation,
      boolean groupCommitSlotReserved) {
    this(transactionId, snapshot, isolation, readOnly, oneOperation, groupCommitSlotReserved, null);
  }

  public TransactionContext(
      String transactionId,
      Snapshot snapshot,
      Isolation isolation,
      boolean readOnly,
      boolean oneOperation,
      boolean groupCommitSlotReserved,
      @Nullable String backupLabel) {
    this(
        transactionId,
        snapshot,
        isolation,
        readOnly,
        oneOperation,
        groupCommitSlotReserved,
        backupLabel,
        System.nanoTime(),
        0);
  }

  public TransactionContext(
      String transactionId,
      Snapshot snapshot,
      Isolation isolation,
      boolean readOnly,
      boolean oneOperation,
      boolean groupCommitSlotReserved,
      @Nullable String backupLabel,
      long beginAtNanos,
      long transactionTimeoutMillis) {
    this.transactionId = transactionId;
    this.snapshot = snapshot;
    this.isolation = isolation;
    this.readOnly = readOnly;
    this.oneOperation = oneOperation;
    this.groupCommitSlotReserved = groupCommitSlotReserved;
    this.backupLabel = backupLabel;
    this.beginAtNanos = beginAtNanos;
    this.transactionTimeoutMillis = transactionTimeoutMillis;
  }

  @VisibleForTesting
  TransactionContext(
      String transactionId,
      Snapshot snapshot,
      Isolation isolation,
      boolean readOnly,
      boolean oneOperation) {
    this(transactionId, snapshot, isolation, readOnly, oneOperation, false);
  }

  /**
   * Returns whether the transaction has exceeded its lifetime bound at the given time. Always
   * {@code false} when the timeout is disabled ({@code transactionTimeoutMillis <= 0}).
   *
   * @param nowNanos the current monotonic time from {@link System#nanoTime()}
   * @return {@code true} if the transaction has run longer than {@code transactionTimeoutMillis}
   */
  public boolean isExpired(long nowNanos) {
    return transactionTimeoutMillis > 0
        && nowNanos - beginAtNanos > TimeUnit.MILLISECONDS.toNanos(transactionTimeoutMillis);
  }

  public boolean isSnapshotReadRequired() {
    return isolation != Isolation.READ_COMMITTED;
  }

  public boolean isValidationPossiblyRequired() {
    return isolation == Isolation.SERIALIZABLE;
  }

  public boolean isValidationRequired() {
    // Only SERIALIZABLE isolation level requires validation
    if (isolation != Isolation.SERIALIZABLE) {
      return false;
    }

    // If the scan set is not empty, we need to perform validation
    if (!snapshot.isScanSetEmpty()) {
      return true;
    }

    // If the scanner set is not empty, we need to perform validation
    if (!snapshot.isScannerSetEmpty()) {
      return true;
    }

    // If all the records in the get set are included in the write set or delete set, no validation
    // is required
    return snapshot.getGetSet().stream()
        .anyMatch(
            getSetEntry -> {
              Snapshot.Key key = new Snapshot.Key(getSetEntry.getKey());
              return !snapshot.containsKeyInWriteSet(key) && !snapshot.containsKeyInDeleteSet(key);
            });
  }

  public boolean areAllScannersClosed() {
    return scanners.stream().allMatch(ConsensusCommitScanner::isClosed);
  }

  public void closeScanners() throws CrudException {
    for (ConsensusCommitScanner scanner : scanners) {
      if (!scanner.isClosed()) {
        scanner.close();
      }
    }
  }
}
