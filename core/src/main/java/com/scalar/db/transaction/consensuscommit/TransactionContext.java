package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.exception.transaction.CrudException;
import java.util.ArrayList;
import java.util.List;

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

  // A list of scanners opened in the transaction
  public final List<ConsensusCommitScanner> scanners = new ArrayList<>();

  // A list of recovery results performed in the transaction
  public final List<RecoveryExecutor.Result> recoveryResults = new ArrayList<>();

  public TransactionContext(
      String transactionId,
      Snapshot snapshot,
      Isolation isolation,
      boolean readOnly,
      boolean oneOperation) {
    this.transactionId = transactionId;
    this.snapshot = snapshot;
    this.isolation = isolation;
    this.readOnly = readOnly;
    this.oneOperation = oneOperation;
  }

  public boolean isSnapshotReadRequired() {
    return isolation != Isolation.READ_COMMITTED;
  }

  public boolean isValidationRequired() {
    return isolation == Isolation.SERIALIZABLE;
  }

  public boolean isValidationActuallyRequired() {
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
