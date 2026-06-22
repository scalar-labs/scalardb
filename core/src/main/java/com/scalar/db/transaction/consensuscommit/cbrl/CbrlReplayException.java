package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Thrown when CBRL chain replay detects a structural anomaly it cannot safely resolve — a fork: two
 * ops on one key sharing a {@code prevTxId}, which serializable commit cannot produce. Replay fails
 * loud rather than silently picking a branch. (A redo op that simply does not connect to the chain
 * is NOT an error — it is tolerated and skipped, as window-scoped logging makes such gaps
 * legitimate.)
 */
public class CbrlReplayException extends RuntimeException {
  public CbrlReplayException(String message) {
    super(message);
  }

  public CbrlReplayException(String message, Throwable cause) {
    super(message, cause);
  }
}
