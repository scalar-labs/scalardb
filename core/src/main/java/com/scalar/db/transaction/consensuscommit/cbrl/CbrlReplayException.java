package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Thrown when CBRL chain replay detects an inconsistency it cannot safely resolve — e.g. a redo op
 * whose {@code prevTxId} links to no other op or the record's current version (a broken chain).
 * Replay fails loud rather than silently producing a wrong restored state.
 */
public class CbrlReplayException extends RuntimeException {
  public CbrlReplayException(String message) {
    super(message);
  }
}
