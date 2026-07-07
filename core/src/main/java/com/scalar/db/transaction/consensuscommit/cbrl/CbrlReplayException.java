package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Wraps a failure surfaced from a CBRL restore-replay worker — a storage error, an I/O error, or
 * another unexpected exception during {@code RecordApplier.apply} — so it propagates out of the
 * worker pool as a single restore-replay failure. Chain replay does not treat a redo op that fails
 * to connect to the chain as an error: window-scoped logging makes such below-base links
 * legitimate, so the op is tolerated and skipped rather than raised here.
 */
class CbrlReplayException extends RuntimeException {
  CbrlReplayException(String message) {
    super(message);
  }

  CbrlReplayException(String message, Throwable cause) {
    super(message, cause);
  }
}
