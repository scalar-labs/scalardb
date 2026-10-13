package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.TransactionCrudOperable;

public interface ConsensusCommitScanner extends TransactionCrudOperable.Scanner {
  boolean isClosed();

  /**
   * Closes this scanner without checking or recording its results. Unlike {@link #close()}, this
   * does not run the before-index check, does not put the results into the scan set or the scanner
   * set, and does not verify the scan against the writes of the transaction. Does nothing if this
   * scanner is already closed.
   *
   * <p>Must only be used when the transaction will not be committed or prepared afterwards, such as
   * on rollback, because the reads of a discarded scanner are never validated.
   */
  void discard();
}
