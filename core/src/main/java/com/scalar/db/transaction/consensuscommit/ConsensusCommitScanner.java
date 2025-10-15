package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.TransactionCrudOperable;

public interface ConsensusCommitScanner extends TransactionCrudOperable.Scanner {
  boolean isClosed();
}
