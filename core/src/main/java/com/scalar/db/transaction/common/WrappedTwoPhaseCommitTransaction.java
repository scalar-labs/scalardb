package com.scalar.db.transaction.common;

import com.scalar.db.api.TwoPhaseCommitTransaction;

public interface WrappedTwoPhaseCommitTransaction {
  TwoPhaseCommitTransaction getOriginalTransaction();
}
