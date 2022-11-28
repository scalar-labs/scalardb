package com.scalar.db.common;

import com.scalar.db.api.TwoPhaseCommitTransaction;

public interface WrappedTwoPhaseCommitTransaction {
  TwoPhaseCommitTransaction getOriginalTransaction();
}
