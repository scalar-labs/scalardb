package com.scalar.db.common;

import com.scalar.db.api.TwoPhaseCommitTransaction;

@FunctionalInterface
public interface TwoPhaseCommitTransactionDecorator {
  DecoratedTwoPhaseCommitTransaction decorate(TwoPhaseCommitTransaction transaction);
}
