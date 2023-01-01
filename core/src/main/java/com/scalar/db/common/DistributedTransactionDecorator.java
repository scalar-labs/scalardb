package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;

@FunctionalInterface
public interface DistributedTransactionDecorator {
  DecoratedDistributedTransaction decorate(DistributedTransaction transaction);
}
