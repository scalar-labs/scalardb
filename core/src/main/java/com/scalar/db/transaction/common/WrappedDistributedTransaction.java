package com.scalar.db.transaction.common;

import com.scalar.db.api.DistributedTransaction;

public interface WrappedDistributedTransaction {
  DistributedTransaction getOriginalTransaction();
}
