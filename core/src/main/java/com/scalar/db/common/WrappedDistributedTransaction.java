package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;

public interface WrappedDistributedTransaction {
  DistributedTransaction getOriginalTransaction();
}
