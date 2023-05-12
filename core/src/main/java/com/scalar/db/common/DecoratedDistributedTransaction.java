package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;

public interface DecoratedDistributedTransaction extends DistributedTransaction {
  DistributedTransaction getOriginalTransaction();
}
