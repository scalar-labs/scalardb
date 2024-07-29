package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface DistributedTransactionExpirationHandlerSettable {
  void setTransactionExpirationHandler(BiConsumer<String, DistributedTransaction> handler);
}
