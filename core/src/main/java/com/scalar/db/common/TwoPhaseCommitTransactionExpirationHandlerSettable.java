package com.scalar.db.common;

import com.scalar.db.api.TwoPhaseCommitTransaction;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface TwoPhaseCommitTransactionExpirationHandlerSettable {
  void setTransactionExpirationHandler(BiConsumer<String, TwoPhaseCommitTransaction> handler);
}
