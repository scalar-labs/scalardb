package com.scalar.db.transaction.consensuscommit;

import java.util.concurrent.Future;

public interface BeforePreparationHook {
  Future<Void> handle(
      TransactionTableMetadataManager transactionTableMetadataManager, TransactionContext context);
}
