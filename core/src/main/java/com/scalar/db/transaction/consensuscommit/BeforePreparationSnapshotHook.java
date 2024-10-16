package com.scalar.db.transaction.consensuscommit;

import java.util.concurrent.Future;

public interface BeforePreparationSnapshotHook {
  Future<Void> handle(
      TransactionTableMetadataManager transactionTableMetadataManager,
      Snapshot.ReadWriteSets readWriteSets);
}
