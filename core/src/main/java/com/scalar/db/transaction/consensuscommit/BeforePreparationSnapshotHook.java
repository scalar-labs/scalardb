package com.scalar.db.transaction.consensuscommit;

public interface BeforePreparationSnapshotHook {
  BeforePreparationSnapshotHookFuture handle(
      TransactionTableMetadataManager transactionTableMetadataManager, Snapshot snapshot);

  interface BeforePreparationSnapshotHookFuture {
    void get();
  }
}
