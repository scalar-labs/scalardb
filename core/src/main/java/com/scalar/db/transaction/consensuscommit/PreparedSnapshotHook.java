package com.scalar.db.transaction.consensuscommit;

public interface PreparedSnapshotHook {
  PreparedSnapshotHookFuture handle(
      TransactionTableMetadataManager transactionTableMetadataManager, Snapshot snapshot);

  interface PreparedSnapshotHookFuture {
    void get();
  }
}
