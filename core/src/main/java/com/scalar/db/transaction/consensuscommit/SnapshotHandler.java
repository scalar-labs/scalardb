package com.scalar.db.transaction.consensuscommit;

public interface SnapshotHandler {
  SnapshotHandleFuture handle(
      TransactionTableMetadataManager transactionTableMetadataManager, Snapshot snapshot);

  interface SnapshotHandleFuture {
    void get();
  }
}
