package com.scalar.db.transaction.consensuscommit.replication;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.transaction.consensuscommit.Snapshot;

public interface LogRecorder extends AutoCloseable {
  void record(Snapshot snapshot) throws PreparationConflictException, ExecutionException;
}
