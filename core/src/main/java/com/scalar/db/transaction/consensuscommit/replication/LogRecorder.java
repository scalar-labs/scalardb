package com.scalar.db.transaction.consensuscommit.replication;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client.PrepareMutationComposerForReplication;
import java.util.concurrent.Future;

public interface LogRecorder {
  Future<Void> record(PrepareMutationComposerForReplication composer)
      throws PreparationConflictException, ExecutionException;
}
