package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.CoordinatorException;
import java.util.Optional;

public class CoordinatorStateRepository {
  private final Coordinator coordinator;

  public CoordinatorStateRepository(
      DistributedStorage coordinatorDbStorage, ConsensusCommitConfig consensusCommitConfig) {
    coordinator = new Coordinator(coordinatorDbStorage, consensusCommitConfig);
  }

  public Optional<Coordinator.State> get(String transactionId) throws ExecutionException {
    try {
      return coordinator.getState(transactionId);
    } catch (CoordinatorException e) {
      // FIXME: Revisit here.
      throw new RuntimeException(e);
    }
  }
}
