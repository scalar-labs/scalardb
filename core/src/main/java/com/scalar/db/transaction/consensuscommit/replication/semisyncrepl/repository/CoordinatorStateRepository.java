package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.CoordinatorState;
import java.time.Instant;
import java.util.Optional;

public class CoordinatorStateRepository {
  private final DistributedStorage coordinatorDbStorage;
  private final String coordinatorDbNamespace;
  private final String coordinatorDbTable;

  public CoordinatorStateRepository(
      DistributedStorage coordinatorDbStorage,
      String coordinatorDbNamespace,
      String coordinatorDbTable) {
    this.coordinatorDbNamespace = coordinatorDbNamespace;
    this.coordinatorDbTable = coordinatorDbTable;
    this.coordinatorDbStorage = coordinatorDbStorage;
  }

  public Optional<CoordinatorState> get(String transactionId) throws ExecutionException {
    Optional<Result> result =
        coordinatorDbStorage.get(
            Get.newBuilder()
                .namespace(coordinatorDbNamespace)
                .table(coordinatorDbTable)
                .partitionKey(Key.ofText("tx_id", transactionId))
                .build());
    return result.map(
        r ->
            new CoordinatorState(
                r.getText("tx_id"),
                TransactionState.getInstance(r.getInt("tx_state")),
                // TODO: Revisit here to think the difference between `${table}.tx_committed_at`
                // and `state.tx_created_at`.
                Instant.ofEpochMilli(r.getBigInt("tx_created_at"))));
  }
}
