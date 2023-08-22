package com.scalar.db.transaction.consensuscommit.replication.repository;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
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

  public boolean isCommitted(String transactionId) throws ExecutionException {
    Optional<Result> result =
        coordinatorDbStorage.get(
            Get.newBuilder()
                .namespace(coordinatorDbNamespace)
                .table(coordinatorDbTable)
                .partitionKey(Key.ofText("tx_id", transactionId))
                .build());
    return result
        .filter(value -> value.getInt("tx_state") == TransactionState.COMMITTED.get())
        .isPresent();
  }
}
