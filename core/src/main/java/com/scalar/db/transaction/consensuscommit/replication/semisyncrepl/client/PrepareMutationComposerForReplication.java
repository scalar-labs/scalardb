package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.PrepareMutationComposer;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class PrepareMutationComposerForReplication extends PrepareMutationComposer {
  private final List<Operation> operations = new ArrayList<>();
  private final List<TransactionResult> transactionResults = new ArrayList<>();

  public PrepareMutationComposerForReplication(
      String id, TransactionTableMetadataManager tableMetadataManager) {
    super(id, tableMetadataManager);
  }

  public String transactionId() {
    return id;
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
    super.add(base, result);
    operations.add(base);
    transactionResults.add(result);
  }

  public List<Operation> operations() {
    return operations;
  }

  public List<TransactionResult> transactionResultList() {
    return transactionResults;
  }
}
