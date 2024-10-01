package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Operation;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class OperationAndResultHolder {
  private final List<Operation> operations = new ArrayList<>();
  private final List<TransactionResult> transactionResults = new ArrayList<>();

  void add(Operation operation, @Nullable TransactionResult result) {
    operations.add(operation);
    transactionResults.add(result);
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public List<TransactionResult> getTransactionResults() {
    return transactionResults;
  }
}
