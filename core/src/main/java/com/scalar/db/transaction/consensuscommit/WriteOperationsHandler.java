package com.scalar.db.transaction.consensuscommit;

public interface WriteOperationsHandler {
  WriteOperationsHandleFuture handle(
      TransactionTableMetadataManager transactionTableMetadataManager,
      String transactionId,
      PrepareMutationComposer composer,
      OperationAndResultHolder operationAndResultHolder);

  interface WriteOperationsHandleFuture {
    void get();
  }
}
