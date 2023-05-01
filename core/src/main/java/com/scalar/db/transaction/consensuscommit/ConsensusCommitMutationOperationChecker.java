package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitMutationOperationChecker {

  private final TransactionTableMetadataManager transactionTableMetadataManager;

  public ConsensusCommitMutationOperationChecker(
      TransactionTableMetadataManager transactionTableMetadataManager) {
    this.transactionTableMetadataManager = transactionTableMetadataManager;
  }

  private TransactionTableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    TransactionTableMetadata metadata =
        transactionTableMetadataManager.getTransactionTableMetadata(operation);
    if (metadata == null) {
      throw new IllegalArgumentException(
          "The specified table is not found: " + operation.forFullTableName().get());
    }
    return metadata;
  }

  public void check(Mutation mutation) throws ExecutionException {
    if (mutation instanceof Put) {
      check((Put) mutation);
    } else if (mutation instanceof Delete) {
      check((Delete) mutation);
    }
  }

  private void check(Put put) throws ExecutionException {
    put.getCondition()
        .ifPresent(
            condition -> {
              if (!(condition instanceof PutIf
                  || condition instanceof PutIfNotExists
                  || condition instanceof PutIfExists)) {
                throw new IllegalArgumentException("The condition is not allowed on Put operation");
              }
            });
    TransactionTableMetadata metadata = getTableMetadata(put);
    for (String column : put.getContainedColumnNames()) {
      if (metadata.getTransactionMetaColumnNames().contains(column)) {
        throw new IllegalArgumentException(
            "Mutating transaction metadata columns is not allowed. column: " + column);
      }
    }
    checkConditionIsNotTargetingMetadataColumns(put, metadata);
  }

  private void check(Delete delete) throws ExecutionException {
    delete
        .getCondition()
        .ifPresent(
            condition -> {
              if (!(condition instanceof DeleteIf || condition instanceof DeleteIfExists)) {
                throw new IllegalArgumentException(
                    "The condition is not allowed on Delete operation");
              }
            });
    checkConditionIsNotTargetingMetadataColumns(delete, getTableMetadata(delete));
  }

  private void checkConditionIsNotTargetingMetadataColumns(
      Mutation mutation, TransactionTableMetadata metadata) {
    List<ConditionalExpression> expressions =
        mutation
            .getCondition()
            .map(MutationCondition::getExpressions)
            .orElse(Collections.emptyList());
    for (ConditionalExpression expression : expressions) {
      String column = expression.getColumn().getName();
      if (metadata.getTransactionMetaColumnNames().contains(column)) {
        throw new IllegalArgumentException(
            "The condition is not allowed to target transaction metadata columns. column: "
                + column);
      }
    }
  }
}
