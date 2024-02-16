package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.checker.ConditionChecker;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
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
          CoreError.TABLE_NOT_FOUND.buildMessage(operation.forFullTableName().get()));
    }
    return metadata;
  }

  /**
   * Checks the mutation validity
   *
   * @param mutation a mutation operation
   * @throws ExecutionException when retrieving the table metadata fails
   * @throws IllegalArgumentException when the mutation is invalid
   */
  public void check(Mutation mutation) throws ExecutionException {
    if (mutation instanceof Put) {
      check((Put) mutation);
    } else if (mutation instanceof Delete) {
      check((Delete) mutation);
    }
  }

  private void check(Put put) throws ExecutionException {
    TransactionTableMetadata metadata = getTableMetadata(put);
    for (String column : put.getContainedColumnNames()) {
      if (metadata.getTransactionMetaColumnNames().contains(column)) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_MUTATING_TRANSACTION_METADATA_COLUMNS_NOT_ALLOWED
                .buildMessage(put.forFullTableName().get(), column));
      }
    }

    if (!put.getCondition().isPresent()) {
      return;
    }
    MutationCondition condition = put.getCondition().get();

    if (!(condition instanceof PutIf
        || condition instanceof PutIfNotExists
        || condition instanceof PutIfExists)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_ON_PUT.buildMessage(
              condition.getClass().getSimpleName()));
    }
    checkConditionIsNotTargetingMetadataColumns(condition, metadata);
    ConditionChecker conditionChecker = createConditionChecker(metadata.getTableMetadata());
    conditionChecker.check(condition, true);
  }

  private void check(Delete delete) throws ExecutionException {
    if (!delete.getCondition().isPresent()) {
      return;
    }
    MutationCondition condition = delete.getCondition().get();

    if (!(condition instanceof DeleteIf || condition instanceof DeleteIfExists)) {
      throw new IllegalArgumentException(
          CoreError.CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_ON_DELETE.buildMessage(
              condition.getClass().getSimpleName()));
    }
    TransactionTableMetadata transactionMetadata = getTableMetadata(delete);
    checkConditionIsNotTargetingMetadataColumns(condition, transactionMetadata);
    ConditionChecker conditionChecker =
        createConditionChecker(transactionMetadata.getTableMetadata());
    conditionChecker.check(condition, false);
  }

  private void checkConditionIsNotTargetingMetadataColumns(
      MutationCondition mutationCondition, TransactionTableMetadata metadata) {
    for (ConditionalExpression expression : mutationCondition.getExpressions()) {
      String column = expression.getColumn().getName();
      if (metadata.getTransactionMetaColumnNames().contains(column)) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_TO_TARGET_TRANSACTION_METADATA_COLUMNS
                .buildMessage(column));
      }
    }
  }

  @VisibleForTesting
  ConditionChecker createConditionChecker(TableMetadata tableMetadata) {
    return new ConditionChecker(tableMetadata);
  }
}
