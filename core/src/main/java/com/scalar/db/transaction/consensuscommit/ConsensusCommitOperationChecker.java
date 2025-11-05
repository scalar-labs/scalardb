package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.checker.ConditionChecker;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ConsensusCommitOperationChecker {

  private final TransactionTableMetadataManager transactionTableMetadataManager;
  private final boolean isIncludeMetadataEnabled;

  public ConsensusCommitOperationChecker(
      TransactionTableMetadataManager transactionTableMetadataManager,
      boolean isIncludeMetadataEnabled) {
    this.transactionTableMetadataManager = transactionTableMetadataManager;
    this.isIncludeMetadataEnabled = isIncludeMetadataEnabled;
  }

  /**
   * Checks the get validity
   *
   * @param get a get operation
   * @param context a transaction context
   * @throws ExecutionException when retrieving the table metadata fails
   * @throws IllegalArgumentException when the get is invalid
   */
  public void check(Get get, TransactionContext context) throws ExecutionException {
    TransactionTableMetadata metadata =
        getTransactionTableMetadata(transactionTableMetadataManager, get);

    // Skip checks if including metadata columns is enabled
    if (!isIncludeMetadataEnabled) {
      // Check projections
      for (String column : get.getProjections()) {
        if (metadata.getTransactionMetaColumnNames().contains(column)) {
          throw new IllegalArgumentException(
              CoreError
                  .CONSENSUS_COMMIT_SPECIFYING_TRANSACTION_METADATA_COLUMNS_IN_PROJECTION_NOT_ALLOWED
                  .buildMessage(get.forFullTableName().get(), column));
        }
      }

      // Check conditions
      for (Selection.Conjunction conjunction : get.getConjunctions()) {
        for (ConditionalExpression condition : conjunction.getConditions()) {
          String column = condition.getColumn().getName();
          if (metadata.getTransactionMetaColumnNames().contains(column)) {
            throw new IllegalArgumentException(
                CoreError
                    .CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_TO_TARGET_TRANSACTION_METADATA_COLUMNS
                    .buildMessage(get.forFullTableName().get(), column));
          }
        }
      }
    }

    // Additional checks for SERIALIZABLE isolation level
    if (context.isolation == Isolation.SERIALIZABLE) {
      // Don't allow index gets
      if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata.getTableMetadata())) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_INDEX_GET_NOT_ALLOWED_IN_SERIALIZABLE.buildMessage());
      }
    }
  }

  /**
   * Checks the scan validity
   *
   * @param scan a scan operation
   * @param context a transaction context
   * @throws ExecutionException when retrieving the table metadata fails
   * @throws IllegalArgumentException when the scan is invalid
   */
  public void check(Scan scan, TransactionContext context) throws ExecutionException {
    TransactionTableMetadata metadata =
        getTransactionTableMetadata(transactionTableMetadataManager, scan);

    // Skip checks if including metadata columns is enabled
    if (!isIncludeMetadataEnabled) {
      // Check projections
      for (String column : scan.getProjections()) {
        if (metadata.getTransactionMetaColumnNames().contains(column)) {
          throw new IllegalArgumentException(
              CoreError
                  .CONSENSUS_COMMIT_SPECIFYING_TRANSACTION_METADATA_COLUMNS_IN_PROJECTION_NOT_ALLOWED
                  .buildMessage(scan.forFullTableName().get(), column));
        }
      }

      // Check conditions
      for (Selection.Conjunction conjunction : scan.getConjunctions()) {
        for (ConditionalExpression condition : conjunction.getConditions()) {
          String column = condition.getColumn().getName();
          if (metadata.getTransactionMetaColumnNames().contains(column)) {
            throw new IllegalArgumentException(
                CoreError
                    .CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_TO_TARGET_TRANSACTION_METADATA_COLUMNS
                    .buildMessage(scan.forFullTableName().get(), column));
          }
        }
      }

      // Check orderings
      for (Scan.Ordering ordering : scan.getOrderings()) {
        String column = ordering.getColumnName();
        if (metadata.getTransactionMetaColumnNames().contains(column)) {
          throw new IllegalArgumentException(
              CoreError
                  .CONSENSUS_COMMIT_SPECIFYING_TRANSACTION_METADATA_COLUMNS_IN_ORDERING_NOT_ALLOWED
                  .buildMessage(scan.forFullTableName().get(), column));
        }
      }
    }

    // Additional checks for SERIALIZABLE isolation level
    if (context.isolation == Isolation.SERIALIZABLE) {
      // Don't allow index scans
      if (ScalarDbUtils.isSecondaryIndexSpecified(scan, metadata.getTableMetadata())) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_INDEX_SCAN_NOT_ALLOWED_IN_SERIALIZABLE.buildMessage());
      }

      // If the scan is a cross-partition scan (ScanAll), don't allow conditions on indexed columns
      if (scan instanceof ScanAll) {
        for (Selection.Conjunction conjunction : scan.getConjunctions()) {
          for (ConditionalExpression condition : conjunction.getConditions()) {
            String column = condition.getColumn().getName();
            if (metadata.getSecondaryIndexNames().contains(column)) {
              throw new IllegalArgumentException(
                  CoreError
                      .CONSENSUS_COMMIT_CONDITION_ON_INDEXED_COLUMNS_NOT_ALLOWED_IN_CROSS_PARTITION_SCAN_IN_SERIALIZABLE
                      .buildMessage());
            }
          }
        }
      }
    }
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
    } else {
      assert mutation instanceof Delete;
      check((Delete) mutation);
    }
  }

  private void check(Put put) throws ExecutionException {
    TransactionTableMetadata metadata =
        getTransactionTableMetadata(transactionTableMetadataManager, put);
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
    checkConditionIsNotTargetingMetadataColumns(put, condition, metadata);
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
    TransactionTableMetadata transactionMetadata =
        getTransactionTableMetadata(transactionTableMetadataManager, delete);
    checkConditionIsNotTargetingMetadataColumns(delete, condition, transactionMetadata);
    ConditionChecker conditionChecker =
        createConditionChecker(transactionMetadata.getTableMetadata());
    conditionChecker.check(condition, false);
  }

  private void checkConditionIsNotTargetingMetadataColumns(
      Mutation mutation, MutationCondition mutationCondition, TransactionTableMetadata metadata) {
    for (ConditionalExpression expression : mutationCondition.getExpressions()) {
      String column = expression.getColumn().getName();
      if (metadata.getTransactionMetaColumnNames().contains(column)) {
        throw new IllegalArgumentException(
            CoreError.CONSENSUS_COMMIT_CONDITION_NOT_ALLOWED_TO_TARGET_TRANSACTION_METADATA_COLUMNS
                .buildMessage(mutation.forFullTableName().get(), column));
      }
    }
  }

  @VisibleForTesting
  ConditionChecker createConditionChecker(TableMetadata tableMetadata) {
    return new ConditionChecker(tableMetadata);
  }
}
