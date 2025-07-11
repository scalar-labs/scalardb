package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isInsertModeEnabled;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getNextTxVersion;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PrepareMutationComposer extends AbstractMutationComposer {

  public PrepareMutationComposer(String id, TransactionTableMetadataManager tableMetadataManager) {
    super(id, tableMetadataManager);
  }

  @VisibleForTesting
  PrepareMutationComposer(
      String id, long current, TransactionTableMetadataManager tableMetadataManager) {
    super(id, current, tableMetadataManager);
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
    if (base instanceof Put) {
      add((Put) base, result);
    } else if (base instanceof Delete) {
      add((Delete) base, result);
    } else if (base instanceof Get) {
      throw new AssertionError(
          "This path should not be reached since the EXTRA_WRITE strategy is deleted");
    } else {
      throw new AssertionError("PrepareMutationComposer.add only accepts Put, Delete, or Get");
    }
  }

  private void add(Put base, @Nullable TransactionResult result) throws ExecutionException {
    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(putBuilder::clusteringKey);
    base.getColumns().values().forEach(putBuilder::value);

    putBuilder.textValue(Attribute.ID, id);
    putBuilder.intValue(Attribute.STATE, TransactionState.PREPARED.get());
    putBuilder.bigIntValue(Attribute.PREPARED_AT, current);

    if (!isInsertModeEnabled(base) && result != null) { // overwrite existing record
      createBeforeColumns(base, result).forEach(putBuilder::value);
      int version = result.getVersion();
      putBuilder.intValue(Attribute.VERSION, getNextTxVersion(version));

      // check if the record is not interrupted by other conflicting transactions
      if (result.isDeemedAsCommitted()) {
        // record is deemed-commit state
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText()).build());
      } else {
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(result.getId()))
                .build());
      }
    } else { // initial record or insert mode enabled
      putBuilder.intValue(Attribute.VERSION, getNextTxVersion(null));

      // check if the record is not created by other conflicting transactions
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    mutations.add(putBuilder.build());
  }

  private void add(Delete base, @Nullable TransactionResult result) throws ExecutionException {
    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(putBuilder::clusteringKey);

    putBuilder.textValue(Attribute.ID, id);
    putBuilder.intValue(Attribute.STATE, TransactionState.DELETED.get());
    putBuilder.bigIntValue(Attribute.PREPARED_AT, current);

    if (result != null) {
      createBeforeColumns(base, result).forEach(putBuilder::value);
      int version = result.getVersion();
      putBuilder.intValue(Attribute.VERSION, getNextTxVersion(version));

      // check if the record is not interrupted by other conflicting transactions
      if (result.isDeemedAsCommitted()) {
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText()).build());
      } else {
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(result.getId()))
                .build());
      }
    } else {
      putBuilder.intValue(Attribute.VERSION, getNextTxVersion(null));

      // check if the record is not created by other conflicting transactions
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    mutations.add(putBuilder.build());
  }

  private List<Column<?>> createBeforeColumns(Mutation base, TransactionResult result)
      throws ExecutionException {
    List<Column<?>> columns = new ArrayList<>();
    for (Column<?> column : result.getColumns().values()) {
      if (isBeforeRequired(base, column.getName())) {
        if (column.getName().equals(Attribute.VERSION) && column.hasNullValue()) {
          // A prepare-state record with NULLs for both before_id and before_version will be deleted
          // as an initial record in a rollback situation. To avoid this for
          // NULL-transaction-metadata records (i.e., records regarded as committed) and roll back
          // them correctly, we need to use version 0 rather than NULL for before_version. Note that
          // we can use other "before" columns to distinguish those two cases.
          columns.add(IntColumn.of(Attribute.BEFORE_VERSION, 0));
        } else {
          columns.add(column.copyWith(Attribute.BEFORE_PREFIX + column.getName()));
        }
      }
    }
    return columns;
  }

  private boolean isBeforeRequired(Mutation base, String columnName) throws ExecutionException {
    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(tableMetadataManager, base);
    return !transactionTableMetadata.getPrimaryKeyColumnNames().contains(columnName)
        && transactionTableMetadata.getAfterImageColumnNames().contains(columnName);
  }
}
