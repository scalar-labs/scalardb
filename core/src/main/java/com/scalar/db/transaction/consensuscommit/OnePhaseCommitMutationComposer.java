package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes.isInsertModeEnabled;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getNextTxVersion;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.LinkedHashSet;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class OnePhaseCommitMutationComposer extends AbstractMutationComposer {

  public OnePhaseCommitMutationComposer(
      String id, TransactionTableMetadataManager tableMetadataManager) {
    super(id, tableMetadataManager);
  }

  @VisibleForTesting
  OnePhaseCommitMutationComposer(
      String id, long current, TransactionTableMetadataManager tableMetadataManager) {
    super(id, current, tableMetadataManager);
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
    if (base instanceof Put) {
      add((Put) base, result);
    } else {
      assert base instanceof Delete;
      add((Delete) base, result);
    }
  }

  private void add(Put base, @Nullable TransactionResult result) throws ExecutionException {
    mutations.add(composePut(base, result));
  }

  private void add(Delete base, @Nullable TransactionResult result) {
    mutations.add(composeDelete(base, result));
  }

  private Put composePut(Put base, @Nullable TransactionResult result) throws ExecutionException {
    assert base.forNamespace().isPresent() && base.forTable().isPresent();

    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(putBuilder::clusteringKey);
    base.getColumns().values().forEach(putBuilder::value);

    putBuilder.textValue(Attribute.ID, id);
    putBuilder.intValue(Attribute.STATE, TransactionState.COMMITTED.get());
    putBuilder.bigIntValue(Attribute.PREPARED_AT, current);
    putBuilder.bigIntValue(Attribute.COMMITTED_AT, current);

    if (!isInsertModeEnabled(base) && result != null) { // overwrite existing record
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

      // Set before image columns to null
      TransactionTableMetadata transactionTableMetadata =
          getTransactionTableMetadata(tableMetadataManager, base);
      LinkedHashSet<String> beforeImageColumnNames =
          transactionTableMetadata.getBeforeImageColumnNames();
      TableMetadata tableMetadata = transactionTableMetadata.getTableMetadata();
      setBeforeImageColumnsToNull(putBuilder, beforeImageColumnNames, tableMetadata);
    } else { // initial record or insert mode enabled
      putBuilder.intValue(Attribute.VERSION, getNextTxVersion(null));

      // check if the record is not created by other conflicting transactions
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    return putBuilder.build();
  }

  private Delete composeDelete(Delete base, @Nullable TransactionResult result) {
    assert base.forNamespace().isPresent() && base.forTable().isPresent();

    // If a record corresponding to a delete in the delete set does not exist in the storage,　we
    // cannot one-phase commit the transaction. This is because the storage does not support
    // delete-if-not-exists semantics, so we cannot detect conflicts with other transactions.
    assert result != null;

    DeleteBuilder.Buildable deleteBuilder =
        Delete.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(deleteBuilder::clusteringKey);

    // check if the record is not interrupted by other conflicting transactions
    if (result.isDeemedAsCommitted()) {
      deleteBuilder.condition(
          ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isNullText()).build());
    } else {
      deleteBuilder.condition(
          ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isEqualToText(result.getId()))
              .build());
    }

    return deleteBuilder.build();
  }
}
