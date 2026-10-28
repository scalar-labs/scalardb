package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RollbackMutationComposer extends AbstractMutationComposer {

  // The latest state of the records the transaction writes, read by the caller. Empty when the
  // caller drives this composer with the record to roll back, which it passes to `add` directly.
  private final Map<Snapshot.Key, TransactionResult> latestRecords;

  /**
   * Creates a composer that uses the latest records the caller has read. The records a transaction
   * writes are read together elsewhere, which lets them be read in parallel and lets the caller use
   * them for other purposes. A caller that drives this composer with the record to roll back, which
   * it passes to {@link #add(Operation, TransactionResult)} itself, passes no records here.
   *
   * @param id the ID of the transaction to roll back
   * @param tableMetadataManager a transaction table metadata manager
   * @param latestRecords the latest state of the records the transaction writes, by key. A key that
   *     has no record in storage is absent from this map
   */
  public RollbackMutationComposer(
      String id,
      TransactionTableMetadataManager tableMetadataManager,
      Map<Snapshot.Key, TransactionResult> latestRecords) {
    // Rollback restores before-images and does not write a fresh phase timestamp, so the inherited
    // `timestamp` field is unused here. We pass 0L to the base constructor only to satisfy it;
    // there is no commit-phase timestamp to thread in on the rollback path.
    super(id, 0L, tableMetadataManager);
    this.latestRecords = ImmutableMap.copyOf(latestRecords);
  }

  /** Rollback in either prepare phase in commit or lazy recovery phase in read. */
  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
    TransactionResult latest;
    if (result == null || !Objects.equals(result.getId(), id)) {
      // For rollback in prepare phase, we need to check the latest status of the record.
      latest = getLatestRecord(base).orElse(null);
      if (latest == null) {
        // The record was not prepared (yet) by this transaction or has already been rollback
        // deleted.
        return;
      }
      if (!Objects.equals(latest.getId(), id)) {
        // This is the case for the record that was not prepared (yet) by this transaction or has
        // already been rolled back. We need to use Objects.equals() here since the transaction ID
        // of the latest record can be NULL (and different from this transaction's ID) when the
        // record has already been rolled back to the deemed committed state by another transaction.
        return;
      }
    } else {
      // For rollback in lazy recovery, we can use the result directly.
      latest = result;
    }

    if (latest.hasBeforeImage()) {
      mutations.add(composePut(base, latest));
    } else {
      // no record to rollback, so it should be deleted
      mutations.add(composeDelete(base, latest));
    }
  }

  /**
   * Returns the latest state of the record the specified operation targets, from the records the
   * caller has read.
   */
  private Optional<TransactionResult> getLatestRecord(Operation base) {
    // A caller that passes no latest records drives this composer with the record to roll back, so
    // this is only reached for the mutations of a transaction
    assert base instanceof Mutation;

    Snapshot.Key key =
        base instanceof Put ? new Snapshot.Key((Put) base) : new Snapshot.Key((Delete) base);
    return Optional.ofNullable(latestRecords.get(key));
  }

  private Put composePut(Operation base, TransactionResult result) throws ExecutionException {
    assert result != null
        && (result.getState().equals(TransactionState.PREPARED)
            || result.getState().equals(TransactionState.DELETED));

    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(tableMetadataManager, base);
    LinkedHashSet<String> beforeImageColumnNames =
        transactionTableMetadata.getBeforeImageColumnNames();
    TableMetadata tableMetadata = transactionTableMetadata.getTableMetadata();

    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata);
    Optional<Key> clusteringKey = ScalarDbUtils.getClusteringKey(result, tableMetadata);

    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(partitionKey)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(id))
                    .and(ConditionBuilder.column(STATE).isEqualToInt(result.getState().get()))
                    .build())
            .consistency(Consistency.LINEARIZABLE);
    clusteringKey.ifPresent(putBuilder::clusteringKey);

    Map<String, Column<?>> columns = new HashMap<>();
    createAfterImageColumnsFromBeforeImage(columns, result, beforeImageColumnNames);
    columns.values().forEach(putBuilder::value);

    // Set before image columns to null
    setBeforeImageColumnsToNull(putBuilder, beforeImageColumnNames, tableMetadata);

    return putBuilder.build();
  }

  private Delete composeDelete(Operation base, TransactionResult result) throws ExecutionException {
    assert result != null
        && (result.getState().equals(TransactionState.PREPARED)
            || result.getState().equals(TransactionState.DELETED));

    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(tableMetadataManager, base);
    TableMetadata tableMetadata = transactionTableMetadata.getTableMetadata();
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata);
    Optional<Key> clusteringKey = ScalarDbUtils.getClusteringKey(result, tableMetadata);

    DeleteBuilder.Buildable deleteBuilder =
        Delete.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(partitionKey)
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column(ID).isEqualToText(id))
                    .and(ConditionBuilder.column(STATE).isEqualToInt(result.getState().get()))
                    .build())
            .consistency(Consistency.LINEARIZABLE);
    clusteringKey.ifPresent(deleteBuilder::clusteringKey);

    return deleteBuilder.build();
  }
}
