package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.createGet;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the participant-side (data-record) operations of the Consensus Commit protocol:
 * preparing, validating, committing, and rolling back the records the transaction touches in user
 * data tables.
 *
 * <p>Methods here touch user data tables via {@link DistributedStorage} and the {@link
 * MutationsGrouper} / {@link ParallelExecutor} stack, and never write the state of the transaction
 * to the Coordinator table. The records of other transactions that block the writes of the
 * transaction are recovered through {@link RecoveryExecutor}, which reads the state of those
 * transactions from the Coordinator table, may abort them there, and rolls their records forward or
 * back.
 */
@ThreadSafe
class ParticipantCommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(ParticipantCommitHandler.class);

  private final DistributedStorage storage;
  private final RecoveryExecutor recoveryExecutor;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final AsyncExecutor asyncExecutor;
  private final MutationsGrouper mutationsGrouper;
  private final boolean onePhaseCommitEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  ParticipantCommitHandler(
      DistributedStorage storage,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      AsyncExecutor asyncExecutor,
      MutationsGrouper mutationsGrouper,
      boolean onePhaseCommitEnabled) {
    this.storage = checkNotNull(storage);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.asyncExecutor = checkNotNull(asyncExecutor);
    this.mutationsGrouper = checkNotNull(mutationsGrouper);
    this.onePhaseCommitEnabled = onePhaseCommitEnabled;
  }

  void prepareRecords(TransactionContext context, long preparedAt) throws PreparationException {
    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(context.transactionId, preparedAt, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
      parallelExecutor.prepareRecords(tasks, context.transactionId);
    } catch (NoMutationException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONDITIONAL_MUTATION_NOT_APPLIED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  void validateRecords(TransactionContext context) throws ValidationException {
    if (!context.isValidationRequired()) {
      return;
    }

    try {
      // validation is executed when SERIALIZABLE is chosen.
      context.snapshot.toSerializable(storage);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  void commitRecords(TransactionContext context, long committedAt) {
    // An asynchronous commit runs on a thread of the async executor and reads the snapshot there,
    // after this method returns. This is safe since the snapshot of a committed transaction is no
    // longer modified
    asyncExecutor.commitRecords(() -> doCommitRecords(context, committedAt), context.transactionId);
  }

  private void doCommitRecords(TransactionContext context, long committedAt) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(context.transactionId, committedAt, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
      parallelExecutor.commitRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Committing records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  void rollbackRecords(TransactionContext context) {
    logger.debug("Rollback from snapshot for {}", context.transactionId);

    // An asynchronous rollback runs on a thread of the async executor and reads the snapshot
    // there, after this method returns. This is safe since the snapshot of a transaction being
    // rolled back is no longer modified
    asyncExecutor.rollbackRecords(() -> doRollbackRecords(context), context.transactionId);
  }

  private void doRollbackRecords(TransactionContext context) {
    // The latest state of the records this transaction writes, which the rollback needs to decide
    // what to restore
    Map<Snapshot.Key, TransactionResult> latestRecords = new ConcurrentHashMap<>();

    try {
      parallelExecutor.readRecordsForRollback(
          toReadTasks(getKeysOfWritesAndDeletes(context), latestRecords), context.transactionId);

      RollbackMutationComposer composer =
          new RollbackMutationComposer(context.transactionId, tableMetadataManager, latestRecords);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
      parallelExecutor.rollbackRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Rolling back records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }

    tryRecoverRecordsBlockingWrites(context, getKeysOfWritesNeverRead(context), latestRecords);
  }

  /**
   * Triggers recovery for the records of other transactions that are not committed yet and that
   * block the writes of this transaction for the keys it never read.
   *
   * <p>The read path recovers such a record when the key is read, so a key this transaction reads
   * is left to that read, or to the read a retry of the transaction performs. A key it never reads
   * is not read by anyone: the mutation for it is prepared with a {@code PutIfNotExists} condition,
   * which fails when any record is physically present on the key, so a record left behind by a
   * transaction that did not finish blocks every subsequent attempt for that key, and the attempts
   * never read it. That is the case for an insert and for a write to a key the transaction did not
   * read.
   *
   * <p>The records are the ones the rollback read to decide what to restore, so recovering them
   * costs no extra read. A failed one-phase commit, which is not rolled back, reads them for this
   * alone. A record this transaction prepared itself is restored by the rollback, and a committed
   * record is a duplicate key that no recovery can resolve; neither is recovered here.
   *
   * <p>This is best-effort. The recovery runs on a background thread that nothing here waits for,
   * and it is skipped for a writer that may still be in flight, so a record is not necessarily
   * resolved when this returns. The caller reports the failure as it did before; the next attempt
   * of the transaction is what benefits from the recovery.
   *
   * @param context the transaction context
   * @param keys the keys of the writes of this transaction that it never read
   * @param latestRecords the latest state of the records, by key. A key that has no record in
   *     storage, or whose record could not be read, is absent
   */
  private void tryRecoverRecordsBlockingWrites(
      TransactionContext context,
      List<Snapshot.Key> keys,
      Map<Snapshot.Key, TransactionResult> latestRecords) {
    for (Snapshot.Key key : keys) {
      TransactionResult latestRecord = latestRecords.get(key);
      if (latestRecord == null
          || latestRecord.isCommitted()
          || context.transactionId.equals(latestRecord.getId())) {
        continue;
      }

      try {
        RecoveryExecutor.Result recoveryResult =
            recoveryExecutor.execute(
                key,
                createGet(key),
                latestRecord,
                context.transactionId,
                RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

        // Register the recovery task so that callers that wait for recovery completion (currently
        // only tests) can observe it. The transaction itself does not wait for it: it has failed to
        // commit or is rolled back, and the record is recovered for the next attempt
        context.recoveryResults.add(recoveryResult);
      } catch (Exception e) {
        logger.warn(
            "Recovering a record blocking a write failed. Key: {}; Transaction ID: {}",
            key,
            context.transactionId,
            e);
      }
    }
  }

  /**
   * Returns the keys of the writes of this transaction that it never read. The deletes are not
   * considered: the record of a delete is always read before it is prepared.
   *
   * @param context the transaction context
   * @return the keys of the writes this transaction never read
   */
  private List<Snapshot.Key> getKeysOfWritesNeverRead(TransactionContext context) {
    List<Snapshot.Key> keys = new ArrayList<>();
    for (Map.Entry<Snapshot.Key, Put> entry : context.snapshot.getWriteSet()) {
      if (!context.snapshot.containsKeyInReadSet(entry.getKey())) {
        keys.add(entry.getKey());
      }
    }
    return keys;
  }

  // The keys of every record this transaction writes, including the ones it deletes
  private List<Snapshot.Key> getKeysOfWritesAndDeletes(TransactionContext context) {
    List<Snapshot.Key> keys = new ArrayList<>();
    context.snapshot.getWriteSet().forEach(entry -> keys.add(entry.getKey()));
    context.snapshot.getDeleteSet().forEach(entry -> keys.add(entry.getKey()));
    return keys;
  }

  /**
   * Creates the tasks that read the latest state of the records of the specified keys.
   *
   * <p>The reads are independent of each other, so they run through the parallel executor. They are
   * not subject to the mutation grouping the storage requires, so every record is read on its own,
   * regardless of how the mutations are grouped afterwards.
   *
   * @param keys the keys of the records to read
   * @param latestRecords the map the tasks put the latest state of the records into, by key. A key
   *     that has no record in storage stays absent
   * @return the tasks that read the records
   */
  private List<ParallelExecutorTask> toReadTasks(
      List<Snapshot.Key> keys, Map<Snapshot.Key, TransactionResult> latestRecords) {
    List<ParallelExecutorTask> tasks = new ArrayList<>(keys.size());
    for (Snapshot.Key key : keys) {
      tasks.add(
          () ->
              storage
                  .get(createGet(key))
                  .ifPresent(result -> latestRecords.put(key, new TransactionResult(result))));
    }
    return tasks;
  }

  private List<ParallelExecutorTask> toTasks(List<List<Mutation>> groupedMutations) {
    List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
    for (List<Mutation> mutations : groupedMutations) {
      tasks.add(() -> storage.mutate(mutations));
    }
    return tasks;
  }

  /**
   * Checks whether the transaction is eligible for a one-phase commit. The caller is the
   * orchestrator: when this returns {@code true}, it follows up with {@link
   * #onePhaseCommitRecords(TransactionContext)} to perform the actual commit; otherwise it falls
   * back to the two-phase path.
   */
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    if (!onePhaseCommitEnabled) {
      return false;
    }

    // If validation is required, we cannot one-phase commit the transaction
    if (context.isValidationRequired()) {
      return false;
    }

    // If the snapshot has no writes or deletes, we do not one-phase commit the transaction
    if (!context.snapshot.hasWritesOrDeletes()) {
      return false;
    }

    Collection<Map.Entry<Snapshot.Key, Delete>> deleteSetEntries = context.snapshot.getDeleteSet();

    // If a record corresponding to a delete in the delete set does not exist in storage, we
    // cannot one-phase commit the transaction. This is because the storage does not support
    // delete-if-not-exists semantics, so we cannot detect conflicts with other transactions.
    for (Map.Entry<Snapshot.Key, Delete> entry : deleteSetEntries) {
      Optional<TransactionResult> result = context.snapshot.getFromReadSet(entry.getKey());

      // For deletes, we always perform implicit pre-reads if the result does not exist in the read
      // set, so the result should normally exist in the read set. If it is absent (null) or empty,
      // the transaction is not eligible for a one-phase commit.
      if (result == null || !result.isPresent()) {
        return false;
      }
    }

    try {
      // If the mutations can be grouped altogether, the mutations can be done in a single mutate
      // API call, so we can one-phase commit the transaction
      return mutationsGrouper.canBeGroupedAltogether(
          Stream.concat(
                  context.snapshot.getWriteSet().stream().map(Map.Entry::getValue),
                  deleteSetEntries.stream().map(Map.Entry::getValue))
              .collect(Collectors.toList()));
    } catch (ExecutionException e) {
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_COMMITTING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  /**
   * Performs the one-phase commit storage mutation. The caller is expected to have just checked
   * eligibility via {@link #canOnePhaseCommit(TransactionContext)}.
   */
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      OnePhaseCommitMutationComposer composer =
          new OnePhaseCommitMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);

      // One-phase commit does not require grouping mutations and using the parallel executor since
      // it is always executed in a single mutate API call.
      storage.mutate(composer.get());
    } catch (NoMutationException e) {
      // Nothing was applied, since the mutations are executed in a single atomic call, so there is
      // nothing to roll back, and the caller does not roll back a one-phase commit. The records
      // blocking the writes, which the rollback recovers otherwise, are recovered here
      readAndRecoverRecordsBlockingWrites(context);

      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONDITIONAL_MUTATION_NOT_APPLIED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_ONE_PHASE_COMMITTING_RECORDS_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  /**
   * Recovers the records of other transactions that are not committed yet and that block the writes
   * of this transaction for the keys it never read, for a failed one-phase commit.
   *
   * <p>A failed one-phase commit applies nothing, so it is not rolled back, and the recovery that
   * the rollback triggers otherwise does not happen. This reads the latest state of the records of
   * those keys itself instead. The reads run on the calling thread; the recovery runs on a
   * background thread, as it does from the rollback.
   *
   * @param context the transaction context
   */
  private void readAndRecoverRecordsBlockingWrites(TransactionContext context) {
    List<Snapshot.Key> keys = getKeysOfWritesNeverRead(context);
    if (keys.isEmpty()) {
      return;
    }

    Map<Snapshot.Key, TransactionResult> latestRecords = new ConcurrentHashMap<>();
    try {
      // These reads take the place of the ones of the rollback, which a failed one-phase commit
      // does not need, so they follow the settings of the rollback
      parallelExecutor.readRecordsForRollback(
          toReadTasks(keys, latestRecords), context.transactionId);
    } catch (Exception e) {
      logger.info(
          "Reading the records blocking the writes failed. Transaction ID: {}",
          context.transactionId,
          e);
      // ignore since the recovery is best-effort, and the records that were read are recovered
    }

    tryRecoverRecordsBlockingWrites(context, keys, latestRecords);
  }
}
