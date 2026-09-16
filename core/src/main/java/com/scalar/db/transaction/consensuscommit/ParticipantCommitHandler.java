package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.createGet;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the participant-side (data-record) operations of the Consensus Commit protocol:
 * preparing, validating, committing, and rolling back the records the transaction touches in user
 * data tables.
 *
 * <p>Methods here only touch user data tables via {@link DistributedStorage} and the {@link
 * MutationsGrouper} / {@link ParallelExecutor} stack, and never write to the Coordinator table
 * themselves. The recovery triggered when a conditional write fails is delegated to {@link
 * RecoveryExecutor}, which does read and may write Coordinator state on a background thread.
 */
@ThreadSafe
class ParticipantCommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(ParticipantCommitHandler.class);

  private final DistributedStorage storage;
  private final RecoveryExecutor recoveryExecutor;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final MutationsGrouper mutationsGrouper;
  private final boolean onePhaseCommitEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  ParticipantCommitHandler(
      DistributedStorage storage,
      RecoveryExecutor recoveryExecutor,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean onePhaseCommitEnabled) {
    this.storage = checkNotNull(storage);
    this.recoveryExecutor = checkNotNull(recoveryExecutor);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.mutationsGrouper = checkNotNull(mutationsGrouper);
    this.onePhaseCommitEnabled = onePhaseCommitEnabled;
  }

  void prepareRecords(TransactionContext context, long preparedAt) throws PreparationException {
    // Collects the mutations of the storage calls that fail because a conditional write is not
    // applied. Written by the tasks below, which may run in parallel. The records blocking them are
    // recovered whichever exception is caught below: with parallel preparation, all the tasks run
    // to the end and the exception of the task that failed first is thrown, so a
    // NoMutationException of another task may only be suppressed in it. The list is empty unless a
    // task failed that way.
    List<Mutation> notAppliedMutations = Collections.synchronizedList(new ArrayList<>());

    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(context.transactionId, preparedAt, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations, notAppliedMutations);
      parallelExecutor.prepareRecords(tasks, context.transactionId);
    } catch (NoMutationException e) {
      tryRecoverRecordsBlockingWrites(context, notAppliedMutations);
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      tryRecoverRecordsBlockingWrites(context, notAppliedMutations);
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      tryRecoverRecordsBlockingWrites(context, notAppliedMutations);
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
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(context.transactionId, storage, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
      parallelExecutor.rollbackRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Rolling back records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  private List<ParallelExecutorTask> toTasks(List<List<Mutation>> groupedMutations) {
    return toTasks(groupedMutations, null);
  }

  /**
   * Builds the tasks that write the given groups of mutations.
   *
   * <p>When {@code notAppliedMutations} is given, the mutations of a group whose storage call fails
   * because a conditional write is not applied are added to it. They are collected here rather than
   * taken from {@link NoMutationException#getMutations()} because that holds the mutations as the
   * storage received them, which a storage may have converted before executing (it may, for
   * instance, rewrite a mutation for a virtual table into mutations for the tables it is made of).
   * The recovery path needs the mutations this transaction composed.
   */
  private List<ParallelExecutorTask> toTasks(
      List<List<Mutation>> groupedMutations, @Nullable List<Mutation> notAppliedMutations) {
    List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
    for (List<Mutation> mutations : groupedMutations) {
      tasks.add(
          () -> {
            try {
              storage.mutate(mutations);
            } catch (NoMutationException e) {
              if (notAppliedMutations != null) {
                notAppliedMutations.addAll(mutations);
              }
              throw e;
            }
          });
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
    List<Mutation> mutations = null;

    try {
      OnePhaseCommitMutationComposer composer =
          new OnePhaseCommitMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      mutations = composer.get();

      // One-phase commit does not require grouping mutations and using the parallel executor since
      // it is always executed in a single mutate API call.
      storage.mutate(mutations);
    } catch (NoMutationException e) {
      // The mutations this transaction composed are used rather than the ones the exception holds;
      // see toTasks(List, List)
      assert mutations != null;
      tryRecoverRecordsBlockingWrites(context, mutations);
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
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
   * Triggers recovery for the records that block the conditional writes that failed in this
   * transaction.
   *
   * <p>A write that creates an initial record is prepared with a {@link PutIfNotExists} condition,
   * which fails when any record is physically present on the key. That is the case for an insert,
   * and also for a write to a key that the transaction never read. Such a write never triggers the
   * lazy recovery that the read path performs, so an uncommitted record left behind by a
   * transaction that did not finish blocks every subsequent attempt for that key until someone
   * reads it, which never happens for a key that is only ever written this way.
   *
   * <p>Only the mutations of the storage calls that failed are examined, one read each. The other
   * mutations of the transaction either succeeded or, when the preparation stops at the first
   * failure, were never attempted.
   *
   * <p>This is best-effort and does not change what the caller reports: the conditional write may
   * have failed for a record this method does not recover, or for a committed record that no
   * recovery can resolve. A failure while reading a record is logged and ignored. The recovery
   * itself runs on a background thread that nothing here waits for, so a failure inside it is not
   * surfaced here either; the caller throws a conflict exception in either case and the next
   * attempt retries the whole thing.
   */
  private void tryRecoverRecordsBlockingWrites(
      TransactionContext context, List<Mutation> notAppliedMutations) {
    for (Mutation mutation : notAppliedMutations) {
      if (!(mutation instanceof Put)
          || !(mutation.getCondition().orElse(null) instanceof PutIfNotExists)) {
        continue;
      }

      Snapshot.Key key = new Snapshot.Key((Put) mutation);
      try {
        Get get = createGet(key);
        Optional<Result> result = storage.get(get);
        if (!result.isPresent()) {
          continue;
        }

        TransactionResult txResult = new TransactionResult(result.get());
        if (txResult.isCommitted()) {
          // The record already exists, which is a duplicate key that recovery cannot resolve
          continue;
        }
        if (context.transactionId.equals(txResult.getId())) {
          // The record was prepared by this transaction, and the caller rolls it back
          continue;
        }

        RecoveryExecutor.Result recoveryResult =
            recoveryExecutor.execute(
                key,
                get,
                txResult,
                context.transactionId,
                RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

        // Register the recovery task so that callers that wait for recovery completion (currently
        // only tests) can observe it. The transaction itself does not wait for it: it is about to
        // fail with a conflict, and the record is recovered for the next attempt.
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
}
