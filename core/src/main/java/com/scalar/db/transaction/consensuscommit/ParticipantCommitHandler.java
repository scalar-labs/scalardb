package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
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
 * <p>This is one of the two specialized handlers that {@link CommitHandler} delegates to. The other
 * is {@link CoordinatorCommitHandler}, which owns Coordinator-table state writes.
 *
 * <p>Methods here only touch user data tables via {@link DistributedStorage} and the {@link
 * MutationsGrouper} / {@link ParallelExecutor} stack. They never write to the Coordinator table.
 */
@ThreadSafe
class ParticipantCommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(ParticipantCommitHandler.class);

  private final DistributedStorage storage;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final MutationsGrouper mutationsGrouper;
  private final boolean onePhaseCommitEnabled;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  ParticipantCommitHandler(
      DistributedStorage storage,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean onePhaseCommitEnabled) {
    this.storage = checkNotNull(storage);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.mutationsGrouper = checkNotNull(mutationsGrouper);
    this.onePhaseCommitEnabled = onePhaseCommitEnabled;
  }

  void prepareRecords(TransactionContext context) throws PreparationException {
    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
      parallelExecutor.prepareRecords(tasks, context.transactionId);
    } catch (NoMutationException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
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

  void commitRecords(TransactionContext context) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(context.transactionId, tableMetadataManager);
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
}
