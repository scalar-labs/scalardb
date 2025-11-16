package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.ConditionSetBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  protected final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final MutationsGrouper mutationsGrouper;
  protected final boolean coordinatorWriteOmissionOnReadOnlyEnabled;
  private final boolean onePhaseCommitEnabled;

  @LazyInit @Nullable private BeforePreparationHook beforePreparationHook;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean onePhaseCommitEnabled) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.mutationsGrouper = checkNotNull(mutationsGrouper);
    this.coordinatorWriteOmissionOnReadOnlyEnabled = coordinatorWriteOmissionOnReadOnlyEnabled;
    this.onePhaseCommitEnabled = onePhaseCommitEnabled;
  }

  /**
   * A callback invoked when any exception occurs before committing transactions.
   *
   * @param context the transaction context
   */
  protected void onFailureBeforeCommit(TransactionContext context) {}

  private void safelyCallOnFailureBeforeCommit(TransactionContext context) {
    try {
      onFailureBeforeCommit(context);
    } catch (Exception e) {
      logger.warn("Failed to call the callback. Transaction ID: {}", context.transactionId, e);
    }
  }

  private Optional<Future<Void>> invokeBeforePreparationHook(TransactionContext context)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHook == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(beforePreparationHook.handle(tableMetadataManager, context));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);
      abortState(context.transactionId);
      rollbackRecords(context);
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  private void waitBeforePreparationHookFuture(
      TransactionContext context, @Nullable Future<Void> beforePreparationHookFuture)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHookFuture == null) {
      return;
    }

    try {
      beforePreparationHookFuture.get();
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);
      abortState(context.transactionId);
      rollbackRecords(context);
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  public void commit(TransactionContext context)
      throws CommitException, UnknownTransactionStatusException {
    boolean hasWritesOrDeletesInSnapshot =
        !context.readOnly && context.snapshot.hasWritesOrDeletes();

    Optional<Future<Void>> beforePreparationHookFuture = invokeBeforePreparationHook(context);

    ValidationInfo validationInfo = buildValidationInfo(context);

    if (canOnePhaseCommit(validationInfo, context)) {
      try {
        onePhaseCommitRecords(context);
        return;
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    if (hasWritesOrDeletesInSnapshot) {
      try {
        prepareRecords(context);
      } catch (PreparationException e) {
        safelyCallOnFailureBeforeCommit(context);
        abortState(context.transactionId);
        rollbackRecords(context);
        if (e instanceof PreparationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    if (validationInfo.isActuallyValidationRequired()) {
      try {
        validateRecords(validationInfo, context.transactionId);
      } catch (ValidationException e) {
        safelyCallOnFailureBeforeCommit(context);

        // If the transaction has no writes and deletes, we don't need to abort-state and
        // rollback-records since there are no changes to be made.
        if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
          abortState(context.transactionId);
        }
        if (hasWritesOrDeletesInSnapshot) {
          rollbackRecords(context);
        }

        if (e instanceof ValidationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    waitBeforePreparationHookFuture(context, beforePreparationHookFuture.orElse(null));

    if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
      commitState(context);
    }
    if (hasWritesOrDeletesInSnapshot) {
      commitRecords(context);
    }
  }

  @VisibleForTesting
  boolean canOnePhaseCommit(ValidationInfo validationInfo, TransactionContext context)
      throws CommitException {
    if (!onePhaseCommitEnabled) {
      return false;
    }

    // If validation is required, we cannot one-phase commit the transaction
    if (validationInfo.isActuallyValidationRequired()) {
      return false;
    }

    // If the snapshot has no write and deletes, we do not one-phase commit the transaction
    if (!context.snapshot.hasWritesOrDeletes()) {
      return false;
    }

    Collection<Map.Entry<Snapshot.Key, Delete>> deleteSetEntries = context.snapshot.getDeleteSet();

    // If a record corresponding to a delete in the delete set does not exist in the storage,　we
    // cannot one-phase commit the transaction. This is because the storage does not support
    // delete-if-not-exists semantics, so we cannot detect conflicts with other transactions.
    for (Map.Entry<Snapshot.Key, Delete> entry : deleteSetEntries) {
      Delete delete = entry.getValue();
      Optional<TransactionResult> result =
          context.snapshot.getFromReadSet(new Snapshot.Key(delete));

      // For deletes, we always perform implicit pre-reads if the result does not exit in the read
      // set. So the result should always exist in the read set.
      assert result != null;

      if (!result.isPresent()) {
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

  private ValidationInfo buildValidationInfo(TransactionContext context) {
    if (context.isValidationRequired()) {
      Set<Snapshot.Key> updatedRecordKeys =
          Stream.concat(
                  context.snapshot.getWriteSet().stream().map(Map.Entry::getKey),
                  context.snapshot.getDeleteSet().stream().map(Map.Entry::getKey))
              .collect(Collectors.toSet());

      Collection<Map.Entry<Get, Optional<TransactionResult>>> getSet = new ArrayList<>();
      for (Map.Entry<Get, Optional<TransactionResult>> geSetEntry : context.snapshot.getGetSet()) {
        // We don't need to validate the record in the write set or delete set
        Snapshot.Key key = new Snapshot.Key(geSetEntry.getKey());
        if (!updatedRecordKeys.contains(key)) {
          getSet.add(geSetEntry);
        }
      }

      Collection<Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>>> scanSet =
          context.snapshot.getScanSet();
      Collection<Snapshot.ScannerInfo> scannerSet = context.snapshot.getScannerSet();

      return new ValidationInfo(getSet, scanSet, scannerSet, updatedRecordKeys);
    }

    return new ValidationInfo();
  }

  @VisibleForTesting
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

  public void prepareRecords(TransactionContext context) throws PreparationException {
    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
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

  public void validateRecords(TransactionContext context) throws ValidationException {
    ValidationInfo validationInfo = buildValidationInfo(context);
    if (!validationInfo.isActuallyValidationRequired()) {
      return;
    }

    validateRecords(validationInfo, context.transactionId);
  }

  private void validateRecords(ValidationInfo validationInfo, String transactionId)
      throws ValidationException {
    try {
      toSerializable(validationInfo, transactionId);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(e.getMessage()),
          e,
          transactionId);
    }
  }

  @VisibleForTesting
  void toSerializable(ValidationInfo validationInfo, String transactionId)
      throws ExecutionException, ValidationConflictException {
    List<ParallelExecutorTask> tasks = new ArrayList<>();

    // Scan set is re-validated to check if there is no anti-dependency
    for (Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> entry :
        validationInfo.scanSet) {
      tasks.add(
          () ->
              validateScanResults(
                  storage,
                  entry.getKey(),
                  entry.getValue(),
                  false,
                  validationInfo.updatedRecordKeys,
                  transactionId));
    }

    // Scanner set is re-validated to check if there is no anti-dependency
    for (Snapshot.ScannerInfo scannerInfo : validationInfo.scannerSet) {
      tasks.add(
          () ->
              validateScanResults(
                  storage,
                  scannerInfo.scan,
                  scannerInfo.results,
                  true,
                  validationInfo.updatedRecordKeys,
                  transactionId));
    }

    // Get set is re-validated to check if there is no anti-dependency
    for (Map.Entry<Get, Optional<TransactionResult>> entry : validationInfo.getSet) {
      Get get = entry.getKey();
      TableMetadata metadata = getTableMetadata(get);

      if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata)) {
        // For Get with index
        tasks.add(
            () ->
                validateGetWithIndexResult(
                    storage,
                    get,
                    entry.getValue(),
                    validationInfo.updatedRecordKeys,
                    transactionId,
                    metadata));
      } else {
        // For other Get
        tasks.add(() -> validateGetResult(storage, get, entry.getValue(), transactionId, metadata));
      }
    }

    parallelExecutor.validateRecords(tasks, transactionId);
  }

  /**
   * Validates the scan results to check if there is no anti-dependency.
   *
   * <p>This method scans the latest data and compares it with the scan results. If there is a
   * discrepancy, it means that the scan results are changed by another transaction. In this case,
   * an {@link ValidationConflictException} is thrown.
   *
   * <p>Since the validation is performed after the prepare-record phase, the scan might include
   * prepared records if the transaction has performed operations that affect the scan result. In
   * such cases, those prepared records can be safely ignored.
   *
   * <p>Note that this logic is based on the assumption that identical scans return results in the
   * same order, provided that the underlying data remains unchanged.
   *
   * @param storage a distributed storage
   * @param scan the scan to be validated
   * @param results the results of the scan
   * @param notFullyScannedScanner if this is a validation for a scanner that has not been fully
   *     scanned
   * @param transactionId the transaction ID
   * @throws ExecutionException if a storage operation fails
   * @throws ValidationConflictException if the scan results are changed by another transaction
   */
  private void validateScanResults(
      DistributedStorage storage,
      Scan scan,
      LinkedHashMap<Snapshot.Key, TransactionResult> results,
      boolean notFullyScannedScanner,
      Set<Snapshot.Key> updatedRecordKeys,
      String transactionId)
      throws ExecutionException, ValidationConflictException {
    Scanner scanner = null;
    try {
      TableMetadata metadata = getTableMetadata(scan);

      scanner = storage.scan(ConsensusCommitUtils.prepareScanForStorage(scan, metadata));

      // Initialize the iterator for the latest scan results
      Optional<Result> latestResult = getNextResult(scanner, scan);

      // Initialize the iterator for the original scan results
      Iterator<Map.Entry<Snapshot.Key, TransactionResult>> originalResultIterator =
          results.entrySet().iterator();
      Map.Entry<Snapshot.Key, TransactionResult> originalResultEntry =
          Iterators.getNext(originalResultIterator, null);

      // Compare the records of the iterators
      while (latestResult.isPresent() && originalResultEntry != null) {
        TransactionResult latestTxResult = new TransactionResult(latestResult.get());
        Snapshot.Key key = new Snapshot.Key(scan, latestTxResult, metadata);

        if (latestTxResult.getId() != null && latestTxResult.getId().equals(transactionId)) {
          // The record is inserted/deleted/updated by this transaction

          // Skip the record of the latest scan results
          latestResult = getNextResult(scanner, scan);

          if (originalResultEntry.getKey().equals(key)) {
            // The record is updated by this transaction

            // Skip the record of the original scan results
            originalResultEntry = Iterators.getNext(originalResultIterator, null);
          } else {
            // The record is inserted/deleted by this transaction
          }

          continue;
        }

        // Compare the records of the original scan results and the latest scan results
        if (!originalResultEntry.getKey().equals(key)) {
          if (updatedRecordKeys.contains(originalResultEntry.getKey())) {
            // The record is inserted/deleted/updated by this transaction

            // Skip the record of the original scan results
            originalResultEntry = Iterators.getNext(originalResultIterator, null);
            continue;
          }

          // The record is inserted/deleted by another transaction
          throwExceptionDueToAntiDependency(transactionId);
        }
        if (isChanged(latestTxResult, originalResultEntry.getValue())) {
          // The record is updated by another transaction
          throwExceptionDueToAntiDependency(transactionId);
        }

        // Proceed to the next record
        latestResult = getNextResult(scanner, scan);
        originalResultEntry = Iterators.getNext(originalResultIterator, null);
      }

      while (originalResultEntry != null) {
        if (updatedRecordKeys.contains(originalResultEntry.getKey())) {
          // The record is inserted/deleted/updated by this transaction

          // Skip the record of the original scan results
          originalResultEntry = Iterators.getNext(originalResultIterator, null);
        } else {
          // The record is inserted/deleted by another transaction
          throwExceptionDueToAntiDependency(transactionId);
        }
      }

      if (!latestResult.isPresent()) {
        return;
      }

      if (scan.getLimit() != 0 && results.size() == scan.getLimit()) {
        // We’ve already checked up to the limit, so no further checks are needed
        return;
      }

      if (notFullyScannedScanner) {
        // If the scanner is not fully scanned, no further checks are needed
        return;
      }

      // Check if there are any remaining records in the latest scan results
      while (latestResult.isPresent()) {
        TransactionResult latestTxResult = new TransactionResult(latestResult.get());

        if (latestTxResult.getId() != null && latestTxResult.getId().equals(transactionId)) {
          // The record is inserted/deleted by this transaction

          // Skip the record
          latestResult = getNextResult(scanner, scan);
        } else {
          // The record is inserted by another transaction
          throwExceptionDueToAntiDependency(transactionId);
        }
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          logger.warn("Failed to close the scanner. Transaction ID: {}", transactionId, e);
        }
      }
    }
  }

  private Optional<Result> getNextResult(Scanner scanner, Scan scan) throws ExecutionException {
    Optional<Result> next = scanner.one();
    if (!next.isPresent()) {
      return next;
    }

    if (!scan.getConjunctions().isEmpty()) {
      // Because we also get records whose before images match the conjunctions, we need to check if
      // the current status of the records actually match the conjunctions.
      next =
          next.filter(
              r ->
                  ScalarDbUtils.columnsMatchAnyOfConjunctions(
                      r.getColumns(), scan.getConjunctions()));
    }

    return next.isPresent() ? next : getNextResult(scanner, scan);
  }

  private void validateGetWithIndexResult(
      DistributedStorage storage,
      Get get,
      Optional<TransactionResult> originalResult,
      Set<Snapshot.Key> updatedRecordKeys,
      String transactionId,
      TableMetadata metadata)
      throws ExecutionException, ValidationConflictException {
    assert get.forNamespace().isPresent() && get.forTable().isPresent();

    // If this transaction or another transaction inserts records into the index range,
    // the Get with index operation may retrieve multiple records, which would result in
    // an IllegalArgumentException. Therefore, we use Scan with index instead.
    Scan scanWithIndex =
        Scan.newBuilder()
            .namespace(get.forNamespace().get())
            .table(get.forTable().get())
            .indexKey(get.getPartitionKey())
            .whereOr(
                get.getConjunctions().stream()
                    .map(c -> ConditionSetBuilder.andConditionSet(c.getConditions()).build())
                    .collect(Collectors.toSet()))
            .consistency(get.getConsistency())
            .attributes(get.getAttributes())
            .build();

    LinkedHashMap<Snapshot.Key, TransactionResult> results = new LinkedHashMap<>(1);
    originalResult.ifPresent(r -> results.put(new Snapshot.Key(scanWithIndex, r, metadata), r));

    // Validate the result to check if there is no anti-dependency
    validateScanResults(storage, scanWithIndex, results, false, updatedRecordKeys, transactionId);
  }

  private void validateGetResult(
      DistributedStorage storage,
      Get get,
      Optional<TransactionResult> originalResult,
      String transactionId,
      TableMetadata metadata)
      throws ExecutionException, ValidationConflictException {
    // Check if a read record is not changed
    Optional<TransactionResult> latestResult =
        storage
            .get(ConsensusCommitUtils.prepareGetForStorage(get, metadata))
            .map(TransactionResult::new);

    if (!get.getConjunctions().isEmpty()) {
      // Because we also get records whose before images match the conjunctions, we need to check if
      // the current status of the records actually match the conjunctions.
      latestResult =
          latestResult.filter(
              r ->
                  ScalarDbUtils.columnsMatchAnyOfConjunctions(
                      r.getColumns(), get.getConjunctions()));
    }

    if (isChanged(latestResult, originalResult)) {
      throwExceptionDueToAntiDependency(transactionId);
    }
  }

  private TableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    TransactionTableMetadata transactionTableMetadata =
        getTransactionTableMetadata(tableMetadataManager, operation);
    return transactionTableMetadata.getTableMetadata();
  }

  private boolean isChanged(
      Optional<TransactionResult> latestResult, Optional<TransactionResult> result) {
    if (latestResult.isPresent() != result.isPresent()) {
      return true;
    }
    if (!latestResult.isPresent()) {
      return false;
    }
    return isChanged(latestResult.get(), result.get());
  }

  private boolean isChanged(TransactionResult latestResult, TransactionResult result) {
    return !Objects.equals(latestResult.getId(), result.getId());
  }

  private void throwExceptionDueToAntiDependency(String transactionId)
      throws ValidationConflictException {
    throw new ValidationConflictException(
        CoreError.CONSENSUS_COMMIT_ANTI_DEPENDENCY_FOUND.buildMessage(), transactionId);
  }

  public void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      handleCommitConflict(context, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  protected void handleCommitConflict(TransactionContext context, Exception cause)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      Optional<State> s = coordinator.getState(context.transactionId);
      if (s.isPresent()) {
        TransactionState state = s.get().getState();
        if (state.equals(TransactionState.ABORTED)) {
          rollbackRecords(context);
          throw new CommitConflictException(
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                  cause.getMessage()),
              cause,
              context.transactionId);
        }
      } else {
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(cause.getMessage()),
            cause,
            context.transactionId);
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(ex.getMessage()),
          ex,
          context.transactionId);
    }
  }

  public void commitRecords(TransactionContext context) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.commitRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Committing records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  public TransactionState abortState(String id) throws UnknownTransactionStatusException {
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      try {
        Optional<Coordinator.State> state = coordinator.getState(id);
        if (state.isPresent()) {
          // successfully COMMITTED or ABORTED
          return state.get().getState();
        }
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_ABORTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(e.getMessage()),
            e,
            id);
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException(
            CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(e1.getMessage()),
            e1,
            id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  public void rollbackRecords(TransactionContext context) {
    logger.debug("Rollback from snapshot for {}", context.transactionId);
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(context.transactionId, storage, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.rollbackRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Rolling back records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  /**
   * Sets the {@link BeforePreparationHook}. This method must be called immediately after the
   * constructor is invoked.
   *
   * @param beforePreparationHook The before-preparation hook to set.
   * @throws NullPointerException If the argument is null.
   */
  public void setBeforePreparationHook(BeforePreparationHook beforePreparationHook) {
    this.beforePreparationHook = checkNotNull(beforePreparationHook);
  }

  static class ValidationInfo {
    public final Collection<Map.Entry<Get, Optional<TransactionResult>>> getSet;
    public final Collection<Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>>>
        scanSet;
    public final Collection<Snapshot.ScannerInfo> scannerSet;
    public final Set<Snapshot.Key> updatedRecordKeys;

    ValidationInfo() {
      this.getSet = new ArrayList<>();
      this.scanSet = new ArrayList<>();
      this.scannerSet = new ArrayList<>();
      this.updatedRecordKeys = new HashSet<>();
    }

    ValidationInfo(
        Collection<Map.Entry<Get, Optional<TransactionResult>>> getSet,
        Collection<Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>>> scanSet,
        Collection<Snapshot.ScannerInfo> scannerSet,
        Set<Snapshot.Key> updatedRecordKeys) {
      this.getSet = getSet;
      this.scanSet = scanSet;
      this.scannerSet = scannerSet;
      this.updatedRecordKeys = updatedRecordKeys;
    }

    boolean isActuallyValidationRequired() {
      return !getSet.isEmpty() || !scanSet.isEmpty() || !scannerSet.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ValidationInfo)) {
        return false;
      }
      ValidationInfo that = (ValidationInfo) o;
      return Iterables.elementsEqual(getSet, that.getSet)
          && Iterables.elementsEqual(scanSet, that.scanSet)
          && Iterables.elementsEqual(scannerSet, that.scannerSet)
          && Iterables.elementsEqual(updatedRecordKeys, that.updatedRecordKeys);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getSet, scanSet, scannerSet, updatedRecordKeys);
    }
  }
}
