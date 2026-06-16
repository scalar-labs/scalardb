package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.groupcommit.Emittable;
import com.scalar.db.util.groupcommit.GroupCommitConflictException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandlerWithGroupCommit extends CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandlerWithGroupCommit.class);
  private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandlerWithGroupCommit(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean onePhaseCommitEnabled,
      boolean txWriteSetIncludeColumnsEnabled,
      CoordinatorGroupCommitter groupCommitter) {
    super(
        storage,
        coordinator,
        tableMetadataManager,
        parallelExecutor,
        mutationsGrouper,
        coordinatorWriteOmissionOnReadOnlyEnabled,
        onePhaseCommitEnabled,
        txWriteSetIncludeColumnsEnabled);
    checkNotNull(groupCommitter);
    // The methods of this emitter will be called via GroupCommitter.ready().
    groupCommitter.setEmitter(
        new Emitter(coordinator, writeSetEncoder, txWriteSetIncludeColumnsEnabled));
    this.groupCommitter = groupCommitter;
  }

  @Override
  public void commit(TransactionContext context)
      throws CommitException, UnknownTransactionStatusException {
    if (!context.readOnly
        && !context.snapshot.hasWritesOrDeletes()
        && coordinatorWriteOmissionOnReadOnlyEnabled) {
      cancelGroupCommitIfNeeded(context);
    }

    super.commit(context);
  }

  @Override
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    try {
      return super.canOnePhaseCommit(context);
    } catch (CommitException e) {
      cancelGroupCommitIfNeeded(context);
      throw e;
    }
  }

  @Override
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    cancelGroupCommitIfNeeded(context);
    super.onePhaseCommitRecords(context);
  }

  @Override
  protected void onFailureBeforeCommit(TransactionContext context) {
    cancelGroupCommitIfNeeded(context);
  }

  private void commitStateViaGroupCommit(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      // Group commit the state by internally calling `groupCommitState()` via the emitter.
      groupCommitter.ready(id, context);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (GroupCommitConflictException e) {
      cancelGroupCommitIfNeeded(context);
      // Throw a proper exception from this method if needed.
      handleCommitConflict(context, e);
    } catch (GroupCommitException e) {
      cancelGroupCommitIfNeeded(context);
      Throwable cause = e.getCause();
      if (cause instanceof CoordinatorConflictException) {
        // Throw a proper exception from this method if needed.
        handleCommitConflict(context, (CoordinatorConflictException) cause);
      } else {
        // Failed to access the coordinator state. The state is unknown.
        throw new UnknownTransactionStatusException("Coordinator status is unknown", cause, id);
      }
    } catch (Exception e) {
      // This is an unexpected exception, but clean up resources just in case.
      cancelGroupCommitIfNeeded(context);
      throw new AssertionError("Group commit unexpectedly failed. TransactionID: " + id, e);
    }
  }

  private void cancelGroupCommitIfNeeded(TransactionContext context) {
    // A transaction only holds a group commit slot when one was reserved at begin time (e.g., a
    // read-only transaction does not reserve a slot when coordinator write omission is enabled).
    // When no slot was reserved there is nothing to release.
    if (!context.groupCommitSlotReserved) {
      return;
    }

    // FIXME: When a slot was reserved (so the early return above did not fire), this can run more
    // than once on a single failure path, because several cleanup callbacks each release the slot:
    //   - onFailureBeforeCommit() runs on every failure path (via safelyCallOnFailureBeforeCommit);
    //   - abortState() additionally runs from abortStateAndRollbackRecordsIfNeeded() when the
    //     transaction has writes/deletes, or coordinator write omission on read-only is disabled;
    //   - onePhaseCommitRecords() pre-cancels the slot before its body, so a one-phase-commit
    //     failure followed by onFailureBeforeCommit() also cancels twice.
    // It is currently safe only because groupCommitter.remove() is idempotent. The cleanup paths
    // should be reworked so the slot is released exactly once instead of relying on that
    // idempotency.
    try {
      groupCommitter.remove(context.transactionId);
    } catch (Exception e) {
      logger.warn(
          "Unexpectedly failed to remove the snapshot ID from the group committer. ID: {}",
          context.transactionId,
          e);
    }
  }

  @Override
  public void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateViaGroupCommit(context);
  }

  @Override
  public TransactionState abortState(TransactionContext context)
      throws UnknownTransactionStatusException {
    cancelGroupCommitIfNeeded(context);
    return super.abortState(context);
  }

  @VisibleForTesting
  static class Emitter implements Emittable<String, String, TransactionContext> {
    private final Coordinator coordinator;
    private final WriteSetEncoder writeSetEncoder;
    private final boolean txWriteSetIncludeColumnsEnabled;

    public Emitter(
        Coordinator coordinator,
        WriteSetEncoder writeSetEncoder,
        boolean txWriteSetIncludeColumnsEnabled) {
      this.coordinator = coordinator;
      this.writeSetEncoder = writeSetEncoder;
      this.txWriteSetIncludeColumnsEnabled = txWriteSetIncludeColumnsEnabled;
    }

    @Override
    public void emitNormalGroup(String parentId, List<TransactionContext> contexts)
        throws CoordinatorException {
      if (contexts.isEmpty()) {
        // This means all buffered transactions were manually rolled back. Nothing to do.
        return;
      }

      // These transactions are contained in a normal group that has multiple transactions.
      // Therefore, the transaction states should be put together in Coordinator.State.
      List<String> transactionIds = new ArrayList<>(contexts.size());
      for (TransactionContext context : contexts) {
        transactionIds.add(context.transactionId);
      }

      coordinator.putStateForGroupCommit(
          parentId,
          transactionIds,
          writeSetEncoder.encodeMultiGroupWriteSet(contexts, txWriteSetIncludeColumnsEnabled),
          TransactionState.COMMITTED,
          System.currentTimeMillis());

      logger.debug(
          "Transaction {} (parent ID) is committed successfully at {}",
          parentId,
          System.currentTimeMillis());
    }

    @Override
    public void emitDelayedGroup(String fullId, TransactionContext context)
        throws CoordinatorException {
      // This transaction is contained in a delayed group that has only a single transaction.
      // Therefore, the transaction state can be committed as if it's a normal commit (not a
      // group commit).
      coordinator.putState(
          new State(
              fullId,
              writeSetEncoder.encodeSingleGroupWriteSet(context, txWriteSetIncludeColumnsEnabled),
              TransactionState.COMMITTED));

      logger.debug(
          "Transaction {} is committed successfully at {}", fullId, System.currentTimeMillis());
    }
  }
}
