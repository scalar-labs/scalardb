package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.groupcommit.Emittable;
import com.scalar.db.util.groupcommit.GroupCommitConflictException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Group-commit-aware variant of {@link CoordinatorCommitHandler}. Routes {@code commitState} into
 * the group-commit emitter so that multiple transactions' COMMITTED states can be batched into one
 * Coordinator-table row. Cancels the group commit slot reservation on the abort path.
 */
@ThreadSafe
class CoordinatorCommitHandlerWithGroupCommit extends CoordinatorCommitHandler {
  private static final Logger logger =
      LoggerFactory.getLogger(CoordinatorCommitHandlerWithGroupCommit.class);
  private static final CoordinatorGroupCommitKeyManipulator KEY_MANIPULATOR =
      new CoordinatorGroupCommitKeyManipulator();

  private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CoordinatorCommitHandlerWithGroupCommit(
      Coordinator coordinator,
      WriteSetEncoder writeSetEncoder,
      CoordinatorGroupCommitter groupCommitter,
      boolean coordinatorWriteSetLoggingEnabled) {
    super(coordinator, writeSetEncoder, coordinatorWriteSetLoggingEnabled);
    checkNotNull(groupCommitter);
    // The methods of this emitter will be called via GroupCommitter.ready(). The emitter respects
    // the same opt-in write-set logging gate as the non-group-commit path.
    groupCommitter.setEmitter(
        new Emitter(coordinator, writeSetEncoder, coordinatorWriteSetLoggingEnabled));
    this.groupCommitter = groupCommitter;
  }

  @Override
  void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      // Group commit the state by internally calling `groupCommitState()` via the emitter.
      groupCommitter.ready(id, context);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (GroupCommitConflictException e) {
      cancelGroupCommitIfNeeded(context);
      handleCommitConflict(context, e);
    } catch (GroupCommitException e) {
      cancelGroupCommitIfNeeded(context);
      Throwable cause = e.getCause();
      if (cause instanceof CoordinatorConflictException) {
        handleCommitConflict(context, (CoordinatorConflictException) cause);
      } else {
        // Failed to access the coordinator state. The state is unknown.
        throw new UnknownTransactionStatusException(
            CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(
                cause == null ? "" : cause.getMessage()),
            cause,
            id);
      }
    } catch (Exception e) {
      // This is an unexpected exception, but clean up resources just in case.
      cancelGroupCommitIfNeeded(context);
      throw new AssertionError("Group commit unexpectedly failed. TransactionID: " + id, e);
    }
  }

  @Override
  TransactionState abortState(TransactionContext context) throws UnknownTransactionStatusException {
    cancelGroupCommitIfNeeded(context);
    return super.abortState(context);
  }

  /**
   * Releases the group-commit slot reservation if one was made for this transaction. Called from
   * the abort/cleanup paths so an aborted transaction doesn't sit in the group buffer waiting for a
   * sibling.
   */
  void cancelGroupCommitIfNeeded(TransactionContext context) {
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

  @VisibleForTesting
  static class Emitter implements Emittable<String, String, TransactionContext, Void> {
    private final Coordinator coordinator;
    private final WriteSetEncoder writeSetEncoder;
    private final boolean coordinatorWriteSetLoggingEnabled;

    Emitter(
        Coordinator coordinator,
        WriteSetEncoder writeSetEncoder,
        boolean coordinatorWriteSetLoggingEnabled) {
      this.coordinator = coordinator;
      this.writeSetEncoder = writeSetEncoder;
      this.coordinatorWriteSetLoggingEnabled = coordinatorWriteSetLoggingEnabled;
    }

    @Override
    public Void emitNormalGroup(String parentId, List<TransactionContext> contexts)
        throws CoordinatorException {
      if (contexts.isEmpty()) {
        return null;
      }
      if (KEY_MANIPULATOR.isFullKey(parentId)) {
        throw new AssertionError(
            "emitNormalGroup is only for normal group commits that use a parent ID as the key");
      }
      List<String> childIds = new ArrayList<>(contexts.size());
      for (TransactionContext context : contexts) {
        childIds.add(KEY_MANIPULATOR.keysFromFullKey(context.transactionId).childKey);
      }
      // Skip WriteSet encoding when write-set logging is disabled — the column is not part of the
      // Coordinator schema in that case.
      WriteSet writeSet =
          coordinatorWriteSetLoggingEnabled
              ? writeSetEncoder.encodeMultiGroupWriteSet(contexts, false)
              : null;
      coordinator.putState(
          new State(
              parentId,
              childIds,
              writeSet,
              TransactionState.COMMITTED,
              System.currentTimeMillis()));
      logger.debug(
          "Transaction {} (parent ID) is committed successfully at {}",
          parentId,
          System.currentTimeMillis());
      return null;
    }

    @Override
    public Void emitDelayedGroup(String fullId, TransactionContext context)
        throws CoordinatorException {
      // Same opt-in gating as emitNormalGroup.
      WriteSet writeSet =
          coordinatorWriteSetLoggingEnabled
              ? writeSetEncoder.encodeSingleGroupWriteSet(context, false)
              : null;
      coordinator.putState(
          new State(fullId, writeSet, TransactionState.COMMITTED, System.currentTimeMillis()));
      logger.debug(
          "Transaction {} is committed successfully at {}", fullId, System.currentTimeMillis());
      return null;
    }
  }
}
