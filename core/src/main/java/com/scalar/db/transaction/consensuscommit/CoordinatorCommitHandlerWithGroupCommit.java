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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Group-commit-aware variant of {@link CoordinatorCommitHandler}. Routes {@code commitState} into
 * the group-commit emitter so that multiple transactions' COMMITTED states can be batched into one
 * Coordinator-table row.
 *
 * <p>The emitter combines the buffered, already-encoded write sets into one parent-row write set at
 * emit time, stamping each with its {@code child_id}. Conflict-resolution and abort helpers are
 * inherited from {@link CoordinatorCommitHandler}.
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
      CoordinatorGroupCommitter groupCommitter,
      boolean coordinatorWriteSetLoggingEnabled) {
    super(coordinator);
    this.groupCommitter = checkNotNull(groupCommitter);
    // The methods of this emitter will be called via GroupCommitter.ready(). The emitter respects
    // the same opt-in write-set logging gate as the non-group-commit path.
    groupCommitter.setEmitter(new Emitter(coordinator, coordinatorWriteSetLoggingEnabled));
  }

  /**
   * Group commits the COMMITTED state, buffering the transaction's id and pre-encoded {@code
   * writeSet} into its slot. Returns the emit-time {@code committedAt} stamped on the batched
   * Coordinator row, so the caller can stamp this transaction's committed data records with the
   * same value.
   */
  @Override
  long commitState(String id, @Nullable WriteSet writeSet)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      // ready() returns the emit-time committedAt from the Emitter. Although ready() is @Nullable,
      // the Emitter only returns null from emitNormalGroup when the group is empty; a slot that
      // reached ready() is a member of its group, so the group is non-empty at emit time and a
      // non-null committedAt is guaranteed.
      Long committedAt = groupCommitter.ready(id, new CoordinatorGroupCommitValue(id, writeSet));
      assert committedAt != null;

      logger.debug("Transaction {} is committed successfully at {}", id, committedAt);
      return committedAt;
    } catch (GroupCommitConflictException e) {
      cancelGroupCommit(id);
      return handleCommitConflict(id, e);
    } catch (GroupCommitException e) {
      cancelGroupCommit(id);
      Throwable cause = e.getCause();
      if (cause instanceof CoordinatorConflictException) {
        return handleCommitConflict(id, (CoordinatorConflictException) cause);
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
      cancelGroupCommit(id);
      throw new AssertionError("Group commit unexpectedly failed. TransactionID: " + id, e);
    }
  }

  /**
   * Releases the transaction's group-commit slot reservation. Called from the orchestrator's
   * abort/cleanup paths so an aborted transaction doesn't sit in the group buffer waiting for a
   * sibling.
   */
  void cancelGroupCommit(String id) {
    try {
      groupCommitter.remove(id);
    } catch (Exception e) {
      logger.warn(
          "Unexpectedly failed to remove the transaction ID from the group committer. ID: {}",
          id,
          e);
    }
  }

  @VisibleForTesting
  static class Emitter implements Emittable<String, String, CoordinatorGroupCommitValue, Long> {
    private final Coordinator coordinator;
    private final boolean coordinatorWriteSetLoggingEnabled;

    Emitter(Coordinator coordinator, boolean coordinatorWriteSetLoggingEnabled) {
      this.coordinator = coordinator;
      this.coordinatorWriteSetLoggingEnabled = coordinatorWriteSetLoggingEnabled;
    }

    @Override
    public Long emitNormalGroup(String parentId, List<CoordinatorGroupCommitValue> values)
        throws CoordinatorException {
      if (values.isEmpty()) {
        return null;
      }
      if (KEY_MANIPULATOR.isFullKey(parentId)) {
        throw new AssertionError(
            "emitNormalGroup is only for normal group commits that use a parent ID as the key");
      }
      // Resolve each member's child id from its carried full id, then hand the per-child
      // pre-encoded write sets to the encoder to assemble the merged parent-row write set. Child-id
      // derivation stays here so the encoder keeps no group-commit key-manipulator dependency.
      List<String> childIds = new ArrayList<>(values.size());
      List<WriteSetEncoder.ChildWriteSet> children = new ArrayList<>(values.size());
      for (CoordinatorGroupCommitValue value : values) {
        String childId = KEY_MANIPULATOR.keysFromFullKey(value.fullId).childKey;
        childIds.add(childId);
        children.add(new WriteSetEncoder.ChildWriteSet(childId, value.writeSet));
      }
      // Skip WriteSet encoding when write-set logging is disabled — the column is not part of the
      // Coordinator schema in that case. When enabled, merge the per-child pre-encoded write sets
      // into the parent row.
      WriteSet writeSet =
          coordinatorWriteSetLoggingEnabled ? WriteSetEncoder.mergeChildWriteSets(children) : null;
      // One committedAt for the whole batched row, returned so every transaction in the group
      // stamps its committed data records with the same value.
      long committedAt = System.currentTimeMillis();
      coordinator.putState(
          new State(parentId, childIds, writeSet, TransactionState.COMMITTED, committedAt));
      logger.debug(
          "Transaction {} (parent ID) is committed successfully at {}", parentId, committedAt);
      return committedAt;
    }

    @Override
    public Long emitDelayedGroup(String fullId, CoordinatorGroupCommitValue value)
        throws CoordinatorException {
      // Same opt-in gating as emitNormalGroup: persist the pre-encoded write set only when
      // write-set logging is enabled.
      WriteSet writeSet = coordinatorWriteSetLoggingEnabled ? value.writeSet : null;
      long committedAt = System.currentTimeMillis();
      coordinator.putState(new State(fullId, writeSet, TransactionState.COMMITTED, committedAt));
      logger.debug("Transaction {} is committed successfully at {}", fullId, committedAt);
      return committedAt;
    }
  }
}
