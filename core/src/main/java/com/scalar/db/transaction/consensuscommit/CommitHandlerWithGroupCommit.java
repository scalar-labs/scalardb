package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.util.groupcommit.Emittable;
import com.scalar.db.util.groupcommit.GroupCommitConflictException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.stream.Collectors;
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
      CoordinatorGroupCommitter groupCommitter) {
    super(storage, coordinator, tableMetadataManager, parallelExecutor);

    checkNotNull(groupCommitter);
    // The methods of this emitter will be called via GroupCommitter.ready().
    groupCommitter.setEmitter(new Emitter(coordinator));
    this.groupCommitter = groupCommitter;
  }

  @Override
  protected void onFailureBeforeCommit(Snapshot snapshot) {
    cancelGroupCommitIfNeeded(snapshot.getId());
  }

  private void commitStateViaGroupCommit(Snapshot snapshot)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      // Group commit the state by internally calling `groupCommitState()` via the emitter.
      groupCommitter.ready(id, snapshot);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (GroupCommitConflictException e) {
      cancelGroupCommitIfNeeded(id);
      // Throw a proper exception from this method if needed.
      handleCommitConflict(snapshot, e);
    } catch (GroupCommitException e) {
      cancelGroupCommitIfNeeded(id);
      Throwable cause = e.getCause();
      if (cause instanceof CoordinatorConflictException) {
        // Throw a proper exception from this method if needed.
        handleCommitConflict(snapshot, (CoordinatorConflictException) cause);
      } else {
        // Failed to access the coordinator state. The state is unknown.
        throw new UnknownTransactionStatusException("Coordinator status is unknown", cause, id);
      }
    } catch (Exception e) {
      // This is an unexpected exception, but clean up resources just in case.
      cancelGroupCommitIfNeeded(id);
      throw new AssertionError("Group commit unexpectedly failed. TransactionID:" + id, e);
    }
  }

  private void cancelGroupCommitIfNeeded(String id) {
    try {
      groupCommitter.remove(id);
    } catch (Exception e) {
      logger.warn(
          "Unexpectedly failed to remove the snapshot ID from the group committer. ID: {}", id);
    }
  }

  @Override
  public void commitState(Snapshot snapshot)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateViaGroupCommit(snapshot);
  }

  @Override
  public TransactionState abortState(String id) throws UnknownTransactionStatusException {
    cancelGroupCommitIfNeeded(id);
    return super.abortState(id);
  }

  private static class Emitter implements Emittable<String, String, Snapshot> {
    private final Coordinator coordinator;

    public Emitter(Coordinator coordinator) {
      this.coordinator = coordinator;
    }

    @Override
    public void emitNormalGroup(String parentId, List<Snapshot> snapshots)
        throws CoordinatorException {
      if (snapshots.isEmpty()) {
        // This means all buffered transactions were manually rolled back. Nothing to do.
        return;
      }

      // These transactions are contained in a normal group that has multiple transactions.
      // Therefore, the transaction states should be put together in Coordinator.State.
      List<String> transactionIds =
          snapshots.stream().map(Snapshot::getId).collect(Collectors.toList());

      coordinator.putStateForGroupCommit(
          parentId, transactionIds, TransactionState.COMMITTED, System.currentTimeMillis());

      logger.debug(
          "Transaction {} (parent ID) is committed successfully at {}",
          parentId,
          System.currentTimeMillis());
    }

    @Override
    public void emitDelayedGroup(String fullId, Snapshot snapshot) throws CoordinatorException {
      // This transaction is contained in a delayed group that has only a single transaction.
      // Therefore, the transaction state can be committed as if it's a normal commit (not a
      // group commit).
      coordinator.putState(new State(fullId, TransactionState.COMMITTED));

      logger.debug(
          "Transaction {} is committed successfully at {}", fullId, System.currentTimeMillis());
    }
  }
}
