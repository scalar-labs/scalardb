package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Adapts a {@link DistributedTransactionManager} to the {@link GlobalTransactionManager} API,
 * backing the global transaction with a single (single-phase) distributed transaction.
 *
 * <p>This is the counterpart of {@link TwoPhaseCommitBackedGlobalTransactionManager}: where the
 * two-phase-commit backing suits a transaction spanning multiple participants (the Separated
 * deployment), this single-phase backing suits the fully-shared deployment, where every branch is
 * served by the same underlying data store and a single-phase commit is sufficient (and avoids the
 * unnecessary prepare phase that a two-phase commit would incur).
 *
 * <p>The mapping onto the global/branch roles:
 *
 * <ul>
 *   <li>{@code begin} begins a new distributed transaction via the manager and returns a {@link
 *       DistributedTransactionBackedGlobalTransaction} — the overall handle used to drive
 *       commit/rollback.
 *   <li>{@code beginBranch} begins a branch served by that same shared transaction, looked up by
 *       the global transaction ID, and returns a {@link
 *       DistributedTransactionBackedBranchTransaction} — the CRUD handle for that branch.
 * </ul>
 *
 * <p>All branches share the single underlying distributed transaction (one snapshot). This backing
 * therefore assumes the branches of a global transaction operate on disjoint data: under that
 * assumption, sharing one context has no functional impact, since there is no read/write
 * interference and the combined validation is equivalent to per-branch validation. The assumption
 * is a precondition on the caller rather than something this class enforces. Branches that do touch
 * the same records observe each other's uncommitted writes, unlike the per-participant snapshots
 * that {@link TwoPhaseCommitBackedGlobalTransactionManager} gives each branch. Sharing the
 * transaction also shares its thread-unsafety: every branch handle drives the same non-thread-safe
 * {@link com.scalar.db.api.DistributedTransaction DistributedTransaction}, so CRUD on different
 * branches of one global transaction must never run concurrently — the branches must be driven one
 * at a time, with the caller providing the happens-before when they hop threads. Likewise, calling
 * {@code beginBranch} again for the same transaction returns a new handle over that same shared
 * transaction — the {@link BranchTransaction#end(BranchTransaction.Status)} bookkeeping is per
 * handle — so begin each branch once and drive it through that one handle.
 *
 * <p>The shared transaction lives in the manager instance this backing wraps, so a global
 * transaction and all of its branches must be driven through that same instance — in practice,
 * within one process. This backing is for in-process orchestration; the separated, multi-process
 * arrangement is what {@link TwoPhaseCommitBackedGlobalTransactionManager} is for. (The current
 * implementation resolves the shared transaction via {@link
 * DistributedTransactionManager#join(String)}.)
 *
 * <p>The per-branch {@code attributes} passed to {@code beginBranch} are propagated client-side
 * into each CRUD operation issued on the branch (via {@link
 * AttributePropagatingBranchTransaction}), distinct from the transaction-level attributes supplied
 * to {@code begin}. They are held client-side on the branch handle only.
 */
@ThreadSafe
public class DistributedTransactionBackedGlobalTransactionManager
    implements GlobalTransactionManager {

  private final DistributedTransactionManager manager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DistributedTransactionBackedGlobalTransactionManager(
      DistributedTransactionManager manager) {
    this.manager = manager;
  }

  @Override
  public GlobalTransaction begin(Map<String, String> attributes) throws TransactionException {
    return new DistributedTransactionBackedGlobalTransaction(manager.begin(attributes));
  }

  @Override
  public GlobalTransaction beginReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return new DistributedTransactionBackedGlobalTransaction(manager.beginReadOnly(attributes));
  }

  @Override
  public BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionException {
    // Look up the shared underlying transaction by the global transaction ID (via the manager's
    // join) and front it with a branch handle; per-branch attributes are applied client-side by
    // AttributePropagatingBranchTransaction.
    BranchTransaction branch =
        new DistributedTransactionBackedBranchTransaction(manager.join(transactionId));
    return attributes.isEmpty()
        ? branch
        : new AttributePropagatingBranchTransaction(branch, attributes);
  }

  @Override
  public void close() {
    manager.close();
  }
}
