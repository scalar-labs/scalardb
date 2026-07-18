package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;

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
 *   <li>{@code beginGlobal} begins a new distributed transaction via the manager and returns a
 *       {@link DistributedTransactionBackedGlobalTransaction} — the overall handle used to drive
 *       commit/rollback.
 *   <li>{@code beginBranch} joins the already-begun distributed transaction by its ID (via {@link
 *       DistributedTransactionManager#join(String)}) and returns a {@link
 *       DistributedTransactionBackedBranchTransaction} — the CRUD handle for that branch.
 * </ul>
 *
 * <p>All branches share the single underlying distributed transaction (one snapshot). This backing
 * therefore assumes the branches of a global transaction operate on disjoint data: under that
 * assumption, sharing one context has no functional impact, since there is no read/write
 * interference and the combined validation is equivalent to per-branch validation. The assumption
 * is a precondition on the caller rather than something this class enforces. Branches that do touch
 * the same records observe each other's uncommitted writes, unlike the per-participant snapshots
 * that {@link TwoPhaseCommitBackedGlobalTransactionManager} gives each branch.
 *
 * <p>Because branches join by transaction ID, this backing requires a {@link
 * DistributedTransactionManager} whose {@link DistributedTransactionManager#join(String)} resolves
 * a transaction begun elsewhere. Running branches in separate processes therefore requires an
 * implementation that can resolve the transaction from the process issuing the join.
 *
 * <p>The per-branch {@code attributes} passed to {@code beginBranch} are propagated client-side
 * into each CRUD operation issued on the branch (via {@link
 * AttributePropagatingBranchTransaction}), distinct from the transaction-level attributes supplied
 * to {@code beginGlobal}. They are not sent to {@link DistributedTransactionManager#join(String)},
 * which takes no attributes.
 */
public class DistributedTransactionBackedGlobalTransactionManager
    implements GlobalTransactionManager {

  private final DistributedTransactionManager manager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DistributedTransactionBackedGlobalTransactionManager(
      DistributedTransactionManager manager) {
    this.manager = manager;
  }

  @Override
  public GlobalTransaction beginGlobal(Map<String, String> attributes) throws TransactionException {
    return new DistributedTransactionBackedGlobalTransaction(manager.begin(attributes));
  }

  @Override
  public GlobalTransaction beginGlobalReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return new DistributedTransactionBackedGlobalTransaction(manager.beginReadOnly(attributes));
  }

  @Override
  public BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionException {
    // Join the already-begun global transaction by its ID. All branches share the single underlying
    // distributed transaction. The per-branch attributes are propagated client-side into each CRUD
    // operation by AttributePropagatingBranchTransaction; they are not sent to join (which carries
    // no attributes) and differ from the transaction-scoped attributes supplied at beginGlobal.
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
