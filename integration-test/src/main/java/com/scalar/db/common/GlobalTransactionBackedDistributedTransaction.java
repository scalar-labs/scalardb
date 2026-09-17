package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-only adapter that presents a {@link GlobalTransaction} (coordinator side) plus a {@link
 * BranchTransaction} (participant side) as a single-phase {@link
 * com.scalar.db.api.DistributedTransaction}.
 *
 * <p>CRUD is delegated to the branch (keyed by the transaction ID); {@link #commit()} and {@link
 * #rollback()} drive the global transaction. The two-phase nature is fully internalized behind the
 * single-phase API, which lets the existing {@code DistributedTransaction} integration-test corpus
 * run against the {@link com.scalar.db.api.GlobalTransactionManager} / {@link GlobalTransaction} /
 * {@link BranchTransaction} layer. It is not intended for production use.
 *
 * <p>See {@link GlobalTransactionBackedDistributedTransactionManager} for how the global
 * transaction and its branch are begun.
 */
public class GlobalTransactionBackedDistributedTransaction extends AbstractDistributedTransaction {
  private static final Logger logger =
      LoggerFactory.getLogger(GlobalTransactionBackedDistributedTransaction.class);

  private final GlobalTransaction globalTransaction;
  private final BranchTransaction branchTransaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public GlobalTransactionBackedDistributedTransaction(
      GlobalTransaction globalTransaction, BranchTransaction branchTransaction) {
    this.globalTransaction = globalTransaction;
    this.branchTransaction = branchTransaction;
  }

  @Override
  public String getId() {
    return globalTransaction.getId();
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return branchTransaction.get(copyAndSetTargetToIfNot(get));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return branchTransaction.scan(copyAndSetTargetToIfNot(scan));
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(Scan scan) throws CrudException {
    return branchTransaction.getScanner(copyAndSetTargetToIfNot(scan));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    branchTransaction.put(copyAndSetTargetToIfNot(put));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    branchTransaction.mutate(copyAndSetTargetToIfNot(puts));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    branchTransaction.insert(copyAndSetTargetToIfNot(insert));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    branchTransaction.upsert(copyAndSetTargetToIfNot(upsert));
  }

  @Override
  public void update(Update update) throws CrudException {
    branchTransaction.update(copyAndSetTargetToIfNot(update));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    branchTransaction.delete(copyAndSetTargetToIfNot(delete));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    branchTransaction.mutate(copyAndSetTargetToIfNot(deletes));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    branchTransaction.mutate(copyAndSetTargetToIfNot(mutations));
  }

  @Override
  public List<CrudOperable.BatchResult> batch(List<? extends Operation> operations)
      throws CrudException {
    return branchTransaction.batch(copyAndSetTargetToIfNot(operations));
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    // The branch is ended before the transaction is committed, as the branch-lifecycle contract
    // requires. end(Status)'s checked exceptions are unrelated to commit()'s, so they are mapped:
    // a conflict stays retriable, anything else does not. Neither is reachable today — no backing
    // throws from end — but the mapping has to exist for this to compile.
    try {
      branchTransaction.end(BranchTransaction.Status.SUCCESS);
    } catch (CrudConflictException e) {
      throw new CommitConflictException(e.getMessage(), e, getId());
    } catch (CrudException e) {
      throw new CommitException(e.getMessage(), e, getId());
    }

    globalTransaction.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    // The branch is ended on this path too: the owner's rollback does not discharge the branch's
    // own obligation. end(FAILURE) is contractually quiet, but its checked signature is unrelated
    // to RollbackException, so log and swallow — and roll back in a finally, so a failure while
    // ending can never strand an in-flight transaction. This mirrors ConsensusCommit.rollback()'s
    // own scanner cleanup.
    try {
      branchTransaction.end(BranchTransaction.Status.FAILURE);
    } catch (CrudException e) {
      logger.warn("Failed to end the branch. Transaction ID: {}", getId(), e);
    } finally {
      globalTransaction.rollback();
    }
  }
}
