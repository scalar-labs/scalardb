package com.scalar.db.common;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Adapts a {@link TwoPhaseCommit.Coordinator} + a single in-process {@link
 * TwoPhaseCommit.Participant} to the single-phase {@link com.scalar.db.api.DistributedTransaction}
 * API.
 *
 * <p>CRUD is delegated to the participant (keyed by this transaction's canonical ID); {@link
 * #commit()} drives the full two-phase commit through the coordinator (which generates the
 * prepared/committed timestamps internally); {@link #rollback()} drives the coordinator's rollback.
 * The two-phase nature is fully internalized — callers see a single-phase transaction, so existing
 * {@code DistributedTransaction} decorators and tests work unchanged.
 *
 * <p>A transaction obtained via {@link CoordinatorBackedDistributedTransactionManager#join(String)}
 * is <em>joined</em>: it participates in CRUD only, and {@link #commit()} / {@link #rollback()} are
 * unsupported because the originator that began the transaction drives them.
 *
 * <p>See {@link CoordinatorBackedDistributedTransactionManager} for how the coordinator and
 * participant are wired and the transaction is begun.
 */
public class CoordinatorBackedDistributedTransaction extends AbstractDistributedTransaction {

  private final TwoPhaseCommit.Coordinator coordinator;
  @Nullable private final TwoPhaseCommit.Participant participant;
  private final String transactionId;

  // True when this transaction was joined (the participant registered into a transaction begun
  // elsewhere) rather than begun here. A joined transaction participates in CRUD only; the
  // originator that began the transaction drives commit/rollback, so they are unsupported here.
  private final boolean joined;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CoordinatorBackedDistributedTransaction(
      TwoPhaseCommit.Coordinator coordinator,
      @Nullable TwoPhaseCommit.Participant participant,
      String transactionId,
      boolean joined) {
    this.coordinator = coordinator;
    this.participant = participant;
    this.transactionId = transactionId;
    this.joined = joined;
  }

  @Override
  public String getId() {
    return transactionId;
  }

  private TwoPhaseCommit.Participant participant() {
    if (participant == null) {
      throw new UnsupportedOperationException(
          "CRUD operations are not supported by this Coordinator-backed transaction because no"
              + " participant is configured");
    }
    return participant;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return crud(() -> participant().get(transactionId, copyAndSetTargetToIfNot(get)));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return crud(() -> participant().scan(transactionId, copyAndSetTargetToIfNot(scan)));
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(Scan scan) throws CrudException {
    return crud(() -> participant().getScanner(transactionId, copyAndSetTargetToIfNot(scan)));
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    crud(() -> participant().put(transactionId, copyAndSetTargetToIfNot(put)));
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    crud(() -> participant().mutate(transactionId, copyAndSetTargetToIfNot(puts)));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    crud(() -> participant().insert(transactionId, copyAndSetTargetToIfNot(insert)));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    crud(() -> participant().upsert(transactionId, copyAndSetTargetToIfNot(upsert)));
  }

  @Override
  public void update(Update update) throws CrudException {
    crud(() -> participant().update(transactionId, copyAndSetTargetToIfNot(update)));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    crud(() -> participant().delete(transactionId, copyAndSetTargetToIfNot(delete)));
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    crud(() -> participant().mutate(transactionId, copyAndSetTargetToIfNot(deletes)));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    crud(() -> participant().mutate(transactionId, copyAndSetTargetToIfNot(mutations)));
  }

  @Override
  public List<CrudOperable.BatchResult> batch(List<? extends Operation> operations)
      throws CrudException {
    return crud(() -> participant().batch(transactionId, copyAndSetTargetToIfNot(operations)));
  }

  @FunctionalInterface
  private interface CrudSupplier<T> {
    T get() throws CrudException, TransactionNotFoundException;
  }

  @FunctionalInterface
  private interface CrudRunnable {
    void run() throws CrudException, TransactionNotFoundException;
  }

  private <T> T crud(CrudSupplier<T> supplier) throws CrudException {
    try {
      return supplier.get();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, transactionId);
    }
  }

  private void crud(CrudRunnable runnable) throws CrudException {
    crud(
        () -> {
          runnable.run();
          return null;
        });
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    if (joined) {
      throw new UnsupportedOperationException(
          "commit is not supported on a joined transaction; the transaction's originator drives the"
              + " commit");
    }
    // Drive the full 2PC; the coordinator generates the prepared/committed timestamps internally.
    // The coordinator already reports prepare/validate failures as CommitConflictException /
    // CommitException, so those propagate as-is; only an unknown transaction needs translating.
    try {
      coordinator.commit(transactionId);
    } catch (TransactionNotFoundException e) {
      // The coordinator no longer knows this transaction (e.g., it expired). Surface it as a
      // retriable commit conflict so the caller can restart the transaction from the beginning.
      throw new CommitConflictException(e.getMessage(), e, transactionId);
    }
  }

  @Override
  public void rollback() throws RollbackException {
    if (joined) {
      throw new UnsupportedOperationException(
          "rollback is not supported on a joined transaction; the transaction's originator drives"
              + " the rollback");
    }
    coordinator.rollback(transactionId);
  }
}
