package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
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
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;

/**
 * Adapts a {@link TwoPhaseCommitParticipant} to the {@link BranchTransaction} API.
 *
 * <p>This is a per-participant branch of a distributed transaction. CRUD is delegated to the
 * participant (keyed by the global transaction's ID); the record-level two-phase commit steps
 * (prepare/validate/commit/rollback) are not exposed here — they are driven by the coordinator
 * behind {@link com.scalar.db.api.GlobalTransaction#commit()}. {@link #end()} triggers no backing
 * action for this backing, which buffers nothing to flush — it only marks the branch ended, after
 * which CRUD (or another {@code end()}) is rejected with {@link IllegalStateException}; the
 * participant's local context is released by the coordinator-driven commit/rollback (or reclaimed
 * by idle expiry), not by {@link #end()}.
 *
 * <p>Operations must be fully qualified with their namespace and table; this handle carries no
 * default target. See {@link TwoPhaseCommitBackedGlobalTransactionManager} for how the participant
 * is wired and the branch is begun.
 */
public class TwoPhaseCommitBackedBranchTransaction implements BranchTransaction {

  private final TwoPhaseCommitParticipant participant;
  private final String transactionId;

  private boolean ended;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseCommitBackedBranchTransaction(
      TwoPhaseCommitParticipant participant, String transactionId) {
    this.participant = participant;
    this.transactionId = transactionId;
  }

  @Override
  public String getId() {
    return transactionId;
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return crud(() -> participant.get(transactionId, get));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return crud(() -> participant.scan(transactionId, scan));
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(Scan scan) throws CrudException {
    return crud(() -> participant.getScanner(transactionId, scan));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    crud(() -> participant.put(transactionId, put));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    crud(() -> participant.mutate(transactionId, puts));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    crud(() -> participant.insert(transactionId, insert));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    crud(() -> participant.upsert(transactionId, upsert));
  }

  @Override
  public void update(Update update) throws CrudException {
    crud(() -> participant.update(transactionId, update));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    crud(() -> participant.delete(transactionId, delete));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    crud(() -> participant.mutate(transactionId, deletes));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    crud(() -> participant.mutate(transactionId, mutations));
  }

  @Override
  public List<CrudOperable.BatchResult> batch(List<? extends Operation> operations)
      throws CrudException {
    return crud(() -> participant.batch(transactionId, operations));
  }

  @Override
  public void end() throws CrudException {
    checkNotEnded();
    ended = true;
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
    checkNotEnded();
    try {
      return supplier.get();
    } catch (TransactionNotFoundException e) {
      // The participant no longer knows this transaction (e.g., it expired). Surface it as a
      // retriable CRUD conflict so the caller can restart the transaction from the beginning.
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

  private void checkNotEnded() {
    if (ended) {
      throw new IllegalStateException(
          CoreError.BRANCH_TRANSACTION_ALREADY_ENDED.buildMessage(transactionId));
    }
  }
}
