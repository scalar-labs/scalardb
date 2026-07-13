package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link TwoPhaseCommit.Participant} decorator that reaps the contexts of inactive transactions.
 *
 * <p>Each transaction is tracked from {@link #join} until its terminal step ({@link #commitRecords}
 * / {@link #rollbackRecords} / {@link #releaseContext}). Every CRUD operation and record-level step
 * refreshes the transaction's idle timer, so inactivity here is measured as true idle time. When a
 * transaction stays inactive longer than the configured expiration time, this decorator calls
 * {@link TwoPhaseCommit.Participant#releaseContext} on the wrapped participant to free its
 * role-local resources (e.g. open scanners, the snapshot) without performing any storage rollback,
 * even when the records are prepared. The records left behind are recovered lazily by the usual
 * recovery path.
 *
 * <p>This decorator is intended to be the outermost participant decorator so that the reap
 * traverses the inner decorators via {@code releaseContext}. It is the participant-side counterpart
 * of {@link ActiveTransactionManagedTwoPhaseCommitCoordinator}.
 *
 * <p>A write-less participant does not always reach {@link #commitRecords}: the Coordinator skips
 * the steps a participant no longer needs, so for such a participant the last driven step is {@link
 * #prepareRecords} (when no validation is required) or {@link #validateRecords} (when it is). To
 * avoid leaking a registry entry until idle expiry (and the spurious expiry WARN that follows),
 * this decorator removes the entry at whichever step is terminal, using {@link
 * TwoPhaseCommit.PreparationResult#isCommitRequired} and {@link
 * TwoPhaseCommit.PreparationResult#isValidationRequired} reported at {@code prepareRecords} to
 * decide where the terminal step lands. The decision is carried across the prepare-to-validate gap
 * by a flag on the registry entry itself ({@link TrackedTransaction#terminalAtValidate}), so it
 * shares the entry's lifecycle and adds no separate state to clean up.
 *
 * <p>Thread safety: the {@link ThreadSafe} guarantee here relies on the wrapped participant
 * serializing its own per-transaction work. The reaper thread's {@code releaseContext} call may run
 * concurrently with an in-flight CRUD or record-level call for the same transaction id; the wrapped
 * role is responsible for making those mutually exclusive (e.g. {@code ConsensusCommitParticipant}
 * synchronizes every per-transaction method, including {@code releaseContext}, on a per-context
 * monitor). A wrapped participant that does not serialize per-transaction calls would break this
 * guarantee with no signal at this layer.
 */
@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitParticipant
    extends DecoratedTwoPhaseCommitParticipant {

  private final ActiveTransactionRegistry<TrackedTransaction> registry;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ActiveTransactionManagedTwoPhaseCommitParticipant(
      TwoPhaseCommit.Participant participant,
      long expirationTimeMillis,
      int maxActiveTransactions) {
    super(participant);
    // The registry stores a small per-transaction entry carrying the transaction ID and the
    // terminal-at-validate flag; on expiry or eviction it is handed back so we can release the
    // corresponding context on the wrapped participant.
    this.registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis,
            maxActiveTransactions,
            tracked -> participant.releaseContext(tracked.transactionId));
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  ActiveTransactionManagedTwoPhaseCommitParticipant(
      TwoPhaseCommit.Participant participant,
      ActiveTransactionRegistry<TrackedTransaction> registry) {
    super(participant);
    this.registry = registry;
  }

  @Override
  public void join(String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    super.join(transactionId, readOnly, attributes);
    // The wrapped participant is the authoritative guard for transaction-ID uniqueness (join throws
    // TRANSACTION_ALREADY_EXISTS on a duplicate), so a successful super.join means the ID was free;
    // the add return value is intentionally ignored.
    registry.add(transactionId, new TrackedTransaction(transactionId));
  }

  @Override
  public Optional<Result> get(String transactionId, Get get)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    return super.get(transactionId, get);
  }

  @Override
  public List<Result> scan(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    return super.scan(transactionId, scan);
  }

  @Override
  public TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    // The scanner outlives this call: wrap it so each one()/all()/iteration also refreshes the idle
    // timer, otherwise a transaction streaming a large result slowly could be reaped mid-scan.
    return new ActiveTransactionRefreshingScanner(
        registry, transactionId, super.getScanner(transactionId, scan));
  }

  /** @deprecated As of release 3.19.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public void put(String transactionId, Put put)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.put(transactionId, put);
  }

  @Override
  public void insert(String transactionId, Insert insert)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.insert(transactionId, insert);
  }

  @Override
  public void upsert(String transactionId, Upsert upsert)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.upsert(transactionId, upsert);
  }

  @Override
  public void update(String transactionId, Update update)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.update(transactionId, update);
  }

  @Override
  public void delete(String transactionId, Delete delete)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.delete(transactionId, delete);
  }

  @Override
  public void mutate(String transactionId, List<? extends Mutation> mutations)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.mutate(transactionId, mutations);
  }

  @Override
  public List<CrudOperable.BatchResult> batch(
      String transactionId, List<? extends Operation> operations)
      throws CrudException, TransactionNotFoundException {
    registry.touch(transactionId);
    return super.batch(transactionId, operations);
  }

  @Override
  public TwoPhaseCommit.PreparationResult prepareRecords(String transactionId, long preparedAt)
      throws PreparationException, TransactionNotFoundException {
    registry.touch(transactionId);
    TwoPhaseCommit.PreparationResult result = super.prepareRecords(transactionId, preparedAt);
    if (result.isCommitRequired()) {
      // Intentionally nothing to do here: commitRecords will be driven and is the terminal step,
      // and its override removes the entry. Kept as an explicit arm so all three terminal-step
      // cases read in protocol order.
    } else if (result.isValidationRequired()) {
      // Write-less but validating: validateRecords is the terminal step. Flag the entry so
      // validateRecords removes it.
      registry.get(transactionId).ifPresent(tracked -> tracked.terminalAtValidate = true);
    } else {
      // Neither validate nor commit will be driven: prepareRecords is the terminal step.
      registry.remove(transactionId);
    }
    return result;
  }

  @Override
  public void validateRecords(String transactionId)
      throws ValidationException, TransactionNotFoundException {
    registry.touch(transactionId);
    super.validateRecords(transactionId);
    // Only on success: if validateRecords was the terminal step, remove the entry now. On failure
    // the transaction aborts and rollbackRecords (which removes the entry) is driven instead.
    Optional<TrackedTransaction> tracked = registry.get(transactionId);
    if (tracked.isPresent() && tracked.get().terminalAtValidate) {
      registry.remove(transactionId);
    }
  }

  @Override
  public void commitRecords(String transactionId, long committedAt)
      throws CommitException, TransactionNotFoundException {
    try {
      super.commitRecords(transactionId, committedAt);
    } finally {
      registry.remove(transactionId);
    }
  }

  @Override
  public void rollbackRecords(String transactionId) throws RollbackException {
    try {
      super.rollbackRecords(transactionId);
    } finally {
      registry.remove(transactionId);
    }
  }

  @Override
  public void releaseContext(String transactionId) {
    try {
      super.releaseContext(transactionId);
    } finally {
      registry.remove(transactionId);
    }
  }

  /**
   * A registry entry tracking one transaction: its ID (used to release the context on expiry) and
   * whether {@link #validateRecords} is its terminal step. The flag is written at {@code
   * prepareRecords} and read at {@code validateRecords}; it lives and dies with the registry entry.
   */
  private static final class TrackedTransaction {
    private final String transactionId;

    // volatile is required, not incidental: the flag is written at prepareRecords and read at
    // validateRecords, which may run on different threads. Do not drop volatile.
    private volatile boolean terminalAtValidate;

    private TrackedTransaction(String transactionId) {
      this.transactionId = transactionId;
    }
  }
}
