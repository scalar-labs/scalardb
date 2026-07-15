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
import com.scalar.db.api.TwoPhaseCommitParticipant;
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
 * A {@link TwoPhaseCommitParticipant} decorator that reaps the contexts of inactive transactions.
 *
 * <p>Each transaction is tracked from {@link #join} until its terminal step ({@link #commitRecords}
 * / {@link #rollbackRecords} / {@link #releaseTransactionContext}). Every CRUD operation and
 * record-level step refreshes the transaction's idle timer, so inactivity here is measured as true
 * idle time. When a transaction stays inactive longer than the configured expiration time, this
 * decorator calls {@link TwoPhaseCommitParticipant#releaseTransactionContext} on the wrapped
 * participant to free its role-local resources (e.g. open scanners, the snapshot) without
 * performing any storage rollback, even when the records are prepared. The records left behind are
 * recovered lazily by the usual recovery path.
 *
 * <p>This decorator is intended to be the outermost participant decorator so that the reap
 * traverses the inner decorators via {@code releaseTransactionContext}. It is the participant-side
 * counterpart of {@link ActiveTransactionManagedTwoPhaseCommitCoordinator}.
 *
 * <p>By default the reaper releases the context by calling {@link
 * TwoPhaseCommitParticipant#releaseTransactionContext} directly on the wrapped participant. An
 * embedder that interposes a cross-cutting decorator between this decorator and the wrapped
 * participant — for example, an authorization decorator that credential-checks every call — may
 * need that reap-driven release to run in a different execution context than a normal caller-driven
 * one, because the reaper runs on an internal timer thread that carries no caller credentials. The
 * {@linkplain #ActiveTransactionManagedTwoPhaseCommitParticipant(TwoPhaseCommitParticipant, long,
 * int, ActiveTransactionRegistry.DisposalHandler) disposal-handler constructor} lets such an
 * embedder substitute the reap-driven release action (e.g. wrap it in a privileged mode) while
 * leaving every other path untouched. This mirrors the seam {@link
 * ActiveTransactionManagedDistributedTransactionManager} already exposes for its 1PC reaper.
 *
 * <p>A write-less participant does not always reach {@link #commitRecords}: the Coordinator skips
 * the steps a participant no longer needs, so for such a participant the last driven step is {@link
 * #prepareRecords} (when no validation is required) or {@link #validateRecords} (when it is). To
 * avoid leaking a registry entry until idle expiry (and the spurious expiry WARN that follows),
 * this decorator removes the entry at whichever step is terminal, using {@link
 * TwoPhaseCommitParticipant.PreparationResult#isCommitRequired} and {@link
 * TwoPhaseCommitParticipant.PreparationResult#isValidationRequired} reported at {@code
 * prepareRecords} to decide where the terminal step lands. The decision is carried across the
 * prepare-to-validate gap by a flag on the registry entry itself ({@link
 * TrackedTransaction#terminalAtValidate}), so it shares the entry's lifecycle and adds no separate
 * state to clean up.
 *
 * <p>{@link TwoPhaseCommitParticipant#hasTransactionContext} is deliberately <em>not</em>
 * overridden here: the probe passes through untouched to the wrapped participant's context map, so
 * it can never refresh this decorator's idle timer — the probe's quiet contract holds by
 * construction. Do not add a registry-based override without preserving that property (a probe that
 * counted as activity would form a feedback loop that keeps abandoned transactions alive forever).
 *
 * <p>Thread safety: the {@link ThreadSafe} guarantee here relies on the wrapped participant
 * honoring the {@link TwoPhaseCommitParticipant} concurrency contract — serializing its own
 * per-transaction work. The reaper thread's {@code releaseTransactionContext} call may run
 * concurrently with an in-flight CRUD or record-level call for the same transaction id; the wrapped
 * role is responsible for making those mutually exclusive (e.g. {@code ConsensusCommitParticipant}
 * synchronizes every per-transaction method, including {@code releaseTransactionContext}, on a
 * per-context monitor). A wrapped participant that violates that contract would break this
 * guarantee with no signal at this layer.
 */
@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitParticipant
    extends DecoratedTwoPhaseCommitParticipant {

  private final ActiveTransactionRegistry<TrackedTransaction> registry;

  /**
   * Creates a decorator whose reaper releases each inactive transaction's context directly on the
   * wrapped participant, treating a {@link TransactionNotFoundException} from the release as the
   * already-released no-op it denotes.
   *
   * @param participant the wrapped participant
   * @param expirationTimeMillis the idle expiration time in milliseconds
   * @param maxActiveTransactions the maximum number of active transactions to track; non-positive
   *     means unbounded
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ActiveTransactionManagedTwoPhaseCommitParticipant(
      TwoPhaseCommitParticipant participant, long expirationTimeMillis, int maxActiveTransactions) {
    // The default disposal action releases the wrapped participant's context directly.
    this(
        participant,
        expirationTimeMillis,
        maxActiveTransactions,
        transactionId -> releaseTransactionContextQuietly(participant, transactionId));
  }

  /**
   * Creates a decorator whose reaper disposes of an inactive transaction by invoking {@code
   * disposalHandler} with the transaction ID, instead of releasing the context directly on the
   * wrapped participant.
   *
   * <p>Use this when the reap-driven release must run in a special execution context — for example,
   * when a cross-cutting decorator between this decorator and the wrapped participant would reject
   * a call made from the credential-less reaper thread, so the embedder needs to wrap the release
   * in a privileged mode (see the class documentation). The handler is invoked for both idle expiry
   * and cap eviction, and it fully replaces the default action: the embedder performs the actual
   * release itself (typically {@code participant.releaseTransactionContext(transactionId)}) and, if
   * its wrapped participant can report an already-released context as a {@link
   * TransactionNotFoundException}, is responsible for treating that as the benign no-op it denotes.
   *
   * @param participant the wrapped participant
   * @param expirationTimeMillis the idle expiration time in milliseconds
   * @param maxActiveTransactions the maximum number of active transactions to track; non-positive
   *     means unbounded
   * @param disposalHandler invoked with the transaction ID when a tracked transaction is reaped
   *     (idle expiry or cap eviction), replacing the default release on the wrapped participant
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ActiveTransactionManagedTwoPhaseCommitParticipant(
      TwoPhaseCommitParticipant participant,
      long expirationTimeMillis,
      int maxActiveTransactions,
      ActiveTransactionRegistry.DisposalHandler<String> disposalHandler) {
    super(participant);
    // The registry stores a small per-transaction entry carrying the transaction ID and the
    // terminal-at-validate flag; on expiry or eviction it is handed back so the disposal handler
    // can release the corresponding context, keyed by transaction ID.
    this.registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis,
            maxActiveTransactions,
            tracked -> disposalHandler.onDisposed(tracked.transactionId));
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  ActiveTransactionManagedTwoPhaseCommitParticipant(
      TwoPhaseCommitParticipant participant,
      ActiveTransactionRegistry<TrackedTransaction> registry) {
    super(participant);
    this.registry = registry;
  }

  private static void releaseTransactionContextQuietly(
      TwoPhaseCommitParticipant participant, String transactionId) throws TransactionException {
    try {
      participant.releaseTransactionContext(transactionId);
    } catch (TransactionNotFoundException e) {
      // The context is already gone — the outcome this release wanted; not-found is its
      // alternative carrier (see the interface Javadoc).
    }
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
  public TwoPhaseCommitParticipant.PreparationResult prepareRecords(
      String transactionId,
      long preparedAt,
      TwoPhaseCommitParticipant.WriteSetDetailLevel detailLevel)
      throws PreparationException, TransactionNotFoundException {
    registry.touch(transactionId);
    TwoPhaseCommitParticipant.PreparationResult result =
        super.prepareRecords(transactionId, preparedAt, detailLevel);
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
  public void rollbackRecords(String transactionId)
      throws RollbackException, TransactionNotFoundException {
    try {
      super.rollbackRecords(transactionId);
    } finally {
      registry.remove(transactionId);
    }
  }

  @Override
  public void releaseTransactionContext(String transactionId) throws TransactionException {
    try {
      super.releaseTransactionContext(transactionId);
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
