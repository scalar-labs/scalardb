package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link TwoPhaseCommit.Coordinator} decorator that reaps the contexts of inactive transactions.
 *
 * <p>Each transaction is tracked from {@link #begin} until its terminal step ({@link #commit} /
 * {@link #rollback} / {@link #releaseContext}). When a transaction stays inactive longer than the
 * configured expiration time, this decorator calls {@link
 * TwoPhaseCommit.Coordinator#releaseContext} on the wrapped coordinator to free its role-local
 * resources without performing any storage rollback. A subsequent {@code commit}/{@code rollback}
 * for a reaped transaction therefore fails with {@link TransactionNotFoundException}, which is
 * retriable from the client's perspective; the records left behind are recovered lazily by the
 * usual recovery path.
 *
 * <p>Inactivity is measured from the last <em>coordinator-observed</em> call ({@code begin} or
 * {@code registerParticipant}). The coordinator does not see the CRUD operations a transaction
 * issues against its participants, so for a transaction that only performs CRUD the expiration time
 * effectively bounds its total lifetime rather than its true idle time. Size the expiration time as
 * the maximum allowed transaction duration accordingly. The participant-side {@link
 * ActiveTransactionManagedTwoPhaseCommitParticipant} reaps on true idle time because it does
 * observe the CRUD operations.
 *
 * <p>This decorator is intended to be the outermost coordinator decorator so that the reap
 * traverses the inner decorators via {@code releaseContext}.
 *
 * <p>Thread safety: the {@link ThreadSafe} guarantee here relies on the wrapped coordinator
 * serializing its own per-transaction work. The reaper thread's {@code releaseContext} call may run
 * concurrently with an in-flight {@code commit}/{@code rollback} for the same transaction id; the
 * wrapped role is responsible for making those mutually exclusive (e.g. {@code
 * ConsensusCommitCoordinator} synchronizes every per-transaction method, including {@code
 * releaseContext}, on a per-context monitor). A wrapped coordinator that does not serialize
 * per-transaction calls would break this guarantee with no signal at this layer.
 */
@ThreadSafe
public class ActiveTransactionManagedTwoPhaseCommitCoordinator
    extends DecoratedTwoPhaseCommitCoordinator {

  private final ActiveTransactionRegistry<String> registry;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ActiveTransactionManagedTwoPhaseCommitCoordinator(
      TwoPhaseCommit.Coordinator coordinator,
      long expirationTimeMillis,
      int maxActiveTransactions) {
    super(coordinator);
    // The registry stores the transaction ID itself; on expiry or eviction it is handed back so we
    // can release the corresponding context on the wrapped coordinator.
    this.registry =
        new ActiveTransactionRegistry<>(
            expirationTimeMillis, maxActiveTransactions, coordinator::releaseContext);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  ActiveTransactionManagedTwoPhaseCommitCoordinator(
      TwoPhaseCommit.Coordinator coordinator, ActiveTransactionRegistry<String> registry) {
    super(coordinator);
    this.registry = registry;
  }

  @Override
  public String begin(
      @Nullable String transactionId,
      boolean readOnly,
      Map<String, String> attributes,
      @Nullable TwoPhaseCommit.Participant participant)
      throws TransactionException {
    String id = super.begin(transactionId, readOnly, attributes, participant);
    // The wrapped coordinator is the authoritative guard for transaction-ID uniqueness (begin
    // throws TRANSACTION_ALREADY_EXISTS on a duplicate), so a successful super.begin means the ID
    // was free; the add return value is intentionally ignored.
    registry.add(id, id);
    return id;
  }

  @Override
  public void registerParticipant(String transactionId, TwoPhaseCommit.Participant participant)
      throws TransactionException {
    // Refresh before delegating, consistent with the participant decorator's CRUD methods, so a
    // slow registration counts as activity. This only narrows the window for a concurrent idle-reap
    // (it cannot fully close it — the reaper's expiry check and eviction are not serialized with
    // this call); a reap that does race is harmless, since the wrapped coordinator serializes
    // releaseContext against this call (it becomes a no-op once registration completes).
    registry.touch(transactionId);
    super.registerParticipant(transactionId, participant);
  }

  @Override
  public void commit(String transactionId)
      throws CommitException, UnknownTransactionStatusException, TransactionNotFoundException {
    try {
      super.commit(transactionId);
    } finally {
      // Remove on every outcome, including CommitException / UnknownTransactionStatusException.
      // This intentionally differs from the manager-side decorator, which keeps the entry when
      // commit throws so that idle expiry later rolls it back: the coordinator's durable state
      // record (not this in-memory entry) is the source of truth for recovery, so keeping the
      // entry would only produce a spurious expiry WARN and a no-op releaseContext.
      registry.remove(transactionId);
    }
  }

  @Override
  public void rollback(String transactionId) throws RollbackException {
    try {
      super.rollback(transactionId);
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
}
