package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The coordinator role of the internal two-phase commit primitives that drive a transaction
 * spanning multiple participants; {@link TwoPhaseCommitParticipant} is the other role.
 *
 * <p>This is an internal interface intended for components that orchestrate two-phase commit across
 * multiple participants. It is not invoked by application code directly; callers reach it only
 * through the components that drive the protocol on its behalf. Breaking changes can and will be
 * introduced to it. Users should not depend on it.
 *
 * <p>These primitives are designed with the Consensus Commit protocol in mind.
 *
 * <p>The two roles together make up a multi-participant transaction:
 *
 * <ul>
 *   <li>This {@link TwoPhaseCommitCoordinator} drives the two-phase commit protocol. It maintains
 *       per-transaction state, allocates the canonical transaction ID, and orchestrates the
 *       prepare/validate/commit/rollback steps across participants. Writing the durable state
 *       record on the Coordinator table is an internal implementation detail of this role.
 *   <li>{@link TwoPhaseCommitParticipant} performs CRUD operations against the data stores it owns
 *       and the record-level two-phase commit steps (prepare/validate/commit/rollback) on those
 *       records. A single participant may front one or multiple data stores (for example, when
 *       using multi-storage).
 * </ul>
 *
 * <p>These primitives are identifier-based: every operation takes the transaction ID as an argument
 * rather than relying on implicit per-connection or per-session state, so callers may distribute
 * work across processes or resume after a restart. The implementations are not stateless, however —
 * both roles hold per-transaction state internally, keyed by the transaction ID.
 *
 * <p>One-phase commit (an optimization that skips the PREPARE phase) is intentionally out of scope
 * for these primitives; it applies only to transactions confined to a single participant.
 *
 * <p>The Coordinator owns per-transaction state—the registered participants, the canonical
 * transaction ID, and any reserved coordinator-side resources—and the orchestration logic (the
 * prepare/validate/commit steps across participants, or the abort counterpart). Writing the durable
 * state record on the Coordinator table is an internal implementation detail; it is not exposed as
 * a separate method on this interface.
 *
 * <p>A typical flow driven by the Coordinator implementation is:
 *
 * <pre>
 *   String tx = coordinator.begin(null, readOnly, attrs, participant1);
 *   coordinator.registerParticipant(tx, participant2);
 *   // ... application CRUD against each participant via its CRUD methods ...
 *   coordinator.commit(tx);
 *   // or coordinator.rollback(tx);
 * </pre>
 *
 * <p>Lazy recovery on the participant side accesses the Coordinator table directly, not through
 * these primitives.
 *
 * <p><b>Concurrency.</b> Implementations must serialize per-transaction work: for one transaction
 * ID the methods here must be mutually exclusive, so concurrent calls cannot corrupt that
 * transaction's state. This explicitly covers {@link #releaseTransactionContext}, which a
 * context-reaper drives from its own thread and may therefore invoke concurrently with any other
 * method for the same transaction ID. Calls for different transaction IDs may proceed concurrently.
 * Decorators that add a background reaper rely on this contract for their own thread-safety.
 */
public interface TwoPhaseCommitCoordinator extends AutoCloseable {

  /**
   * Begins a new transaction.
   *
   * <p>Allocates per-transaction state and returns the canonical transaction ID. The returned ID
   * may incorporate implementation-specific structure, so it is not necessarily equal to {@code
   * transactionId}; all subsequent calls for this transaction must use the returned ID.
   *
   * <p>If {@code transactionId} is non-null, the implementation derives the canonical transaction
   * ID from it. The caller is responsible for ensuring the ID is unique. If it is {@code null}, the
   * implementation generates one.
   *
   * <p>If {@code readOnly} is {@code true}, the implementation may optimize for a transaction that
   * will not write.
   *
   * <p>If {@code participant} is non-null, the Coordinator registers it for the transaction—
   * invoking {@link TwoPhaseCommitParticipant#join} on it with the {@code readOnly} flag and {@code
   * attributes}—exactly as {@link #registerParticipant} would. Additional participants can be
   * registered afterward via {@link #registerParticipant}.
   *
   * @param transactionId the caller-supplied transaction ID, or {@code null} to have the
   *     implementation generate one
   * @param readOnly whether the transaction is known to be read-only
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @param participant the participant to register for this transaction (as if by {@link
   *     #registerParticipant}), or {@code null} to register none — for example, a coordinator-only
   *     transaction, or one whose participants are all registered separately via {@link
   *     #registerParticipant}
   * @return the canonical transaction ID to use for all subsequent operations
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  String begin(
      @Nullable String transactionId,
      boolean readOnly,
      Map<String, String> attributes,
      @Nullable TwoPhaseCommitParticipant participant)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Registers a participant in the transaction.
   *
   * <p>Use this to register participants in addition to the optional {@code participant} passed to
   * {@link #begin}.
   *
   * <p>The Coordinator adds the participant to its per-transaction state and internally invokes
   * {@link TwoPhaseCommitParticipant#join} on it—forwarding the {@code readOnly} flag and {@code
   * attributes} supplied to {@link #begin}—so that the participant establishes a local context for
   * the transaction. The {@link TwoPhaseCommitParticipant#join} method is not intended to be
   * invoked directly by callers.
   *
   * <p>Registration is idempotent per participant ID: if a participant with the same {@link
   * TwoPhaseCommitParticipant#getId()} is already registered for the transaction, this call is a
   * no-op (the participant is not joined again).
   *
   * @param transactionId the canonical transaction ID returned by {@link #begin}
   * @param participant the participant to register
   * @throws TransactionNotFoundException if registering the participant fails due to transient
   *     faults. You can retry the transaction from the beginning
   * @throws TransactionException if registering the participant fails due to transient or
   *     nontransient faults
   */
  void registerParticipant(String transactionId, TwoPhaseCommitParticipant participant)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Drives the commit protocol across the registered participants.
   *
   * <p>The flow runs a prepare phase ({@link TwoPhaseCommitParticipant#prepareRecords}), a
   * validation phase ({@link TwoPhaseCommitParticipant#validateRecords}), then—once the outcome is
   * durably decided—{@link TwoPhaseCommitParticipant#commitRecords}. It is always two-phase; this
   * interface provides no one-phase commit fast path. An implementation may omit work that is
   * unnecessary for a given transaction (for example, for a read-only or write-less transaction it
   * may skip steps that would have nothing to do).
   *
   * <p>The prepare and validate phases are internal to this method, so their failures are not
   * surfaced as {@link PreparationException} or {@link ValidationException}. On such a failure the
   * Coordinator drives the rollback internally—rolling back the PREPARED records on every
   * registered participant, not only the one whose prepare or validate failed—and then reports the
   * outcome as a commit-level exception: a conflict (a retriable failure) as {@link
   * CommitConflictException}, any other failure as {@link CommitException}. If the outcome cannot
   * be durably recorded, {@link UnknownTransactionStatusException} is propagated instead. If a
   * participant no longer knows the transaction while preparing or validating (its local context is
   * gone, e.g. it expired), the Coordinator performs the same internal rollback and then propagates
   * that {@link TransactionNotFoundException} as-is.
   *
   * <p>The Coordinator generates the prepared and committed timestamps internally.
   *
   * @param transactionId the canonical transaction ID returned by {@link #begin}
   * @throws CommitConflictException if the commit fails due to transient faults (e.g., a conflict
   *     error, including one detected while preparing or validating). You can retry the transaction
   *     from the beginning
   * @throws CommitException if the commit fails due to transient or nontransient faults (including
   *     a non-conflict failure detected while preparing or validating)
   * @throws UnknownTransactionStatusException if the final commit status cannot be determined. The
   *     outcome is indeterminate—the transaction may or may not have been committed. Do not blindly
   *     retry or roll back; determine the outcome (for example, by checking the coordinator state,
   *     which lazy recovery also relies on) before deciding how to proceed
   * @throws TransactionNotFoundException if no transaction with this ID is registered with this
   *     Coordinator (never begun, or already finished by a prior commit/rollback), or if a
   *     registered participant no longer knows the transaction while preparing or validating (its
   *     local context is gone, e.g. it expired). You can retry the transaction from the beginning
   */
  void commit(String transactionId)
      throws CommitConflictException, CommitException, UnknownTransactionStatusException,
          TransactionNotFoundException;

  /**
   * Drives the rollback protocol across the registered participants.
   *
   * <p>Drives only the work needed to undo whatever may have been prepared, via {@link
   * TwoPhaseCommitParticipant#rollbackRecords} on each registered participant.
   *
   * <p>Unlike {@link #commit}, an unknown transaction ID (never begun, or already finished) is a
   * no-op rather than an error: no participants are contacted, and rolling back a transaction the
   * Coordinator does not know about leaves nothing to undo. Any participant context still alive for
   * such a transaction is reclaimed by the participant's own timeout.
   *
   * @param transactionId the canonical transaction ID returned by {@link #begin}
   * @throws RollbackException if the rollback fails due to transient or nontransient faults. Not
   *     thrown for an unknown transaction ID
   */
  void rollback(String transactionId) throws RollbackException;

  /**
   * Releases the Coordinator's in-memory state for the transaction without touching storage.
   *
   * <p>This is a reap-only terminal used to reclaim an in-memory context that did not reach a
   * normal {@link #commit} / {@link #rollback} — for example, an abandoned transaction, or one
   * reaped on idle by a decorator. It discards only this Coordinator's per-transaction state; it
   * does <strong>not</strong> drive the participants and does <strong>not</strong> write any
   * Coordinator state row or mutate records. The transaction's durable outcome is left to lazy
   * recovery, which relies on the Coordinator state row if one was already written (its absence is
   * resolved as an abort).
   *
   * <p>An unknown transaction ID (never begun, or already finished) is a no-op. A decorator may
   * treat this as a terminal step, releasing any per-transaction resources it holds.
   *
   * @param transactionId the canonical transaction ID returned by {@link #begin}
   * @throws TransactionNotFoundException if the implementation reports the unknown-transaction
   *     no-op on its conventional not-found channel instead of returning silently (typical across a
   *     transport boundary). An alternative carrier of the no-op outcome — there was no context to
   *     release — so callers must treat it like a normal return
   * @throws TransactionException if releasing the context fails due to transient or nontransient
   *     faults. The context may still be held; reap-style callers treat the failure as best-effort,
   *     since the context is reclaimed by other means (e.g. a later reap or close)
   */
  void releaseTransactionContext(String transactionId)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Closes the Coordinator and releases any resources it holds.
   *
   * <p>Does not commit or roll back transactions that are still in flight; their in-memory state is
   * discarded and any durable outcome is left to lazy recovery.
   */
  @Override
  void close();
}
