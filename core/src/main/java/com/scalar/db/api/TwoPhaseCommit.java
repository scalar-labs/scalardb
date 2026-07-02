package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Primitives for driving a two-phase commit transaction that spans multiple participants.
 *
 * <p>This is an internal interface intended for components that orchestrate two-phase commit across
 * multiple participants. It is not invoked by application code directly; callers reach it only
 * through the components that drive the protocol on its behalf. Breaking changes can and will be
 * introduced to it. Users should not depend on it.
 *
 * <p>This interface is designed with the Consensus Commit protocol in mind.
 *
 * <p>The interface defines two roles—{@link Coordinator} and {@link Participant}—that together make
 * up a multi-participant transaction:
 *
 * <ul>
 *   <li>{@link Coordinator} drives the two-phase commit protocol. It maintains per-transaction
 *       state, allocates the canonical transaction ID, and orchestrates the
 *       prepare/validate/commit/rollback steps across participants. Writing the durable state
 *       record on the Coordinator table is an internal implementation detail of this role.
 *   <li>{@link Participant} performs CRUD operations against the data stores it owns and the
 *       record-level two-phase commit steps (prepare/validate/commit/rollback) on those records. A
 *       single participant may front one or multiple data stores (for example, when using
 *       multi-storage).
 * </ul>
 *
 * <p>This interface is identifier-based: every operation takes the transaction ID as an argument
 * rather than relying on implicit per-connection or per-session state, so callers may distribute
 * work across processes or resume after a restart. The implementations are not stateless, however —
 * both roles hold per-transaction state internally, keyed by the transaction ID.
 *
 * <p>One-phase commit (an optimization that skips the PREPARE phase) is intentionally out of scope
 * for this interface; it applies only to transactions confined to a single participant.
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
 * this interface.
 */
public interface TwoPhaseCommit {

  /**
   * Drives the two-phase commit protocol across the registered participants.
   *
   * <p>The Coordinator owns per-transaction state—the registered participants, the canonical
   * transaction ID, and any reserved coordinator-side resources—and the orchestration logic (the
   * prepare/validate/commit steps across participants, or the abort counterpart). Writing the
   * durable state record on the Coordinator table is an internal implementation detail; it is not
   * exposed as a separate method on this interface.
   */
  interface Coordinator extends AutoCloseable {

    /**
     * Begins a new transaction.
     *
     * <p>Allocates per-transaction state and returns the canonical transaction ID. The returned ID
     * may incorporate implementation-specific structure, so it is not necessarily equal to {@code
     * transactionId}; all subsequent calls for this transaction must use the returned ID.
     *
     * <p>If {@code transactionId} is non-null, the implementation derives the canonical transaction
     * ID from it. The caller is responsible for ensuring the ID is unique. If it is {@code null},
     * the implementation generates one.
     *
     * <p>If {@code readOnly} is {@code true}, the implementation may optimize for a transaction
     * that will not write.
     *
     * <p>If {@code participant} is non-null, the Coordinator registers it for the transaction—
     * invoking {@link Participant#join} on it with the {@code readOnly} flag and {@code
     * attributes}—exactly as {@link #registerParticipant} would. Additional participants can be
     * registered afterward via {@link #registerParticipant}.
     *
     * @param transactionId the caller-supplied transaction ID, or {@code null} to have the
     *     implementation generate one
     * @param readOnly whether the transaction is known to be read-only
     * @param attributes implementation-specific transaction attributes (may be empty)
     * @param participant the participant to register for this transaction (as if by {@link
     *     #registerParticipant}), or {@code null} to register none — for example, a
     *     coordinator-only transaction, or one whose participants are all registered separately via
     *     {@link #registerParticipant}
     * @return the canonical transaction ID to use for all subsequent operations
     * @throws TransactionNotFoundException if the transaction fails to begin due to transient
     *     faults. You can retry the transaction from the beginning
     * @throws TransactionException if the transaction fails to begin due to transient or
     *     nontransient faults. You can try retrying the transaction, but you may not be able to
     *     begin the transaction due to nontransient faults
     */
    String begin(
        @Nullable String transactionId,
        boolean readOnly,
        Map<String, String> attributes,
        @Nullable Participant participant)
        throws TransactionNotFoundException, TransactionException;

    /**
     * Registers a participant in the transaction.
     *
     * <p>Use this to register participants in addition to the optional {@code participant} passed
     * to {@link #begin}.
     *
     * <p>The Coordinator adds the participant to its per-transaction state and internally invokes
     * {@link Participant#join} on it—forwarding the {@code readOnly} flag and {@code attributes}
     * supplied to {@link #begin}—so that the participant establishes a local context for the
     * transaction. The {@link Participant#join} method is not intended to be invoked directly by
     * callers.
     *
     * <p>Registration is idempotent per participant ID: if a participant with the same {@link
     * Participant#getId()} is already registered for the transaction, this call is a no-op (the
     * participant is not joined again).
     *
     * @param transactionId the canonical transaction ID returned by {@link #begin}
     * @param participant the participant to register
     * @throws TransactionNotFoundException if registering the participant fails due to transient
     *     faults. You can retry the transaction from the beginning
     * @throws TransactionException if registering the participant fails due to transient or
     *     nontransient faults
     */
    void registerParticipant(String transactionId, Participant participant)
        throws TransactionNotFoundException, TransactionException;

    /**
     * Drives the commit protocol across the registered participants.
     *
     * <p>The flow runs a prepare phase ({@link Participant#prepareRecords}), a validation phase
     * ({@link Participant#validateRecords}), then—once the outcome is durably decided—{@link
     * Participant#commitRecords}. It is always two-phase; this interface provides no one-phase
     * commit fast path. An implementation may omit work that is unnecessary for a given transaction
     * (for example, for a read-only or write-less transaction it may skip steps that would have
     * nothing to do).
     *
     * <p>The prepare and validate phases are internal to this method, so their failures are not
     * surfaced as {@link PreparationException} or {@link ValidationException}. On such a failure
     * the Coordinator drives the rollback internally—rolling back the PREPARED records on every
     * registered participant, not only the one whose prepare or validate failed—and then reports
     * the outcome as a commit-level exception: a conflict (a retriable failure) as {@link
     * CommitConflictException}, any other failure as {@link CommitException}. If the outcome cannot
     * be durably recorded, {@link UnknownTransactionStatusException} is propagated instead. If a
     * participant no longer knows the transaction while preparing or validating (its local context
     * is gone, e.g. it expired), the Coordinator performs the same internal rollback and then
     * propagates that {@link TransactionNotFoundException} as-is.
     *
     * <p>The Coordinator generates the prepared and committed timestamps internally.
     *
     * @param transactionId the canonical transaction ID returned by {@link #begin}
     * @throws CommitConflictException if the commit fails due to transient faults (e.g., a conflict
     *     error, including one detected while preparing or validating). You can retry the
     *     transaction from the beginning
     * @throws CommitException if the commit fails due to transient or nontransient faults
     *     (including a non-conflict failure detected while preparing or validating)
     * @throws UnknownTransactionStatusException if the final commit status cannot be determined.
     *     The outcome is indeterminate—the transaction may or may not have been committed. Do not
     *     blindly retry or roll back; determine the outcome (for example, by checking the
     *     coordinator state, which lazy recovery also relies on) before deciding how to proceed
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
     * Participant#rollbackRecords} on each registered participant.
     *
     * <p>Unlike {@link #commit}, an unknown transaction ID (never begun, or already finished) is a
     * no-op rather than an error: no participants are contacted, and rolling back a transaction the
     * Coordinator does not know about leaves nothing to undo. Any participant context still alive
     * for such a transaction is reclaimed by the participant's own timeout.
     *
     * @param transactionId the canonical transaction ID returned by {@link #begin}
     * @throws RollbackException if the rollback fails due to transient or nontransient faults. Not
     *     thrown for an unknown transaction ID
     */
    void rollback(String transactionId) throws RollbackException;

    /**
     * Closes the Coordinator and releases any resources it holds.
     *
     * <p>Does not commit or roll back transactions that are still in flight; their in-memory state
     * is discarded and any durable outcome is left to lazy recovery.
     */
    @Override
    void close();
  }

  /**
   * Performs CRUD operations and record-level two-phase commit steps for the data stores it owns
   * within a transaction.
   *
   * <p>A single participant may front one or multiple data stores (for example, when using
   * multi-storage). The data stores fronted by one participant are managed as a single unit for the
   * record-level two-phase commit steps.
   *
   * <p>Each participant maintains a local context (snapshot, read set, write set) keyed by
   * transaction ID. The context is created by {@link #join} and discarded once the participant has
   * run the last step the Coordinator drives for it. For a participant with writes that is the
   * commit (via {@link #commitRecords}) or the abort (via {@link #rollbackRecords}); for a
   * write-less participant the Coordinator may skip those steps, so the context is discarded
   * earlier — at {@link #validateRecords}, or at {@link #prepareRecords} when validation is not
   * required either. It is also discarded on a timeout if {@link Coordinator#commit} or {@link
   * Coordinator#rollback} is never reached.
   *
   * <p>The methods on this interface have two different intended callers:
   *
   * <ul>
   *   <li>CRUD methods ({@link #get}, {@link #scan}, {@link #getScanner}, {@link #put(String,
   *       Put)}, {@link #insert}, {@link #upsert}, {@link #update}, {@link #delete}, {@link
   *       #mutate}, {@link #batch}) are driven on behalf of application code (possibly across a
   *       transport boundary, depending on deployment).
   *   <li>Lifecycle and record-level two-phase commit methods ({@link #join}, {@link
   *       #prepareRecords}, {@link #validateRecords}, {@link #commitRecords}, {@link
   *       #rollbackRecords}) are invoked by {@link Coordinator}.
   * </ul>
   *
   * <p>Operations must follow the transaction's lifecycle: CRUD while the transaction is open, then
   * {@link #prepareRecords} → {@link #validateRecords} → {@link #commitRecords} (or {@link
   * #rollbackRecords}). An operation invoked out of order — for example, CRUD or {@link
   * #prepareRecords} after the transaction has been prepared, {@link #validateRecords} before
   * {@link #prepareRecords}, or a mutation on a transaction begun read-only (the {@code readOnly}
   * flag passed to {@link Coordinator#begin} / {@link #join}) — throws {@link
   * IllegalStateException}.
   */
  interface Participant extends AutoCloseable {

    /**
     * Returns the stable logical identifier of this participant.
     *
     * @return the participant ID
     */
    String getId();

    /**
     * Joins an existing transaction and establishes a local context on this participant.
     *
     * <p>Invoked by {@link Coordinator#registerParticipant} (including when {@link
     * Coordinator#begin} is called with a participant); not intended to be invoked directly by
     * other callers. {@code transactionId} must be the canonical ID returned by {@link
     * Coordinator#begin}.
     *
     * <p>If {@code readOnly} is {@code true}, the implementation may skip preparation and commit
     * write paths entirely.
     *
     * @param transactionId the canonical transaction ID returned by {@link Coordinator#begin}
     * @param readOnly whether the transaction is known to be read-only
     * @param attributes implementation-specific transaction attributes (may be empty)
     * @throws TransactionNotFoundException if joining the transaction fails due to transient
     *     faults. You can retry the transaction from the beginning
     * @throws TransactionException if joining the transaction fails due to transient or
     *     nontransient faults. You can try retrying the transaction from the beginning, but the
     *     transaction may still fail if the cause is nontransient
     */
    void join(String transactionId, boolean readOnly, Map<String, String> attributes)
        throws TransactionNotFoundException, TransactionException;

    /**
     * Retrieves a single record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param get the get operation
     * @return an {@code Optional} containing the result if the record exists, otherwise empty
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    Optional<Result> get(String transactionId, Get get)
        throws CrudConflictException, CrudException, TransactionNotFoundException;

    /**
     * Scans records within the transaction and returns all matching results materialized in memory.
     *
     * <p>For potentially large result sets, prefer {@link #getScanner}.
     *
     * @param transactionId the canonical transaction ID
     * @param scan the scan operation
     * @return the list of results
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    List<Result> scan(String transactionId, Scan scan)
        throws CrudConflictException, CrudException, TransactionNotFoundException;

    /**
     * Opens a streaming scanner within the transaction.
     *
     * <p>The returned scanner is stateful and holds resources on the participant. The caller must
     * close it (for example, with try-with-resources) before the transaction proceeds to the commit
     * phase; {@link #prepareRecords} will fail if any scanner for the same transaction is still
     * open.
     *
     * <p>The participant performs best-effort cleanup of leaked scanners at transaction end (via
     * {@link #commitRecords}, {@link #rollbackRecords}, or a timeout).
     *
     * @param transactionId the canonical transaction ID
     * @param scan the scan operation
     * @return a scanner that streams results
     * @throws CrudConflictException if opening the scanner fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if opening the scanner fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
        throws CrudConflictException, CrudException, TransactionNotFoundException;

    /**
     * Puts a record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param put the put operation
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is
     *     not satisfied or the entry does not exist
     * @deprecated As of release 3.19.0. Will be removed in release 4.0.0. Use {@link #insert},
     *     {@link #upsert}, or {@link #update} instead.
     */
    @Deprecated
    void put(String transactionId, Put put)
        throws CrudConflictException, CrudException, TransactionNotFoundException,
            UnsatisfiedConditionException;

    /**
     * Inserts a record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param insert the insert operation
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    void insert(String transactionId, Insert insert)
        throws CrudConflictException, CrudException, TransactionNotFoundException;

    /**
     * Upserts a record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param upsert the upsert operation
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    void upsert(String transactionId, Upsert upsert)
        throws CrudConflictException, CrudException, TransactionNotFoundException;

    /**
     * Updates a record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param update the update operation
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws UnsatisfiedConditionException if a condition is specified on the update and the
     *     condition is not satisfied
     */
    void update(String transactionId, Update update)
        throws CrudConflictException, CrudException, TransactionNotFoundException,
            UnsatisfiedConditionException;

    /**
     * Deletes a record within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param delete the delete operation
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws UnsatisfiedConditionException if a condition is specified on the delete and the
     *     condition is not satisfied
     */
    void delete(String transactionId, Delete delete)
        throws CrudConflictException, CrudException, TransactionNotFoundException,
            UnsatisfiedConditionException;

    /**
     * Applies a batch of mutations within the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param mutations the mutations to apply
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws UnsatisfiedConditionException if a condition is specified on one of the mutations and
     *     the condition is not satisfied
     */
    void mutate(String transactionId, List<? extends Mutation> mutations)
        throws CrudConflictException, CrudException, TransactionNotFoundException,
            UnsatisfiedConditionException;

    /**
     * Applies a batch of mixed operations within the transaction and returns the per-operation
     * results.
     *
     * @param transactionId the canonical transaction ID
     * @param operations the operations to apply
     * @return one result per operation, in input order
     * @throws CrudConflictException if the operation fails due to transient faults (e.g., a
     *     conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the operation fails due to transient or nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws UnsatisfiedConditionException if a condition is specified on one of the operations
     *     and the condition is not satisfied
     */
    List<CrudOperable.BatchResult> batch(String transactionId, List<? extends Operation> operations)
        throws CrudConflictException, CrudException, TransactionNotFoundException,
            UnsatisfiedConditionException;

    /**
     * Writes PREPARED records for the transaction and returns the result of the prepare phase.
     *
     * <p>Invoked by {@link Coordinator#commit}, which aggregates the returned write sets across all
     * participants.
     *
     * <p>Throws {@link IllegalStateException} if any scanner opened on this transaction via {@link
     * #getScanner} is still open; the application must close such scanners before the transaction
     * proceeds to commit.
     *
     * @param transactionId the canonical transaction ID
     * @param preparedAt the timestamp written as the {@code prepared_at} column on the PREPARED
     *     records
     * @return the result of the prepare phase, including the write set produced by this participant
     *     (see {@link PreparationResult})
     * @throws PreparationConflictException if the transaction fails to prepare due to transient
     *     faults (e.g., a conflict error). You can retry the transaction from the beginning
     * @throws PreparationException if the transaction fails to prepare due to transient or
     *     nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     * @throws IllegalStateException if a scanner opened via {@link #getScanner} is still open (a
     *     programming error); being unchecked, it propagates out of {@link Coordinator#commit}
     *     rather than being reported as a commit-level exception
     */
    PreparationResult prepareRecords(String transactionId, long preparedAt)
        throws PreparationConflictException, PreparationException, TransactionNotFoundException;

    /**
     * Performs the validation phase of the transaction.
     *
     * <p>Invoked by {@link Coordinator#commit} between {@link #prepareRecords} and the COMMITTED
     * state write. The Coordinator may skip this call when the prepare result reports that this
     * participant does not require validation (see {@link PreparationResult#isValidationRequired}).
     *
     * @param transactionId the canonical transaction ID
     * @throws ValidationConflictException if the transaction fails to validate due to transient
     *     faults (e.g., a conflict error). You can retry the transaction from the beginning
     * @throws ValidationException if the transaction fails to validate due to transient or
     *     nontransient faults
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., it expired). You can retry the transaction from the beginning
     */
    void validateRecords(String transactionId)
        throws ValidationConflictException, ValidationException, TransactionNotFoundException;

    /**
     * Commits the participant's PREPARED records.
     *
     * <p>Invoked by {@link Coordinator#commit} during the commit phase, once the transaction's
     * outcome has been durably decided. Marks the participant's PREPARED records COMMITTED,
     * stamping {@code committed_at}. The Coordinator may skip this call for a participant that
     * produced no writes (an empty {@link PreparationResult#getWriteSet}).
     *
     * <p>The participant reports failures by throwing, but the Coordinator drives this step
     * best-effort: it does not retry inline and relies on lazy recovery to commit the affected
     * records on a subsequent read. On successful return, the participant discards its local
     * context for the transaction.
     *
     * @param transactionId the canonical transaction ID
     * @param committedAt the timestamp written as the {@code committed_at} column
     * @throws CommitException if committing the records fails due to transient or nontransient
     *     faults. This does not mean the transaction failed to commit—its outcome was already
     *     durably decided before this call; lazy recovery will mark the records COMMITTED on a
     *     subsequent read
     * @throws TransactionNotFoundException if the transaction is not found on this participant
     *     (e.g., its context expired). As with {@link CommitException} above, the outcome was
     *     already durably decided; the Coordinator absorbs this best-effort and lazy recovery
     *     reconciles the records, so there is nothing for the caller to retry
     */
    void commitRecords(String transactionId, long committedAt)
        throws CommitException, TransactionNotFoundException;

    /**
     * Rolls back the participant's PREPARED records.
     *
     * <p>Invoked by {@link Coordinator#rollback}, or by {@link Coordinator#commit} on an internal
     * abort path. Restores the participant's PREPARED records to their pre-transaction state.
     *
     * <p>The participant reports failures by throwing, but the Coordinator drives this step
     * best-effort: it does not retry inline and relies on lazy recovery to roll back the affected
     * records on a subsequent read. On successful return, the participant discards its local
     * context for the transaction.
     *
     * <p>Unlike the other record-level steps, an unknown transaction — one this participant never
     * joined, or whose context it has already released (for example, a write-less participant that
     * released early when its later steps were skipped, or a prior rollback) — is a no-op rather
     * than an error: there is nothing left to undo.
     *
     * @param transactionId the canonical transaction ID
     * @throws RollbackException if rolling back the records of a known transaction fails due to
     *     transient or nontransient faults; never thrown for an unknown transaction, which is a
     *     no-op
     */
    void rollbackRecords(String transactionId) throws RollbackException;

    /**
     * Closes the Participant and releases any resources it holds.
     *
     * <p>Does not terminate transactions whose context is still alive; such contexts are discarded
     * and lazy recovery reconciles any PREPARED records.
     */
    @Override
    void close();
  }

  /**
   * The result of a participant's prepare phase, returned by {@link Participant#prepareRecords}.
   *
   * <p>It lets the participant report what the Coordinator needs to drive the rest of the protocol
   * — the write set to record, whether this participant still requires validation, and whether its
   * records still require committing — so the Coordinator can skip steps that would have nothing to
   * do (see {@link Participant}). It is a dedicated envelope so the prepare phase can report
   * additional information in the future without changing the method signature.
   */
  interface PreparationResult {

    /**
     * Returns the write set produced by this participant (one entry per record written or deleted).
     *
     * @return the write set; empty if the participant has no writes
     */
    List<WriteSetEntry> getWriteSet();

    /**
     * Returns whether this participant still requires {@link Participant#validateRecords} for this
     * transaction. When {@code false}, the Coordinator may skip validating this participant.
     *
     * <p>Validation is only meaningful under SERIALIZABLE isolation; under SNAPSHOT isolation it is
     * a no-op, so an implementation returns {@code false}. Even under SERIALIZABLE it can be
     * skipped when the participant read nothing that needs re-checking at commit time (for example,
     * a participant whose reads are all covered by its own writes).
     *
     * @return {@code true} if validation is required for this participant
     */
    boolean isValidationRequired();

    /**
     * Returns whether this participant still requires {@link Participant#commitRecords} for this
     * transaction. When {@code false}, the Coordinator may skip committing this participant.
     *
     * <p>A participant that produced no records to commit (an empty {@link #getWriteSet}) does not
     * require committing. This is the single source of truth for the commit-step skip decision: the
     * Coordinator drives {@code commitRecords} only when this returns {@code true}, and a
     * participant whose last driven step is therefore not {@code commitRecords} releases its own
     * context at that earlier step.
     *
     * @return {@code true} if committing is required for this participant
     */
    boolean isCommitRequired();
  }

  /** A single record-level write or delete in a transaction's write set. */
  interface WriteSetEntry {

    /** Whether this entry represents a write (insert/upsert/update) or a delete. */
    enum Type {
      WRITE,
      DELETE
    }

    /**
     * Returns whether this entry is a write or a delete.
     *
     * @return the entry type
     */
    Type getType();

    /**
     * Returns the namespace name of the record.
     *
     * @return the namespace name
     */
    String getNamespaceName();

    /**
     * Returns the table name of the record.
     *
     * @return the table name
     */
    String getTableName();

    /**
     * Returns the partition key of the record.
     *
     * @return the partition key
     */
    Key getPartitionKey();

    /**
     * Returns the clustering key of the record, if any.
     *
     * @return an {@code Optional} containing the clustering key, or empty if the table has none
     */
    Optional<Key> getClusteringKey();

    /**
     * Returns the non-key columns written by this entry.
     *
     * <p>Always empty when {@link #getType} is {@link Type#DELETE}. May also be empty for {@link
     * Type#WRITE} when the implementation chose to persist only the keys (for example, for the
     * active-recovery use case).
     *
     * @return the columns
     */
    List<Column<?>> getColumns();
  }
}
