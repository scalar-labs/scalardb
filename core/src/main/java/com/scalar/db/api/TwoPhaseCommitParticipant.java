package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The participant role of the internal two-phase commit primitives; {@link
 * TwoPhaseCommitCoordinator} drives the protocol and documents the overall two-phase-commit design.
 * Like the coordinator role, this is an internal interface not invoked by application code
 * directly; breaking changes can and will be introduced to it, and users should not depend on it.
 *
 * <p>{@link PreparationResult}, {@link WriteSetEntry}, and {@link WriteSetDetailLevel} are nested
 * in this interface as they belong to the participant's contract.
 *
 * <p>Performs CRUD operations and record-level two-phase commit steps for the data stores it owns
 * within a transaction.
 *
 * <p>A single participant may front one or multiple data stores (for example, when using
 * multi-storage). The data stores fronted by one participant are managed as a single unit for the
 * record-level two-phase commit steps.
 *
 * <p>Each participant maintains a local context (snapshot, read set, write set) keyed by
 * transaction ID. The context is created by {@link #join} and discarded once the participant has
 * run the last step the Coordinator drives for it. For a participant with writes that is the commit
 * (via {@link #commitRecords}) or the abort (via {@link #rollbackRecords}); for a write-less
 * participant the Coordinator may skip those steps, so the context is discarded earlier — at {@link
 * #validateRecords}, or at {@link #prepareRecords} when validation is not required either. It is
 * also discarded on a timeout if {@link TwoPhaseCommitCoordinator#commit} or {@link
 * TwoPhaseCommitCoordinator#rollback} is never reached.
 *
 * <p>The methods on this interface have three different intended callers:
 *
 * <ul>
 *   <li>CRUD methods ({@link #get}, {@link #scan}, {@link #getScanner}, {@link #put(String, Put)},
 *       {@link #insert}, {@link #upsert}, {@link #update}, {@link #delete}, {@link #mutate}, {@link
 *       #batch}) are driven on behalf of application code (possibly across a transport boundary,
 *       depending on deployment).
 *   <li>Lifecycle and record-level two-phase commit methods ({@link #join}, {@link
 *       #prepareRecords}, {@link #validateRecords}, {@link #commitRecords}, {@link
 *       #rollbackRecords}) are invoked by {@link TwoPhaseCommitCoordinator}.
 *   <li>{@link #releaseTransactionContext} and {@link #hasTransactionContext} are invoked by
 *       neither — they are driven by a context-reaper/decorator: the former a reap-only terminal
 *       that reclaims an abandoned or idle-reaped context without touching storage, the latter a
 *       liveness probe consulted before such a reap.
 * </ul>
 *
 * <p>Operations must follow the transaction's lifecycle: CRUD while the transaction is open, then
 * {@link #prepareRecords} → {@link #validateRecords} → {@link #commitRecords} (or {@link
 * #rollbackRecords}). An operation invoked out of order — for example, CRUD or {@link
 * #prepareRecords} after the transaction has been prepared, {@link #validateRecords} before {@link
 * #prepareRecords}, or a mutation on a transaction begun read-only (the {@code readOnly} flag
 * passed to {@link TwoPhaseCommitCoordinator#begin} / {@link #join}) — throws {@link
 * IllegalStateException}.
 *
 * <p><b>Concurrency.</b> Implementations must serialize per-transaction work: for one transaction
 * ID the methods here must be mutually exclusive, so concurrent calls cannot corrupt that
 * transaction's context. Because a context-reaper drives {@link #releaseTransactionContext} and
 * {@link #hasTransactionContext} from its own thread (see above), either may be invoked
 * concurrently with an in-flight CRUD or record-level call for the same transaction ID. Calls for
 * different transaction IDs may proceed concurrently. Decorators that add a background reaper rely
 * on this contract for their own thread-safety.
 */
public interface TwoPhaseCommitParticipant extends AutoCloseable {

  /**
   * Returns the stable logical identifier of this participant.
   *
   * @return the participant ID
   */
  String getId();

  /**
   * Joins an existing transaction and establishes a local context on this participant.
   *
   * <p>Invoked by {@link TwoPhaseCommitCoordinator#registerParticipant} (including when {@link
   * TwoPhaseCommitCoordinator#begin} is called with a participant); not intended to be invoked
   * directly by other callers. {@code transactionId} must be the canonical ID returned by {@link
   * TwoPhaseCommitCoordinator#begin}.
   *
   * <p>If {@code readOnly} is {@code true}, the implementation may skip preparation and commit
   * write paths entirely.
   *
   * @param transactionId the canonical transaction ID returned by {@link
   *     TwoPhaseCommitCoordinator#begin}
   * @param readOnly whether the transaction is known to be read-only
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @throws TransactionNotFoundException if joining the transaction fails due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if joining the transaction fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  void join(String transactionId, boolean readOnly, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Retrieves a single record within the transaction.
   *
   * @param transactionId the canonical transaction ID
   * @param get the get operation
   * @return an {@code Optional} containing the result if the record exists, otherwise empty
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
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
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   */
  List<Result> scan(String transactionId, Scan scan)
      throws CrudConflictException, CrudException, TransactionNotFoundException;

  /**
   * Opens a streaming scanner within the transaction.
   *
   * <p>The returned scanner is stateful and holds resources on the participant. The caller must
   * close it (for example, with try-with-resources) before the transaction proceeds to the commit
   * phase; {@link #prepareRecords} will fail if any scanner for the same transaction is still open.
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
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   */
  TransactionCrudOperable.Scanner getScanner(String transactionId, Scan scan)
      throws CrudConflictException, CrudException, TransactionNotFoundException;

  /**
   * Puts a record within the transaction.
   *
   * @param transactionId the canonical transaction ID
   * @param put the put operation
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.19.0. Will be removed in release 4.0.0. Use {@link #insert}, {@link
   *     #upsert}, or {@link #update} instead.
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
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   */
  void insert(String transactionId, Insert insert)
      throws CrudConflictException, CrudException, TransactionNotFoundException;

  /**
   * Upserts a record within the transaction.
   *
   * @param transactionId the canonical transaction ID
   * @param upsert the upsert operation
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   */
  void upsert(String transactionId, Upsert upsert)
      throws CrudConflictException, CrudException, TransactionNotFoundException;

  /**
   * Updates a record within the transaction.
   *
   * @param transactionId the canonical transaction ID
   * @param update the update operation
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
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
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
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
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
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
   * @throws CrudConflictException if the operation fails due to transient faults (e.g., a conflict
   *     error). You can retry the transaction from the beginning
   * @throws CrudException if the operation fails due to transient or nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   * @throws UnsatisfiedConditionException if a condition is specified on one of the operations and
   *     the condition is not satisfied
   */
  List<CrudOperable.BatchResult> batch(String transactionId, List<? extends Operation> operations)
      throws CrudConflictException, CrudException, TransactionNotFoundException,
          UnsatisfiedConditionException;

  /**
   * Writes PREPARED records for the transaction and returns the result of the prepare phase.
   *
   * <p>Invoked by {@link TwoPhaseCommitCoordinator#commit}, which aggregates the returned write
   * sets across all participants.
   *
   * <p>{@code detailLevel} controls only how much of each record the returned {@link
   * WriteSetEntry}s carry (see {@link WriteSetDetailLevel}): under {@link
   * WriteSetDetailLevel#KEYS_ONLY} every entry's {@link WriteSetEntry#getColumns()} must be empty,
   * while under {@link WriteSetDetailLevel#FULL} every {@link WriteSetEntry.Type#WRITE} entry
   * carries the non-key columns it writes. It never changes which entries are returned: the entries
   * and their keys are identical at either level, so an empty write set means a write-less
   * participant regardless of the requested detail.
   *
   * <p>Throws {@link IllegalStateException} if any scanner opened on this transaction via {@link
   * #getScanner} is still open; the application must close such scanners before the transaction
   * proceeds to commit.
   *
   * @param transactionId the canonical transaction ID
   * @param preparedAt the timestamp written as the {@code prepared_at} column on the PREPARED
   *     records
   * @param detailLevel how much detail the returned {@link WriteSetEntry}s must carry
   * @return the result of the prepare phase, including the write set produced by this participant
   *     (see {@link PreparationResult})
   * @throws PreparationConflictException if the transaction fails to prepare due to transient
   *     faults (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws PreparationException if the transaction fails to prepare due to transient or
   *     nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   * @throws IllegalStateException if a scanner opened via {@link #getScanner} is still open (a
   *     programming error); being unchecked, it propagates out of {@link
   *     TwoPhaseCommitCoordinator#commit} rather than being reported as a commit-level exception
   */
  PreparationResult prepareRecords(
      String transactionId, long preparedAt, WriteSetDetailLevel detailLevel)
      throws PreparationConflictException, PreparationException, TransactionNotFoundException;

  /**
   * Performs the validation phase of the transaction.
   *
   * <p>Invoked by {@link TwoPhaseCommitCoordinator#commit} between {@link #prepareRecords} and the
   * COMMITTED state write. The Coordinator may skip this call when the prepare result reports that
   * this participant does not require validation (see {@link
   * PreparationResult#isValidationRequired}).
   *
   * @param transactionId the canonical transaction ID
   * @throws ValidationConflictException if the transaction fails to validate due to transient
   *     faults (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws ValidationException if the transaction fails to validate due to transient or
   *     nontransient faults
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     it expired). You can retry the transaction from the beginning
   */
  void validateRecords(String transactionId)
      throws ValidationConflictException, ValidationException, TransactionNotFoundException;

  /**
   * Commits the participant's PREPARED records.
   *
   * <p>Invoked by {@link TwoPhaseCommitCoordinator#commit} during the commit phase, once the
   * transaction's outcome has been durably decided. Marks the participant's PREPARED records
   * COMMITTED, stamping {@code committed_at}. The Coordinator may skip this call for a participant
   * that produced no writes (an empty {@link PreparationResult#getWriteSet}).
   *
   * <p>The participant reports failures by throwing, but the Coordinator drives this step
   * best-effort: it does not retry inline and relies on lazy recovery to commit the affected
   * records on a subsequent read. On successful return, the participant discards its local context
   * for the transaction.
   *
   * @param transactionId the canonical transaction ID
   * @param committedAt the timestamp written as the {@code committed_at} column
   * @throws CommitException if committing the records fails due to transient or nontransient
   *     faults. This does not mean the transaction failed to commit—its outcome was already durably
   *     decided before this call; lazy recovery will mark the records COMMITTED on a subsequent
   *     read
   * @throws TransactionNotFoundException if the transaction is not found on this participant (e.g.,
   *     its context expired). As with {@link CommitException} above, the outcome was already
   *     durably decided; the Coordinator absorbs this best-effort and lazy recovery reconciles the
   *     records, so there is nothing for the caller to retry
   */
  void commitRecords(String transactionId, long committedAt)
      throws CommitException, TransactionNotFoundException;

  /**
   * Rolls back the participant's PREPARED records.
   *
   * <p>Invoked by {@link TwoPhaseCommitCoordinator#rollback}, or by {@link
   * TwoPhaseCommitCoordinator#commit} on an internal abort path. Restores the participant's
   * PREPARED records to their pre-transaction state.
   *
   * <p>The participant reports failures by throwing, but the Coordinator drives this step
   * best-effort: it does not retry inline and relies on lazy recovery to roll back the affected
   * records on a subsequent read. On successful return, the participant discards its local context
   * for the transaction.
   *
   * <p>Unlike the other record-level steps, an unknown transaction — one this participant never
   * joined, or whose context it has already released (for example, a write-less participant that
   * released early when its later steps were skipped, or a prior rollback) — is not an error: there
   * is nothing left for this participant to undo. Implementations report it as a silent no-op or as
   * a {@link TransactionNotFoundException}, never as a {@link RollbackException}.
   *
   * @param transactionId the canonical transaction ID
   * @throws TransactionNotFoundException if the implementation reports the unknown-transaction
   *     no-op on its conventional not-found channel instead of returning silently (typical across a
   *     transport boundary). An alternative carrier of the no-op outcome — there was nothing left
   *     for this participant to undo — so callers must treat it like a normal return. Routine on
   *     abort paths: the Coordinator rolls back every participant uniformly, including ones that
   *     already self-released
   * @throws RollbackException if rolling back the records of a known transaction fails due to
   *     transient or nontransient faults; never thrown for an unknown transaction
   */
  void rollbackRecords(String transactionId) throws RollbackException, TransactionNotFoundException;

  /**
   * Returns whether this participant still holds a local context for the transaction.
   *
   * <p>This is a liveness probe, not an operation on the transaction, and it carries two contracts
   * that differ from every other method on this interface:
   *
   * <ul>
   *   <li><b>Quiet.</b> The probe must not count as transaction activity: it must not refresh any
   *       idle-expiration timer or usage recency that the implementation (or a decorator around it)
   *       maintains for the transaction. Probing sides extend lifetimes based on the answer, so a
   *       probe that itself counted as activity would form a feedback loop that keeps abandoned
   *       transactions alive forever.
   *   <li><b>Time-bounded.</b> Callers invoke this from housekeeping paths (e.g. an expiry reaper);
   *       an implementation that crosses a network must bound the probe with its own deadline
   *       rather than block indefinitely.
   * </ul>
   *
   * @param transactionId the canonical transaction ID returned by {@link
   *     TwoPhaseCommitCoordinator#begin}
   * @return true if a live context for the transaction is held; false if it is definitely not
   *     (never joined, already finished, or already reaped)
   * @throws TransactionNotFoundException if this participant definitely holds no context for the
   *     transaction. This is an alternative carrier of the {@code false} answer: a remote
   *     implementation may surface its conventional not-found error mapping - for example, a probe
   *     rerouted to a successor node after a crash, where the context is genuinely gone - instead
   *     of converting it to a return value. Callers must treat it exactly like a {@code false}
   *     return. Note the inversion from every other method on this interface, where this exception
   *     marks a transient, retriable condition: here it is an authoritative answer that callers act
   *     on terminally (by reaping). An implementation must therefore throw it only when the absence
   *     is authoritative for the participant as a whole - never for a transient miss, such as a
   *     probe answered by a node that does not hold the context while another node still might. A
   *     condition like that must surface as {@link TransactionException} (fail-open) instead
   * @throws TransactionException if the probe fails due to transient or nontransient faults (for
   *     example, a remote participant that cannot be reached, or one that does not implement the
   *     probe). Callers must treat a failed probe conservatively, as possibly present
   */
  boolean hasTransactionContext(String transactionId)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Releases the participant's in-memory context for the transaction without touching storage.
   *
   * <p>This is a reap-only terminal used to reclaim a local context that did not reach {@link
   * #commitRecords} / {@link #rollbackRecords} — for example, an abandoned transaction, or one
   * reaped on idle by a decorator. It closes any scanners and discards the in-memory snapshot for
   * the transaction; it does <strong>not</strong> roll back or commit any PREPARED records — those
   * are reconciled by lazy recovery on a subsequent read.
   *
   * <p>An unknown transaction — never joined, or whose context was already released — is a no-op. A
   * decorator may treat this as a terminal step, releasing any per-transaction resources it holds.
   *
   * <p>Callers invoke this from housekeeping paths (e.g. an idle reaper), or best-effort while a
   * client awaits a terminal outcome; an implementation that crosses a network must bound the call
   * with its own deadline rather than block indefinitely.
   *
   * @param transactionId the canonical transaction ID
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
   * Closes the Participant and releases any resources it holds.
   *
   * <p>Does not terminate transactions whose context is still alive; such contexts are discarded
   * and lazy recovery reconciles any PREPARED records.
   */
  @Override
  void close();

  /**
   * The result of a participant's prepare phase, returned by {@link
   * TwoPhaseCommitParticipant#prepareRecords}.
   *
   * <p>It lets the participant report what the Coordinator needs to drive the rest of the protocol
   * — the write set to record, whether this participant still requires validation, and whether its
   * records still require committing — so the Coordinator can skip steps that would have nothing to
   * do (see {@link TwoPhaseCommitParticipant}). It is a dedicated envelope so the prepare phase can
   * report additional information in the future without changing the method signature.
   */
  interface PreparationResult {

    /**
     * Returns the write set produced by this participant (one entry per record written or deleted).
     *
     * @return the write set; empty if the participant has no writes
     */
    List<WriteSetEntry> getWriteSet();

    /**
     * Returns whether this participant still requires {@link
     * TwoPhaseCommitParticipant#validateRecords} for this transaction. When {@code false}, the
     * Coordinator may skip validating this participant.
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
     * Returns whether this participant still requires {@link
     * TwoPhaseCommitParticipant#commitRecords} for this transaction. When {@code false}, the
     * Coordinator may skip committing this participant.
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
     * <p>Always empty when {@link #getType} is {@link Type#DELETE}, and always empty — even for
     * {@link Type#WRITE} — when the Coordinator requested {@link WriteSetDetailLevel#KEYS_ONLY}
     * from {@link TwoPhaseCommitParticipant#prepareRecords}.
     *
     * @return the columns
     */
    List<Column<?>> getColumns();
  }

  /**
   * How much of the record content the {@link WriteSetEntry}s returned by {@link
   * TwoPhaseCommitParticipant#prepareRecords} must carry, as requested by the Coordinator.
   *
   * <p>The detail level never affects which entries a write set contains — the entries and their
   * primary keys are identical at every level; it only controls whether {@link
   * WriteSetEntry#getColumns()} is populated. {@link WriteSetEntry.Type#DELETE} entries never carry
   * columns at any level.
   *
   * <p>This enum is a single ordered axis of record-content detail, and {@link #FULL} is its
   * permanent ceiling. Additions that go beyond the record content of a write — transaction
   * metadata, before images, and the like — are not higher levels; they belong on a separate axis
   * (for example, dedicated {@link WriteSetEntry} fields gated by their own flag), so that {@code
   * FULL} keeps meaning exactly "the whole record content".
   */
  enum WriteSetDetailLevel {
    /**
     * Every entry carries only its primary keys; {@link WriteSetEntry#getColumns()} must be empty.
     * Sufficient when the Coordinator does not persist column values (for example, when the write
     * set is recorded for the active-recovery use case), and keeps the prepare result minimal.
     */
    KEYS_ONLY,

    /**
     * Every {@link WriteSetEntry.Type#WRITE} entry additionally carries the non-key columns it
     * writes — the full user-visible record content of the write. Implementation-internal columns
     * (for example, transaction-metadata columns) are not part of the record content at this
     * interface and are never included. Note that this is the write's own content, not a full row
     * image: a partial update carries exactly the columns it sets, not the rest of the row.
     * Intended for use cases that persist the record content (for example, backup/changelog
     * capture).
     */
    FULL
  }
}
