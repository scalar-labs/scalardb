package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommit.Participant;
import com.scalar.db.api.TwoPhaseCommit.WriteSetEntry;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consensus-commit-backed {@link TwoPhaseCommit.Coordinator} implementation.
 *
 * <p>Drives the two-phase commit protocol across the participants registered for a transaction
 * (prepare -&gt; validate -&gt; write COMMITTED state -&gt; commit records, with the abort/rollback
 * path on failure) and holds the per-transaction state in an in-memory map keyed by canonical
 * transaction ID. Each entry records the registered participants and the read-only flag and
 * attributes passed at {@code begin}. The map is cleaned up after every {@code commit} or {@code
 * rollback}.
 *
 * <p>TODO: support group commit on the new Coordinator (tracked separately from this change). This
 * is a planned extension, not a design limitation: the {@code WriteSet} persistence proto's {@code
 * child_id} (group-commit dimension) and {@code participant_id} (two-phase-commit dimension) are
 * orthogonal and already compose, and {@link CoordinatorStateAccessor#getState(String)} already
 * resolves full (parent + child) transaction IDs. The remaining work is reserving a group-commit
 * slot in {@code begin} (so the transaction ID becomes a full key) and routing the COMMITTED-state
 * write through the group-commit variant of the Coordinator-side handler.
 */
@ThreadSafe
public class ConsensusCommitCoordinator implements TwoPhaseCommit.Coordinator {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommitCoordinator.class);

  private final DistributedStorage storage;
  private final CoordinatorCommitHandler coordinatorCommitHandler;
  private final boolean coordinatorWriteOmissionOnReadOnlyEnabled;

  private final ConcurrentMap<String, CoordinatorContext> contexts = new ConcurrentHashMap<>();

  public ConsensusCommitCoordinator(DatabaseConfig databaseConfig) {
    ConsensusCommitConfig config = new ConsensusCommitConfig(databaseConfig);
    throwIfGroupCommitIsEnabled(config);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    this.storage = storageFactory.getStorage();
    this.coordinatorCommitHandler =
        new CoordinatorCommitHandler(new CoordinatorStateAccessor(storage, config));
    this.coordinatorWriteOmissionOnReadOnlyEnabled =
        config.isCoordinatorWriteOmissionOnReadOnlyEnabled();
  }

  @VisibleForTesting
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  ConsensusCommitCoordinator(
      CoordinatorCommitHandler coordinatorCommitHandler, ConsensusCommitConfig config) {
    throwIfGroupCommitIsEnabled(config);
    this.storage = null;
    this.coordinatorCommitHandler = checkNotNull(coordinatorCommitHandler);
    this.coordinatorWriteOmissionOnReadOnlyEnabled =
        config.isCoordinatorWriteOmissionOnReadOnlyEnabled();
  }

  // TODO: Remove this guard once group commit is supported on the new Coordinator (see the class
  // Javadoc). It is a temporary not-yet-implemented gate, not a permanent restriction.
  private static void throwIfGroupCommitIsEnabled(ConsensusCommitConfig config) {
    if (CoordinatorGroupCommitter.isEnabled(config)) {
      throw new IllegalArgumentException(
          "Group commit is not yet supported on the new Coordinator implementation");
    }
  }

  @Override
  public String begin(
      @Nullable String transactionId,
      boolean readOnly,
      Map<String, String> attributes,
      @Nullable Participant participant)
      throws TransactionException {
    // Use the caller-supplied ID when present; otherwise generate one. Group commit is not yet
    // supported on this path, so no parent-prefix logic is needed.
    String canonical = transactionId != null ? transactionId : UUID.randomUUID().toString();
    CoordinatorContext existing =
        contexts.putIfAbsent(canonical, new CoordinatorContext(canonical, readOnly, attributes));
    if (existing != null) {
      throw new TransactionException(
          CoreError.TRANSACTION_ALREADY_EXISTS.buildMessage(), canonical);
    }
    if (participant != null) {
      // Register the optional participant exactly as registerParticipant would. If join fails, drop
      // the just-created context so a failed begin leaves no orphaned state.
      try {
        registerParticipant(canonical, participant);
      } catch (TransactionException e) {
        releaseResources(canonical);
        throw e;
      }
    }
    return canonical;
  }

  @Override
  public void registerParticipant(String transactionId, Participant participant)
      throws TransactionException {
    CoordinatorContext context = getContext(transactionId);
    synchronized (context) {
      context.checkActive();
      // Registration is idempotent per participant ID: if a participant with the same getId() is
      // already registered, this is a no-op (the participant is not joined again). commit() keys
      // each participant's write set by getId(), so registering the same ID twice would otherwise
      // merge into one EntryGroup and misattribute the persisted records.
      for (Participant registered : context.participants) {
        if (registered.getId().equals(participant.getId())) {
          return;
        }
      }
      // Invoke Participant.join first; only on success do we add the participant to the list so
      // subsequent commit/rollback drives only successfully-joined participants.
      participant.join(transactionId, context.readOnly, context.attributes);
      context.participants.add(participant);
    }
  }

  @Override
  public void commit(String transactionId)
      throws CommitException, UnknownTransactionStatusException, TransactionNotFoundException {
    CoordinatorContext context = getContext(transactionId);
    synchronized (context) {
      context.checkActive();
      try {
        List<Participant> participants = context.participants;

        // Key each participant's write set by its stable ID so the persisted commit-state proto is
        // stamped with the owning participant.
        Map<String, List<WriteSetEntry>> writeSetsByParticipant = new LinkedHashMap<>();

        // A write-less transaction (every participant returns an empty write set) writes no
        // COMMITTED Coordinator state row when coordinator-write omission on read-only is enabled.
        // Only the Coordinator state writes (commitState/abortState) are gated on this; the
        // record-level steps (prepareRecords / commitRecords / rollbackRecords) are still driven on
        // every participant so their local contexts are released.
        boolean hasWrites = false;

        long preparedAt = System.currentTimeMillis();
        try {
          for (Participant participant : participants) {
            List<WriteSetEntry> entries =
                participant.prepareRecords(transactionId, preparedAt).getWriteSet();
            if (!entries.isEmpty()) {
              hasWrites = true;
            }
            writeSetsByParticipant
                .computeIfAbsent(participant.getId(), k -> new ArrayList<>())
                .addAll(entries);
          }
          for (Participant participant : participants) {
            participant.validateRecords(transactionId);
          }
        } catch (PreparationException | ValidationException e) {
          // Prepare or validate failed. Abort the transaction and roll back the PREPARED records on
          // every participant. The prepare/validate phases are internal to commit(), so the failure
          // is reported as a commit-level exception: a conflict as CommitConflictException so the
          // caller's retry logic still sees it as retriable, anything else as CommitException.
          abortAndRollbackRecords(transactionId, participants, hasWrites);
          if (e instanceof PreparationConflictException
              || e instanceof ValidationConflictException) {
            throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
          }
          throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
        } catch (TransactionNotFoundException e) {
          // A participant no longer knows this transaction (its local context is gone, e.g. it
          // expired). The transaction cannot commit: abort it and roll back the records already
          // PREPARED on the other participants, then surface the TransactionNotFoundException as-is
          // (the facade maps it to a retriable conflict).
          abortAndRollbackRecords(transactionId, participants, hasWrites);
          throw e;
        }
        long committedAt;
        if (hasWrites || !coordinatorWriteOmissionOnReadOnlyEnabled) {
          try {
            committedAt = commitState(transactionId, writeSetsByParticipant);
          } catch (CommitConflictException e) {
            // The COMMITTED-state write lost a putState race that resolved to ABORTED: the
            // transaction is aborted. Roll back the PREPARED records on every participant before
            // surfacing it.
            for (Participant participant : participants) {
              bestEffortRollbackRecords(participant, transactionId);
            }
            throw e;
          }
        } else {
          // Write-less transaction with the Coordinator write omitted: committedAt is an unused
          // placeholder for the (no-op) commitRecords calls below.
          committedAt = preparedAt;
        }
        for (Participant participant : participants) {
          bestEffortCommitRecords(participant, transactionId, committedAt);
        }
      } finally {
        context.markReleased();
        releaseResources(transactionId);
      }
    }
  }

  @Override
  public void rollback(String transactionId) {
    CoordinatorContext context = contexts.get(transactionId);
    if (context == null) {
      // Unknown or already-finished transaction: nothing to roll back. Lenient no-op (unlike
      // commit, which treats an unknown transaction as a caller error).
      return;
    }
    synchronized (context) {
      if (context.isReleased()) {
        // Already terminated by a concurrent commit/rollback; nothing to roll back.
        return;
      }
      try {
        // This is an application rollback. Records are only ever PREPARED inside commit() (which
        // handles its own abort and releases the context), so nothing is PREPARED here: no storage
        // rollback and no ABORTED state write happen (an absent Coordinator state row is treated as
        // ABORTED by lazy recovery). rollbackRecords is driven on each participant only to release
        // its resources — close scanners and discard its in-memory snapshot/context.
        for (Participant participant : context.participants) {
          bestEffortRollbackRecords(participant, transactionId);
        }
      } finally {
        context.markReleased();
        releaseResources(transactionId);
      }
    }
  }

  @Override
  public void close() {
    storage.close();
  }

  // Aborts the transaction and rolls back the PREPARED records on every participant. The ABORTED
  // Coordinator state write is skipped for a write-less transaction when coordinator-write omission
  // on read-only is enabled, since an absent Coordinator state row is treated as ABORTED by lazy
  // recovery.
  private void abortAndRollbackRecords(
      String transactionId, List<Participant> participants, boolean hasWrites)
      throws UnknownTransactionStatusException {
    if (hasWrites || !coordinatorWriteOmissionOnReadOnlyEnabled) {
      abortState(transactionId);
    }
    for (Participant participant : participants) {
      bestEffortRollbackRecords(participant, transactionId);
    }
  }

  private void bestEffortCommitRecords(Participant participant, String txId, long committedAt) {
    try {
      participant.commitRecords(txId, committedAt);
    } catch (Exception e) {
      // Best-effort step: the records are left PREPARED and lazy recovery commits them on a
      // subsequent read of the record (no background sweep). Logged at WARN since the records hold
      // locks until then.
      logger.warn(
          "commitRecords failed; the records are left PREPARED and will be committed by lazy "
              + "recovery on a subsequent read. Transaction ID: {}",
          txId,
          e);
    }
  }

  private void bestEffortRollbackRecords(Participant participant, String txId) {
    try {
      participant.rollbackRecords(txId);
    } catch (Exception e) {
      // Best-effort step: the records are left PREPARED and lazy recovery rolls them back on a
      // subsequent read of the record (no background sweep). Logged at WARN since the records hold
      // locks until then.
      logger.warn(
          "rollbackRecords failed; the records are left PREPARED and will be rolled back by lazy "
              + "recovery on a subsequent read. Transaction ID: {}",
          txId,
          e);
    }
  }

  // Resolves the in-memory context for a known transaction, or throws if none is registered (never
  // begun on this Coordinator, or already finished by a prior commit/rollback).
  private CoordinatorContext getContext(String transactionId) throws TransactionNotFoundException {
    CoordinatorContext context = contexts.get(transactionId);
    if (context == null) {
      throw new TransactionNotFoundException(
          CoreError.TRANSACTION_NOT_FOUND.buildMessage(), transactionId);
    }
    return context;
  }

  // Encodes the per-participant write sets and writes the COMMITTED state row via the
  // Coordinator-side handler, which generates the committedAt and returns it so the records are
  // committed with the same timestamp.
  private long commitState(
      String transactionId, Map<String, List<WriteSetEntry>> writeSetsByParticipant)
      throws CommitConflictException, UnknownTransactionStatusException {
    WriteSet writeSet = WriteSetEncoder.encodeFromWriteSetEntries(writeSetsByParticipant, false);
    return coordinatorCommitHandler.commitState(transactionId, writeSet);
  }

  // Writes the ABORTED state row via the Coordinator-side handler. ABORTED rows carry no write set.
  private void abortState(String transactionId) throws UnknownTransactionStatusException {
    coordinatorCommitHandler.abortState(transactionId, null);
  }

  private void releaseResources(String transactionId) {
    contexts.remove(transactionId);
  }

  /**
   * Per-transaction state owned by the Coordinator, plus a lifecycle status.
   *
   * <p>The status rejects operations on a terminated transaction. The entry is removed from {@link
   * #contexts} at {@code commit} / {@code rollback} (no terminal retention); {@link
   * Status#RELEASED} only lets a concurrent in-flight operation that obtained its reference before
   * the removal observe the termination under the lock. All {@code status} reads and transitions
   * must be performed while synchronized on the instance.
   */
  private static final class CoordinatorContext {

    private enum Status {
      ACTIVE,
      RELEASED
    }

    final String transactionId;
    final boolean readOnly;
    final Map<String, String> attributes;
    final List<Participant> participants = new ArrayList<>();
    private Status status = Status.ACTIVE;

    CoordinatorContext(String transactionId, boolean readOnly, Map<String, String> attributes) {
      this.transactionId = transactionId;
      this.readOnly = readOnly;
      // Defensive copy of attributes since the map is held for the lifetime of the transaction.
      this.attributes =
          attributes.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new HashMap<>(attributes));
    }

    /** Throws if the transaction has already been terminated (committed or rolled back). */
    void checkActive() throws TransactionNotFoundException {
      if (status == Status.RELEASED) {
        throw new TransactionNotFoundException(
            CoreError.TRANSACTION_NOT_FOUND.buildMessage(), transactionId);
      }
    }

    boolean isReleased() {
      return status == Status.RELEASED;
    }

    void markReleased() {
      status = Status.RELEASED;
    }
  }
}
