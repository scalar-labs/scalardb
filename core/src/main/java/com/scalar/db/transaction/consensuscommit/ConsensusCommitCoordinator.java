package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommit.Participant;
import com.scalar.db.api.TwoPhaseCommit.PreparationResult;
import com.scalar.db.api.TwoPhaseCommit.WriteSetDetailLevel;
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
        // The abort path applies the same omission when the transaction is known write-less (see
        // the knownWriteLess arguments below and abortAndRollbackRecords).
        boolean hasWrites = false;

        // Drive only the steps each participant still needs (mirroring CommitHandler, minus the
        // one-phase fast path): validateRecords only where the prepare result reports it required,
        // and commitRecords only where the prepare result reports it required. A participant whose
        // later steps are all skipped releases its own context at its last driven step, so the
        // Coordinator must not drive it again.
        List<Participant> toValidate = new ArrayList<>(participants.size());
        List<Participant> toCommit = new ArrayList<>(participants.size());

        long preparedAt = System.currentTimeMillis();
        try {
          for (Participant participant : participants) {
            PreparationResult result =
                participant.prepareRecords(
                    transactionId, preparedAt, WriteSetDetailLevel.KEYS_ONLY);
            List<WriteSetEntry> entries = result.getWriteSet();
            // hasWrites tracks whether any participant produced PREPARED records; it gates writing
            // the COMMITTED Coordinator state row.
            if (!entries.isEmpty()) {
              hasWrites = true;
            }
            // Drive validateRecords only on participants that still require it; a participant that
            // does not has already self-released its context if validateRecords was its last step.
            if (result.isValidationRequired()) {
              toValidate.add(participant);
            }
            // Drive commitRecords only on participants that still require it. A write-less
            // participant requires no commit and has already self-released its context
            // (commitRecords would be a no-op); isCommitRequired is kept in agreement with that
            // self-release. See ConsensusCommitParticipant#prepareRecords / #validateRecords.
            if (result.isCommitRequired()) {
              toCommit.add(participant);
            }
            writeSetsByParticipant
                .computeIfAbsent(participant.getId(), k -> new ArrayList<>())
                .addAll(entries);
          }
        } catch (PreparationException e) {
          // Prepare failed. Abort the transaction and roll back every participant: writers undo
          // their PREPARED records; others (not yet prepared, or already self-released) just
          // release, and rollbackRecords is a no-op for an already-released participant. hasWrites
          // is NOT trustworthy here — a participant that threw part-way through prepareRecords
          // never returned its write set — so only the static readOnly flag can prove the
          // transaction write-less. The prepare phase is internal to commit(), so the failure is
          // reported as a commit-level exception: a conflict as CommitConflictException so the
          // caller's retry logic still sees it as retriable, anything else as CommitException.
          abortAndRollbackRecords(transactionId, context.readOnly, participants);
          if (e instanceof PreparationConflictException) {
            throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
          }
          throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
        } catch (TransactionNotFoundException e) {
          // A participant no longer knows this transaction while preparing (its local context is
          // gone, e.g. it expired). The transaction cannot commit: abort it and roll back the
          // records already PREPARED on the other participants, then surface the
          // TransactionNotFoundException as-is (the facade maps it to a retriable conflict). As
          // above, hasWrites is not trustworthy mid-prepare, so only readOnly counts.
          abortAndRollbackRecords(transactionId, context.readOnly, participants);
          throw e;
        }

        try {
          for (Participant participant : toValidate) {
            participant.validateRecords(transactionId);
          }
        } catch (ValidationException e) {
          // Validate failed. Unlike the prepare-phase failures above, every prepareRecords has
          // returned by now, so hasWrites authoritatively reflects every participant's write set
          // and can prove the transaction write-less (matching CommitHandler, which gates its
          // abort on the snapshot it owns). The failure is reported as a commit-level exception,
          // mirroring the prepare phase: a conflict as CommitConflictException, anything else as
          // CommitException.
          abortAndRollbackRecords(transactionId, !hasWrites, participants);
          if (e instanceof ValidationConflictException) {
            throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
          }
          throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
        } catch (TransactionNotFoundException e) {
          // A participant no longer knows this transaction while validating. As above, hasWrites
          // is authoritative at this point.
          abortAndRollbackRecords(transactionId, !hasWrites, participants);
          throw e;
        }

        long committedAt;
        if (shouldWriteCoordinatorState(hasWrites)) {
          try {
            committedAt = commitState(transactionId, writeSetsByParticipant);
          } catch (CommitConflictException e) {
            // The COMMITTED-state write lost a putState race that resolved to ABORTED: the
            // transaction is aborted. Only participants with writes still hold PREPARED records
            // (and a live context); roll those back before surfacing it.
            for (Participant participant : toCommit) {
              bestEffortRollbackRecords(participant, transactionId);
            }
            throw e;
          }
        } else {
          // Write-less transaction with the Coordinator write omitted: no participant has writes,
          // so toCommit is empty and committedAt is an unused placeholder.
          committedAt = preparedAt;
        }
        for (Participant participant : toCommit) {
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
  public void releaseContext(String transactionId) {
    CoordinatorContext context = contexts.get(transactionId);
    if (context == null) {
      // Unknown or already-released transaction: nothing to release.
      return;
    }
    synchronized (context) {
      if (context.isReleased()) {
        // Already terminated by a concurrent commit/rollback/release; nothing to do.
        return;
      }
      // Reap-only terminal: discard this Coordinator's in-memory state only. Unlike rollback(), the
      // participants are NOT contacted (each role reaps its own context independently), and no
      // Coordinator state row is written and no record is mutated — the durable outcome is left to
      // lazy recovery, which uses the Coordinator state row if one was already written (its absence
      // is resolved as an abort).
      context.markReleased();
      releaseResources(transactionId);
    }
  }

  @Override
  public void close() {
    storage.close();
  }

  // Aborts the transaction and rolls back the PREPARED records on every participant.
  //
  // knownWriteLess reports whether the transaction provably has no writes anywhere — the static
  // readOnly flag on a prepare-phase failure, or the observed !hasWrites once every prepareRecords
  // has returned (see the call sites in commit()). When it is true and coordinator-write omission
  // on read-only is enabled, the ABORTED Coordinator state row is skipped, mirroring the
  // commit-success path's COMMITTED-row gate (and CommitHandler's abort path, which gates on the
  // snapshot it owns): no PREPARED record exists for lazy recovery to consult the row for, and
  // skipping the write also avoids surfacing a spurious UnknownTransactionStatusException from a
  // failed abortState whose outcome is in fact fully known. When the omission is disabled, the row
  // is written regardless, preserving the legacy behavior that every terminated transaction leaves
  // a Coordinator state row.
  //
  // For a transaction NOT known write-less, the ABORTED row is written unconditionally: records
  // may be left PREPARED, and writing ABORTED lets lazy recovery abort them on the next read
  // instead of leaving them locked until the transaction lifetime expires.
  private void abortAndRollbackRecords(
      String transactionId, boolean knownWriteLess, List<Participant> participants)
      throws UnknownTransactionStatusException {
    if (shouldWriteCoordinatorState(!knownWriteLess)) {
      abortState(transactionId);
    }
    for (Participant participant : participants) {
      bestEffortRollbackRecords(participant, transactionId);
    }
  }

  // The single policy behind both Coordinator state gates: the row (COMMITTED on the
  // commit-success path, ABORTED on the abort path) is written unless the transaction is provably
  // write-less and coordinator-write omission on read-only is enabled. Mirrors CommitHandler's
  // gate of the same name.
  private boolean shouldWriteCoordinatorState(boolean mayHaveWrites) {
    return mayHaveWrites || !coordinatorWriteOmissionOnReadOnlyEnabled;
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
