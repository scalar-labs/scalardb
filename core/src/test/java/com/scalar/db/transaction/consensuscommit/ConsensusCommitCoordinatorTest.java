package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommit.Participant;
import com.scalar.db.api.TwoPhaseCommit.PreparationResult;
import com.scalar.db.api.TwoPhaseCommit.WriteSetDetailLevel;
import com.scalar.db.api.TwoPhaseCommit.WriteSetEntry;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ConsensusCommitCoordinatorTest {
  private static final long ANY_COMMITTED_AT = 200L;

  @Mock private CoordinatorCommitHandler coordinatorCommitHandler;
  @Mock private ConsensusCommitConfig config;

  private ConsensusCommitCoordinator consensusCommitCoordinator;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(false);
    // The handler generates the committedAt and returns it (in production it stamps the COMMITTED
    // row with it); the orchestrator drives commitRecords with that returned value.
    when(coordinatorCommitHandler.commitState(anyString(), any())).thenReturn(ANY_COMMITTED_AT);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
  }

  @Test
  void constructor_GroupCommitEnabled_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitCoordinator(coordinatorCommitHandler, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void begin_NoTransactionIdGiven_ShouldGenerateAndReturnId() throws Exception {
    // Act
    String id = consensusCommitCoordinator.begin(null, false, Collections.emptyMap(), null);

    // Assert
    assertThat(id).isNotNull().isNotEmpty();
  }

  @Test
  void begin_TransactionIdGiven_ShouldReturnSameId() throws Exception {
    // Act
    String id = consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);

    // Assert
    assertThat(id).isEqualTo("tx-1");
  }

  @Test
  void begin_SameTransactionIdTwice_ShouldThrowTransactionException() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);

    // Act Assert
    assertThatThrownBy(
            () -> consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  void registerParticipant_UnknownTransaction_ShouldThrowTransactionNotFoundException() {
    // Act Assert
    assertThatThrownBy(
            () ->
                consensusCommitCoordinator.registerParticipant("unknown", mock(Participant.class)))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void registerParticipant_ShouldInvokeJoinAndTrackParticipant() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), null);
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn("participant-1");
    // A writer that also requires validation, so all of its record-level steps are driven.
    when(participant.prepareRecords(anyString(), anyLong(), any()))
        .thenReturn(preparation(writeSet(), true));

    // Act
    consensusCommitCoordinator.registerParticipant("tx-1", participant);

    // Assert — join is invoked with the begin-time readOnly flag and attributes ...
    verify(participant).join("tx-1", true, Collections.emptyMap());
    // ... and the participant is tracked, so a subsequent commit drives its record-level steps.
    consensusCommitCoordinator.commit("tx-1");
    verify(participant).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(participant).validateRecords("tx-1");
    verify(participant).commitRecords(eq("tx-1"), anyLong());
  }

  @Test
  void registerParticipant_WhenJoinThrows_ShouldNotTrackParticipant() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = mock(Participant.class);
    doThrow(new TransactionException("join failed", "tx-1"))
        .when(participant)
        .join("tx-1", false, Collections.emptyMap());

    // Act
    assertThatThrownBy(() -> consensusCommitCoordinator.registerParticipant("tx-1", participant))
        .isInstanceOf(TransactionException.class);

    // Assert — a failed join leaves the participant untracked, so rollback never touches it.
    consensusCommitCoordinator.rollback("tx-1");
    verify(participant, never()).rollbackRecords(anyString());
  }

  @Test
  void begin_WithParticipant_ShouldRegisterParticipant() throws Exception {
    // Arrange
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn("participant-1");
    when(participant.prepareRecords(anyString(), anyLong(), any())).thenReturn(emptyPreparation());

    // Act — passing a participant to begin registers it exactly as registerParticipant would.
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), participant);

    // Assert — join is invoked with the begin-time readOnly flag and attributes ...
    verify(participant).join("tx-1", true, Collections.emptyMap());
    // ... and the participant is tracked, so a subsequent commit drives its record-level steps.
    consensusCommitCoordinator.commit("tx-1");
    verify(participant).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
  }

  @Test
  void begin_WithParticipant_WhenJoinThrows_ShouldReleaseContextAndPropagate() throws Exception {
    // Arrange
    Participant participant = mock(Participant.class);
    doThrow(new TransactionException("join failed", "tx-1"))
        .when(participant)
        .join("tx-1", false, Collections.emptyMap());

    // Act Assert — the join failure propagates ...
    assertThatThrownBy(
            () ->
                consensusCommitCoordinator.begin(
                    "tx-1", false, Collections.emptyMap(), participant))
        .isInstanceOf(TransactionException.class);

    // ... and the context created by begin is released, so the same id can be begun again.
    assertThat(consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null))
        .isEqualTo("tx-1");
  }

  @Test
  void commit_ShouldDelegateCommitStateToCoordinatorHandler() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — the COMMITTED state write is delegated to the Coordinator-side handler with the
    // transaction id and the encoded write set (the handler generates the committedAt).
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
  }

  @Test
  void commit_WhenCommitStateConflicts_ShouldThrowCommitConflictException() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    doThrow(new CommitConflictException("conflict", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class);
  }

  @Test
  void commit_WhenCommitStateConflicts_ShouldRollBackPreparedRecordsOnEachParticipant()
      throws Exception {
    // Arrange — prepare/validate succeed on both participants, but the COMMITTED-state write loses
    // a putState race that resolves to ABORTED (CommitConflictException).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    Participant p2 = registeredWritingParticipant("tx-1", "participant-2");
    doThrow(new CommitConflictException("conflict", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert — the conflict propagates ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class);

    // ... and the PREPARED records are rolled back on every writing participant before it surfaces
    // ...
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
    // ... while commitRecords is never reached.
    verify(p1, never()).commitRecords(anyString(), anyLong());
    verify(p2, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void commit_WhenCommitStateConflicts_ShouldRollBackOnlyWritingParticipants() throws Exception {
    // Arrange — commitState loses a putState race (CommitConflictException). The writer holds
    // PREPARED records and a live context; the write-less participant already self-released at
    // prepareRecords, so the Coordinator rolls back only toCommit (the writer), not every
    // participant.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant writer = registeredWritingParticipant("tx-1", "participant-1");
    Participant writeLess = registeredParticipant("tx-1", "participant-2");
    doThrow(new CommitConflictException("conflict", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert — the conflict propagates ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class);

    // ... only the writing participant is rolled back; the self-released one is left untouched.
    verify(writer).rollbackRecords("tx-1");
    verify(writeLess, never()).rollbackRecords(anyString());
  }

  @Test
  void commit_NoWrites_WithWriteOmission_ShouldSkipCommitStateValidateAndCommitRecords()
      throws Exception {
    // Arrange — coordinator-write omission on read-only is enabled (production default). The
    // participant has no writes and requires no validation (prepareRecords returns an empty write
    // set with isValidationRequired() == false).
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — only prepareRecords is driven. The COMMITTED Coordinator state row is not written,
    // and validateRecords / commitRecords are skipped (the participant released its own context at
    // prepareRecords, its last driven step).
    verify(participant).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
    verify(participant, never()).validateRecords(anyString());
    verify(participant, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void commit_NoWrites_WithWriteOmission_WhenValidateFails_ShouldRollBackWithoutWritingAborted()
      throws Exception {
    // Arrange — omission enabled, no writes, but the participant requires validation and validation
    // fails.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(Collections.emptyList(), true));
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(participant)
        .validateRecords("tx-1");

    // Act Assert — the validate conflict surfaces as a retriable CommitConflictException. By the
    // validate phase every prepareRecords has returned, so hasWrites authoritatively proves the
    // transaction write-less: no PREPARED record exists anywhere, and the ABORTED state row is
    // omitted (it would never be consulted). Records are still rolled back to release the
    // participants' local resources.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void commit_WithWrites_WithWriteOmission_WhenValidateFails_ShouldWriteAbortedAndRollback()
      throws Exception {
    // Arrange — omission enabled, but the participant produced writes and validation fails.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(writeSet(), true));
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(participant)
        .validateRecords("tx-1");

    // Act Assert — PREPARED records exist (hasWrites is true), so the ABORTED state row is written
    // for lazy recovery even though the omission is enabled, now carrying the keys-only write set
    // (validate-phase aborts have the complete aggregate), and the records are rolled back.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    verify(coordinatorCommitHandler).abortState(eq("tx-1"), notNull());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void
      commit_NoWrites_WithWriteOmission_WhenValidateThrowsTransactionNotFound_ShouldRollBackWithoutWritingAborted()
          throws Exception {
    // Arrange — omission enabled, no writes, validation required, but the participant no longer
    // knows the transaction at validate time (e.g. its context expired between prepare and
    // validate).
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(Collections.emptyList(), true));
    doThrow(new TransactionNotFoundException("transaction not found", "tx-1"))
        .when(participant)
        .validateRecords("tx-1");

    // Act Assert — the exception is rethrown as-is (the facade maps it to a retriable conflict),
    // NOT wrapped in a CommitException. Every prepareRecords has returned, so hasWrites
    // authoritatively proves the transaction write-less and the ABORTED state row is omitted;
    // records are still rolled back to release the participants' local resources.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void
      commit_WithWrites_WithWriteOmission_WhenValidateThrowsTransactionNotFound_ShouldWriteAbortedAndRollback()
          throws Exception {
    // Arrange — omission enabled, but the participant produced writes and no longer knows the
    // transaction at validate time.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(writeSet(), true));
    doThrow(new TransactionNotFoundException("transaction not found", "tx-1"))
        .when(participant)
        .validateRecords("tx-1");

    // Act Assert — the exception is rethrown as-is, and PREPARED records exist (hasWrites is
    // true), so the ABORTED state row is written for lazy recovery even though the omission is
    // enabled, now carrying the keys-only write set (validate-phase aborts have the complete
    // aggregate); the records are rolled back.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
    verify(coordinatorCommitHandler).abortState(eq("tx-1"), notNull());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void commit_NotReadOnly_WithWriteOmission_WhenPrepareFails_ShouldWriteAbortedAndRollback()
      throws Exception {
    // Arrange — omission enabled and no writes were observed, but the failure happens during the
    // prepare phase of a non-read-only transaction.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(participant)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — mid-prepare, hasWrites is not trustworthy (the throwing participant never
    // returned its write set and may have left records PREPARED), so the ABORTED state row is
    // still written despite the omission being enabled and no writes having been observed.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void commit_ReadOnly_WithWriteOmission_WhenPrepareFails_ShouldRollBackWithoutWritingAborted()
      throws Exception {
    // Arrange — omission enabled and the transaction is read-only, so even a part-way prepare
    // failure cannot have left PREPARED records anywhere (participants reject mutations on a
    // read-only transaction).
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(participant)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — the prepare conflict still surfaces as a retriable CommitConflictException, but
    // no ABORTED state row is written (the static readOnly flag proves the transaction
    // write-less); the participants are still rolled back to release their local resources.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void
      commit_ReadOnly_WithWriteOmission_WhenPrepareThrowsTransactionNotFound_ShouldRollBackWithoutWritingAborted()
          throws Exception {
    // Arrange — omission enabled and the transaction is read-only, but the participant no longer
    // knows the transaction while preparing (e.g. its context expired).
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");
    doThrow(new TransactionNotFoundException("transaction not found", "tx-1"))
        .when(participant)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — the exception is rethrown as-is (the facade maps it to a retriable conflict),
    // NOT wrapped in a CommitException. The static readOnly flag proves the transaction
    // write-less, so no ABORTED state row is written; the participants are still rolled back to
    // release their local resources.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void commit_ReadOnly_WithoutWriteOmission_WhenPrepareFails_ShouldStillWriteAborted()
      throws Exception {
    // Arrange — read-only, but omission disabled (the setUp default): the legacy behavior where
    // every terminated transaction leaves a Coordinator state row is preserved, mirroring the
    // commit-success path's gate.
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(participant)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(participant).rollbackRecords("tx-1");
  }

  @Test
  void commit_UnknownTransaction_ShouldThrowTransactionNotFoundException() throws Exception {
    // Act Assert — committing a transaction never begun on this Coordinator is a caller error; no
    // COMMITTED state is written.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("unknown"))
        .isInstanceOf(TransactionNotFoundException.class);
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
  }

  @Test
  void rollback_ShouldDriveRollbackRecordsAndReleaseWithoutWritingAbortedState() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");

    // Act
    consensusCommitCoordinator.rollback("tx-1");

    // Assert — rollbackRecords is driven on the participant to undo its work and release its local
    // context; no ABORTED state row is written (records are only PREPARED inside commit()).
    verify(participant).rollbackRecords("tx-1");
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
  }

  @Test
  void rollback_UnknownTransaction_ShouldBeNoOp() throws Exception {
    // Act — rolling back a transaction never begun (or already finished) is a lenient no-op.
    consensusCommitCoordinator.rollback("unknown");

    // Assert — no ABORTED state is written.
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
  }

  @Test
  void releaseTransactionContext_ShouldReleaseContextWithoutDrivingParticipantsOrWritingState()
      throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredWritingParticipant("tx-1", "participant-1");

    // Act
    consensusCommitCoordinator.releaseTransactionContext("tx-1");

    // Assert — reap-only terminal: the participant is NOT contacted (role-local), no Coordinator
    // state row is written, and the context is released so a subsequent commit no longer knows the
    // transaction.
    verify(participant, never()).rollbackRecords(anyString());
    verify(participant, never()).commitRecords(anyString(), anyLong());
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void releaseTransactionContext_UnknownTransaction_ShouldBeNoOp() throws Exception {
    // Releasing a transaction never begun (or already finished) is a lenient no-op.
    consensusCommitCoordinator.releaseTransactionContext("unknown");
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
  }

  @Test
  void commit_AfterCommit_ShouldThrowTransactionNotFoundException() throws Exception {
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    consensusCommitCoordinator.commit("tx-1");

    // The context was released by the first commit; a second commit no longer knows the
    // transaction.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void registerParticipant_AfterCommit_ShouldThrowTransactionNotFoundException() throws Exception {
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    consensusCommitCoordinator.commit("tx-1");

    assertThatThrownBy(
            () -> consensusCommitCoordinator.registerParticipant("tx-1", mock(Participant.class)))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void rollback_AfterCommit_ShouldBeNoOp() throws Exception {
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    consensusCommitCoordinator.commit("tx-1");

    // Rolling back an already-committed (released) transaction is a lenient no-op, not an error.
    consensusCommitCoordinator.rollback("tx-1");
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
  }

  @Test
  void releaseTransactionContext_AfterCommit_ShouldBeNoOp() throws Exception {
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    consensusCommitCoordinator.commit("tx-1");

    // Releasing the context of an already-committed (released) transaction is a lenient no-op: the
    // context was already discarded by commit, so there is nothing to release and no abort is
    // written.
    consensusCommitCoordinator.releaseTransactionContext("tx-1");
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
  }

  @Test
  void commit_TwoParticipants_ShouldDriveFullTwoPhaseCommitInRegistrationOrder() throws Exception {
    // Arrange — both participants write and require validation, so every record-level step is
    // driven.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 =
        registeredParticipant(
            "tx-1", "participant-1", preparation(writeSet(), /* validationRequired= */ true));
    Participant p2 =
        registeredParticipant(
            "tx-1", "participant-2", preparation(writeSet(), /* validationRequired= */ true));

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — prepare and validate on each, COMMITTED state written, then commitRecords on each.
    verify(p1).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(p2).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(p1).validateRecords("tx-1");
    verify(p2).validateRecords("tx-1");
    // The committedAt the handler returns from commitState is the one passed to every participant's
    // commitRecords, so the COMMITTED state row and the records share one timestamp.
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
    verify(p1).commitRecords("tx-1", ANY_COMMITTED_AT);
    verify(p2).commitRecords("tx-1", ANY_COMMITTED_AT);
  }

  @Test
  void commit_WriteOnlyParticipant_ShouldSkipValidateRecords() throws Exception {
    // Arrange — the participant writes but does not require validation.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(writeSet(), false));

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — validateRecords is skipped; the COMMITTED state row is written and commitRecords
    // runs.
    verify(participant).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(participant, never()).validateRecords(anyString());
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
    verify(participant).commitRecords(eq("tx-1"), anyLong());
  }

  @Test
  void commit_WriteLessValidatingParticipant_ShouldSkipCommitRecords() throws Exception {
    // Arrange — the participant has no writes but requires validation.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant =
        registeredParticipant("tx-1", "participant-1", preparation(Collections.emptyList(), true));

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — validateRecords runs, but commitRecords is skipped (no writes to commit).
    verify(participant).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(participant).validateRecords("tx-1");
    verify(participant, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void commit_MixedParticipants_ShouldDriveOnlyEachParticipantsRequiredSteps() throws Exception {
    // Arrange — all three participant categories in one transaction, exercising both the
    // toValidate and toCommit list construction at once:
    //   writer:         writes, no validation        -> prepare + commitRecords, no validate
    //   writeLessVal:   no writes, requires validation -> prepare + validate, no commitRecords
    //   writeLessNoVal: no writes, no validation      -> prepare only
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant writer =
        registeredParticipant("tx-1", "participant-1", preparation(writeSet(), false));
    Participant writeLessVal =
        registeredParticipant("tx-1", "participant-2", preparation(Collections.emptyList(), true));
    Participant writeLessNoVal =
        registeredParticipant("tx-1", "participant-3", preparation(Collections.emptyList(), false));

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — each participant gets exactly the steps it still needs.
    verify(writer).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(writer, never()).validateRecords(anyString());
    verify(writer).commitRecords(eq("tx-1"), anyLong());

    verify(writeLessVal).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(writeLessVal).validateRecords("tx-1");
    verify(writeLessVal, never()).commitRecords(anyString(), anyLong());

    verify(writeLessNoVal).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(writeLessNoVal, never()).validateRecords(anyString());
    verify(writeLessNoVal, never()).commitRecords(anyString(), anyLong());

    // A writer exists, so the COMMITTED Coordinator state row is written.
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
  }

  @Test
  void commit_WhenPrepareFailsOnSecondParticipant_ShouldWriteAbortedAndDriveRollback()
      throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new PreparationConflictException("conflict on p2", "tx-1"))
        .when(p2)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — the prepare conflict surfaces as a retriable CommitConflictException ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);

    // ... ABORTED state is written and rollbackRecords is driven on both participants ...
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
    // ... and commitState / commitRecords are never reached.
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
    verify(p1, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void
      commit_WhenPrepareThrowsTransactionNotFoundOnSecondParticipant_ShouldAbortRollBackAndRethrow()
          throws Exception {
    // Arrange — the second participant no longer knows the transaction (its local context is gone).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new TransactionNotFoundException("gone on p2", "tx-1"))
        .when(p2)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — the TransactionNotFoundException surfaces as-is (not wrapped), since a missing
    // participant context means the transaction is gone; the facade maps it to a retriable
    // conflict.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);

    // ... but the eager cleanup still runs: ABORTED state is written and rollbackRecords is driven
    // on every participant ...
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
    // ... and commitState / commitRecords are never reached.
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
    verify(p1, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void commit_WhenValidateFails_ShouldWriteAbortedAndDriveRollback() throws Exception {
    // Arrange — the participant requires validation, and validation fails.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 =
        registeredParticipant("tx-1", "participant-1", preparation(Collections.emptyList(), true));
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(p1)
        .validateRecords("tx-1");

    // Act Assert — the validate conflict surfaces as a retriable CommitConflictException. The
    // ABORTED row is written (omission disabled) and now carries the keys-only write set (empty
    // here, since the transaction is write-less), mirroring the commit path.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    ArgumentCaptor<WriteSet> captor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorCommitHandler).abortState(eq("tx-1"), captor.capture());
    // A non-null but empty write set (no entry groups), not null — the write-less validate-phase
    // abort proves the transaction touched no records, mirroring the write-less commit path.
    assertThat(captor.getValue().getEntryGroupsList()).isEmpty();
    verify(p1).rollbackRecords("tx-1");
  }

  @Test
  void commit_WhenPrepareFailsWithNonConflict_ShouldWrapAsCommitExceptionAndDriveRollback()
      throws Exception {
    // Arrange — a plain (non-conflict) PreparationException, e.g. an I/O failure during prepare
    // rather than a write conflict.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new PreparationException("prepare failed", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — a non-conflict prepare failure surfaces as a non-retriable CommitException (NOT
    // a retriable CommitConflictException), and the abort path still runs.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationException.class);
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
  }

  @Test
  void commit_WhenValidateFailsWithNonConflict_ShouldWrapAsCommitExceptionAndDriveRollback()
      throws Exception {
    // Arrange — a plain (non-conflict) ValidationException. The participant must require validation
    // (no writes, validation required) so the Coordinator actually drives validateRecords on it.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 =
        registeredParticipant(
            "tx-1",
            "participant-1",
            preparation(Collections.emptyList(), /* validationRequired= */ true));
    doThrow(new ValidationException("validation failed", "tx-1")).when(p1).validateRecords("tx-1");

    // Act Assert — a non-conflict validate failure surfaces as a non-retriable CommitException, and
    // the abort path still runs, writing the ABORTED row with the keys-only write set (empty here,
    // since the transaction is write-less).
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationException.class);
    ArgumentCaptor<WriteSet> captor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorCommitHandler).abortState(eq("tx-1"), captor.capture());
    // A non-null but empty write set (no entry groups), not null — the write-less validate-phase
    // abort proves the transaction touched no records, mirroring the write-less commit path.
    assertThat(captor.getValue().getEntryGroupsList()).isEmpty();
    verify(p1).rollbackRecords("tx-1");
  }

  @Test
  void commit_WhenCommitRecordsThrows_ShouldSwallowAndComplete() throws Exception {
    // Arrange — a writing participant, so the Coordinator drives commitRecords on it (a write-less
    // participant would be skipped and the doThrow stub below would never fire).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    doThrow(new RuntimeException("network blip")).when(p1).commitRecords(eq("tx-1"), anyLong());

    // Act Assert — commitRecords is best-effort: the failure is swallowed and commit completes.
    consensusCommitCoordinator.commit("tx-1");
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
  }

  @Test
  void commit_WhenRollbackRecordsThrows_ShouldSwallowAndPropagateOriginal() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    doThrow(new RuntimeException("network blip")).when(p1).rollbackRecords("tx-1");

    // Act Assert — rollbackRecords failure is swallowed; the prepare conflict still surfaces (as a
    // CommitConflictException).
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);
  }

  @Test
  void commit_TwoParticipants_WhenCommitRecordsThrowsOnFirst_ShouldStillCommitSecond()
      throws Exception {
    // Arrange — both participants write (so commitRecords is driven on each) and commit
    // successfully through prepare/commitState; the first participant's best-effort commitRecords
    // then fails.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    Participant p2 = registeredWritingParticipant("tx-1", "participant-2");
    doThrow(new RuntimeException("network blip")).when(p1).commitRecords(eq("tx-1"), anyLong());

    // Act — commitRecords is best-effort, so the first participant's failure is swallowed.
    consensusCommitCoordinator.commit("tx-1");

    // Assert — the second participant's commitRecords is still driven despite the first failing.
    verify(p1).commitRecords(eq("tx-1"), anyLong());
    verify(p2).commitRecords(eq("tx-1"), anyLong());
  }

  @Test
  void commit_TwoParticipants_WhenRollbackRecordsThrowsOnFirst_ShouldStillRollBackSecond()
      throws Exception {
    // Arrange — drive the abort path via a validate conflict on the second participant (which
    // therefore must require validation), then make the first participant's best-effort
    // rollbackRecords fail.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 =
        registeredParticipant("tx-1", "participant-2", preparation(Collections.emptyList(), true));
    doThrow(new ValidationConflictException("conflict", "tx-1")).when(p2).validateRecords("tx-1");
    doThrow(new RuntimeException("network blip")).when(p1).rollbackRecords("tx-1");

    // Act Assert — the validate conflict still surfaces as a retriable CommitConflictException ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class);

    // ... and the second participant's rollbackRecords is still driven despite the first failing.
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
  }

  @Test
  void rollback_WhenParticipantReportsNotFound_ShouldTreatAsBenignNoOpAndRollBackOthers()
      throws Exception {
    // Arrange — the first participant reports the unknown transaction on its conventional
    // not-found channel (typical for a remote participant that already self-released or was
    // idle-reaped), which the contract defines as an alternative carrier of the no-op outcome.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new TransactionNotFoundException("gone", "tx-1")).when(p1).rollbackRecords("tx-1");

    // Act — the rollback completes normally; the not-found is treated exactly like a no-op return.
    consensusCommitCoordinator.rollback("tx-1");

    // Assert — every participant is still driven.
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
  }

  @Test
  void commit_WhenAbortStateWriteFailsUnknown_ShouldPropagateUnknownStatusAndNotRollBack()
      throws Exception {
    // Arrange — prepare fails, and writing the ABORTED state genuinely fails with unknown status
    // (a real coordinator failure, not a mere putState conflict, which the handler would resolve to
    // ABORTED).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .abortState(anyString(), any());

    // Act Assert — the ABORTED write's unknown status surfaces (not the retryable prepare
    // conflict), and records are NOT rolled back: the Coordinator state is the source of truth, so
    // lazy recovery reconciles later. Mirrors CommitHandler#abortStateAndRollbackRecordsIfNeeded.
    // The participant's in-memory context, however, is released: commit is terminal on every
    // outcome, so nothing can ever drive the participant again.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p1, never()).rollbackRecords(anyString());
    verify(p1).releaseTransactionContext("tx-1");
  }

  @Test
  void commit_TwoParticipants_WhenAbortStateWriteFailsUnknown_ShouldReleaseBothContexts()
      throws Exception {
    // Arrange — the first participant's prepare fails, driving the internal abort path, and the
    // ABORTED-state write fails with unknown status. Both participants are registered, so the
    // abort path holds a context for each.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .abortState(anyString(), any());

    // Act Assert — the unknown status surfaces, records are not rolled back on either participant,
    // and the abort path releases every registered participant's context (unlike the commit-state
    // path's toCommit scoping, a participant that never prepared still holds an ACTIVE context
    // here).
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p1, never()).rollbackRecords(anyString());
    verify(p2, never()).rollbackRecords(anyString());
    verify(p1).releaseTransactionContext("tx-1");
    verify(p2).releaseTransactionContext("tx-1");
  }

  @Test
  void
      commit_TwoParticipants_WhenAbortStateWriteFailsUnknownAndFirstReleaseFails_ShouldStillReleaseSecond()
          throws Exception {
    // Arrange — the internal abort path with an unknown-status ABORTED write, and the first
    // participant's release itself fails.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .abortState(anyString(), any());
    doThrow(new TransactionException("unreachable", "tx-1"))
        .when(p1)
        .releaseTransactionContext("tx-1");

    // Act Assert — the release is best-effort per participant on this path too: the first failure
    // neither masks the unknown status nor stops the second participant's release, and the records
    // are still not rolled back on either participant.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p2).releaseTransactionContext("tx-1");
    verify(p1, never()).rollbackRecords(anyString());
    verify(p2, never()).rollbackRecords(anyString());
  }

  @Test
  void commit_WhenCommitStateWriteFailsUnknown_ShouldPropagateUnknownStatusAndReleaseContexts()
      throws Exception {
    // Arrange — a writing participant, and the COMMITTED state write fails with unknown status.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert — the unknown status surfaces, and the records are neither committed nor rolled
    // back: they stay PREPARED for lazy recovery to resolve against the Coordinator state row
    // (COMMITTED row -> roll-forward; no row -> abort after the transaction lifetime). The
    // participant's in-memory context is released promptly: commit is terminal on every outcome,
    // so nothing can ever drive the participant again, and releaseTransactionContext touches no
    // storage.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p1, never()).commitRecords(anyString(), anyLong());
    verify(p1, never()).rollbackRecords(anyString());
    verify(p1).releaseTransactionContext("tx-1");

    // The Coordinator context is released too (commit is terminal): a retried commit is rejected
    // as unknown.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void
      commit_WhenCommitStateWriteFailsUnknownAndReleaseTransactionContextFails_ShouldStillPropagateUnknown()
          throws Exception {
    // Arrange — the participant's releaseTransactionContext itself fails (e.g. an unreachable
    // remote participant, which is likely correlated with the coordinator-table trouble that
    // caused the unknown status in the first place).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());
    doThrow(new TransactionException("unreachable", "tx-1"))
        .when(p1)
        .releaseTransactionContext("tx-1");

    // Act Assert — the release is best-effort: its failure must not mask the unknown status (the
    // participant's context is then reclaimed by its own idle reaping).
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  @Test
  void commit_WhenCommitStateWriteFailsUnknown_ShouldReleaseOnlyWritingParticipants()
      throws Exception {
    // Arrange — a writer and a write-less participant; the COMMITTED-state write fails with
    // unknown status. The write-less participant already self-released at prepareRecords, so only
    // toCommit (the writer) still holds a context.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant writer = registeredWritingParticipant("tx-1", "participant-1");
    Participant writeLess = registeredParticipant("tx-1", "participant-2");
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert — the unknown status surfaces, and only the writing participant is released; the
    // self-released one is left untouched (mirroring the CommitConflictException path's rollback
    // scoping).
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(writer).releaseTransactionContext("tx-1");
    verify(writeLess, never()).releaseTransactionContext(anyString());
  }

  @Test
  void
      commit_TwoParticipants_WhenCommitStateWriteFailsUnknownAndFirstReleaseFails_ShouldStillReleaseSecond()
          throws Exception {
    // Arrange — two writing participants; the COMMITTED-state write fails with unknown status and
    // the first participant's release itself fails.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredWritingParticipant("tx-1", "participant-1");
    Participant p2 = registeredWritingParticipant("tx-1", "participant-2");
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());
    doThrow(new TransactionException("unreachable", "tx-1"))
        .when(p1)
        .releaseTransactionContext("tx-1");

    // Act Assert — the release is best-effort per participant: the first failure neither masks the
    // unknown status nor stops the second participant's release.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p2).releaseTransactionContext("tx-1");
    verify(p1, never()).commitRecords(anyString(), anyLong());
    verify(p2, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void registerParticipant_WhenDuplicateParticipantId_ShouldBeNoOp() throws Exception {
    // Arrange — one participant with a given ID is already registered.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant original = registeredParticipant("tx-1", "participant-1");
    Participant duplicate = mock(Participant.class);
    when(duplicate.getId()).thenReturn("participant-1");

    // Act — registering a second participant that returns the same getId() is a no-op.
    consensusCommitCoordinator.registerParticipant("tx-1", duplicate);

    // Assert — the duplicate is never joined, and only the original participant is tracked (a
    // subsequent commit drives the original, not the duplicate).
    verify(duplicate, never()).join(anyString(), anyBoolean(), anyMap());
    consensusCommitCoordinator.commit("tx-1");
    verify(original).prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));
    verify(duplicate, never()).prepareRecords(anyString(), anyLong(), any());
  }

  @Test
  void
      commit_TwoParticipantsWithWrites_WithWriteOmission_ShouldEncodeWriteSetGroupedByParticipantId()
          throws Exception {
    // Arrange — coordinator-write omission on read-only is enabled, but both participants return a
    // non-empty write set (hasWrites=true), so the COMMITTED state IS written and the write sets
    // are aggregated per participant and stamped with the owning participant id.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    registeredParticipantWithWrites(
        "tx-1",
        "participant-1",
        writeSetEntry(WriteSetEntry.Type.WRITE, Key.ofInt("pk", 1), Optional.empty()),
        writeSetEntry(WriteSetEntry.Type.DELETE, Key.ofInt("pk", 2), Optional.empty()));
    registeredParticipantWithWrites(
        "tx-1",
        "participant-2",
        writeSetEntry(WriteSetEntry.Type.WRITE, Key.ofInt("pk", 3), Optional.empty()));

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — the COMMITTED state is written (hasWrites=true overrides write omission) with a
    // WriteSet that has one EntryGroup per participant, in registration order, each entry stamped
    // with the owning participant id and the right entry type.
    ArgumentCaptor<WriteSet> captor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), captor.capture());
    WriteSet writeSet = captor.getValue();
    assertThat(writeSet.getEntryGroupsList()).hasSize(2);

    EntryGroup group1 = writeSet.getEntryGroups(0);
    assertThat(group1.getEntriesList()).hasSize(2);
    assertThat(group1.getEntries(0).getParticipantId()).isEqualTo("participant-1");
    assertThat(group1.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
    assertThat(group1.getEntries(1).getParticipantId()).isEqualTo("participant-1");
    assertThat(group1.getEntries(1).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_DELETE);

    EntryGroup group2 = writeSet.getEntryGroups(1);
    assertThat(group2.getEntriesList()).hasSize(1);
    assertThat(group2.getEntries(0).getParticipantId()).isEqualTo("participant-2");
    assertThat(group2.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
  }

  @Test
  void commit_TwoParticipantsWithWrites_WhenValidateFails_ShouldWriteAbortedWithAggregatedWriteSet()
      throws Exception {
    // Arrange — two writing participants that both require validation; the second fails validation,
    // driving the validate-phase abort. By then every prepareRecords has returned, so the
    // aggregated write set is complete — the same map the commit path would persist.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 =
        registeredParticipant(
            "tx-1",
            "participant-1",
            preparation(
                Arrays.asList(
                    writeSetEntry(WriteSetEntry.Type.WRITE, Key.ofInt("pk", 1), Optional.empty()),
                    writeSetEntry(WriteSetEntry.Type.DELETE, Key.ofInt("pk", 2), Optional.empty())),
                /* validationRequired= */ true));
    Participant p2 =
        registeredParticipant(
            "tx-1",
            "participant-2",
            preparation(
                Arrays.asList(
                    writeSetEntry(WriteSetEntry.Type.WRITE, Key.ofInt("pk", 3), Optional.empty())),
                /* validationRequired= */ true));
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(p2)
        .validateRecords("tx-1");

    // Act Assert — the ABORTED row carries the same participant-grouped, keys-only write set the
    // commit path encodes (one EntryGroup per participant in registration order), so active
    // recovery can find the touched records.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    ArgumentCaptor<WriteSet> captor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorCommitHandler).abortState(eq("tx-1"), captor.capture());
    WriteSet writeSet = captor.getValue();
    assertThat(writeSet.getEntryGroupsList()).hasSize(2);

    EntryGroup group1 = writeSet.getEntryGroups(0);
    assertThat(group1.getEntriesList()).hasSize(2);
    assertThat(group1.getEntries(0).getParticipantId()).isEqualTo("participant-1");
    assertThat(group1.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
    assertThat(group1.getEntries(1).getParticipantId()).isEqualTo("participant-1");
    assertThat(group1.getEntries(1).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_DELETE);

    EntryGroup group2 = writeSet.getEntryGroups(1);
    assertThat(group2.getEntriesList()).hasSize(1);
    assertThat(group2.getEntries(0).getParticipantId()).isEqualTo("participant-2");
    assertThat(group2.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);

    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
  }

  @Test
  void commit_WithWrites_WithWriteOmission_WhenPrepareFails_ShouldWriteAbortedState()
      throws Exception {
    // Arrange — write omission enabled, the first participant prepares a non-empty write set and
    // the second participant then fails to prepare, leaving p1's records PREPARED.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 =
        registeredParticipantWithWrites(
            "tx-1",
            "participant-1",
            writeSetEntry(WriteSetEntry.Type.WRITE, Key.ofInt("pk", 1), Optional.empty()));
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new PreparationConflictException("conflict on p2", "tx-1"))
        .when(p2)
        .prepareRecords(eq("tx-1"), anyLong(), eq(WriteSetDetailLevel.KEYS_ONLY));

    // Act Assert — the prepare conflict surfaces as a retriable CommitConflictException ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);

    // ... and the ABORTED state row IS written: on a prepare-phase failure hasWrites is not
    // trustworthy, so only the static readOnly flag can justify the omission, and this
    // transaction is not read-only — unlike
    // commit_ReadOnly_WithWriteOmission_WhenPrepareFails_ShouldRollBackWithoutWritingAborted,
    // where the row is omitted. Both participants are rolled back, undoing p1's PREPARED records.
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
  }

  private Participant registeredParticipant(String transactionId) throws Exception {
    return registeredParticipant(transactionId, "participant-1");
  }

  private Participant registeredParticipant(String transactionId, String participantId)
      throws Exception {
    return registeredParticipant(transactionId, participantId, emptyPreparation());
  }

  private Participant registeredParticipant(
      String transactionId, String participantId, PreparationResult preparationResult)
      throws Exception {
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn(participantId);
    when(participant.prepareRecords(anyString(), anyLong(), any())).thenReturn(preparationResult);
    consensusCommitCoordinator.registerParticipant(transactionId, participant);
    return participant;
  }

  // A registered participant that produces a write (so the Coordinator drives commitRecords on it).
  private Participant registeredWritingParticipant(String transactionId, String participantId)
      throws Exception {
    return registeredParticipant(transactionId, participantId, writePreparation());
  }

  private static PreparationResult preparation(
      List<WriteSetEntry> writeSet, boolean validationRequired) {
    return new PreparationResult() {
      @Override
      public List<WriteSetEntry> getWriteSet() {
        return writeSet;
      }

      @Override
      public boolean isValidationRequired() {
        return validationRequired;
      }

      @Override
      public boolean isCommitRequired() {
        // Mirrors ConsensusCommitParticipant: committing is required iff there are PREPARED
        // records.
        return !writeSet.isEmpty();
      }
    };
  }

  // A PreparationResult carrying no writes and not requiring validation.
  private static PreparationResult emptyPreparation() {
    return preparation(Collections.emptyList(), false);
  }

  // A PreparationResult carrying a single write and not requiring validation.
  private static PreparationResult writePreparation() {
    return preparation(writeSet(), false);
  }

  // A non-empty write set (one entry) with valid fields so the Coordinator can encode it for the
  // COMMITTED state row. Enough to make the participant count as a writer.
  private static List<WriteSetEntry> writeSet() {
    return Collections.singletonList(
        new WriteSetEntry() {
          @Override
          public Type getType() {
            return Type.WRITE;
          }

          @Override
          public String getNamespaceName() {
            return "ns";
          }

          @Override
          public String getTableName() {
            return "tbl";
          }

          @Override
          public Key getPartitionKey() {
            return Key.ofInt("pk", 1);
          }

          @Override
          public Optional<Key> getClusteringKey() {
            return Optional.empty();
          }

          @Override
          public List<Column<?>> getColumns() {
            return Collections.emptyList();
          }
        });
  }

  // Registers a participant whose prepareRecords returns the given (non-empty) write set, so
  // commit() exercises the hasWrites=true aggregation/encoding path.
  private Participant registeredParticipantWithWrites(
      String transactionId, String participantId, WriteSetEntry... entries) throws Exception {
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn(participantId);
    List<WriteSetEntry> writeSet = Arrays.asList(entries);
    // Non-empty write set, so isCommitRequired() is true (the Coordinator drives commitRecords and
    // aggregates the write set); validation is not required for these encoding-focused tests.
    when(participant.prepareRecords(anyString(), anyLong(), any()))
        .thenReturn(preparation(writeSet, /* validationRequired= */ false));
    consensusCommitCoordinator.registerParticipant(transactionId, participant);
    return participant;
  }

  // A minimal WriteSetEntry stub sufficient for encoding (includeColumns=false, so getColumns is
  // not read). The namespace/table are stubbed non-null because the encoder sets them on the proto.
  private static WriteSetEntry writeSetEntry(
      WriteSetEntry.Type type, Key partitionKey, Optional<Key> clusteringKey) {
    WriteSetEntry entry = mock(WriteSetEntry.class);
    when(entry.getType()).thenReturn(type);
    when(entry.getNamespaceName()).thenReturn("ns");
    when(entry.getTableName()).thenReturn("tbl");
    when(entry.getPartitionKey()).thenReturn(partitionKey);
    when(entry.getClusteringKey()).thenReturn(clusteringKey);
    return entry;
  }
}
