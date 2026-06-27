package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommit.Participant;
import com.scalar.db.api.TwoPhaseCommit.PreparationResult;
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
    when(participant.prepareRecords(anyString(), anyLong())).thenReturn(emptyPreparation());

    // Act
    consensusCommitCoordinator.registerParticipant("tx-1", participant);

    // Assert — join is invoked with the begin-time readOnly flag and attributes ...
    verify(participant).join("tx-1", true, Collections.emptyMap());
    // ... and the participant is tracked, so a subsequent commit drives its record-level steps.
    consensusCommitCoordinator.commit("tx-1");
    verify(participant).prepareRecords(eq("tx-1"), anyLong());
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
    when(participant.prepareRecords(anyString(), anyLong())).thenReturn(emptyPreparation());

    // Act — passing a participant to begin registers it exactly as registerParticipant would.
    consensusCommitCoordinator.begin("tx-1", true, Collections.emptyMap(), participant);

    // Assert — join is invoked with the begin-time readOnly flag and attributes ...
    verify(participant).join("tx-1", true, Collections.emptyMap());
    // ... and the participant is tracked, so a subsequent commit drives its record-level steps.
    consensusCommitCoordinator.commit("tx-1");
    verify(participant).prepareRecords(eq("tx-1"), anyLong());
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
    // a
    // putState race that resolves to ABORTED (CommitConflictException).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
    doThrow(new CommitConflictException("conflict", "tx-1"))
        .when(coordinatorCommitHandler)
        .commitState(anyString(), any());

    // Act Assert — the conflict propagates ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class);

    // ... and the PREPARED records are rolled back on every participant before it surfaces ...
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
    // ... while commitRecords is never reached.
    verify(p1, never()).commitRecords(anyString(), anyLong());
    verify(p2, never()).commitRecords(anyString(), anyLong());
  }

  @Test
  void commit_NoWrites_WithWriteOmission_ShouldSkipCommitStateButStillDriveRecords()
      throws Exception {
    // Arrange — coordinator-write omission on read-only is enabled (the production default), and
    // the
    // participant has no writes (prepareRecords returns an empty write set).
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — with no writes, the COMMITTED Coordinator state row is not written ...
    verify(coordinatorCommitHandler, never()).commitState(anyString(), any());
    // ... but the record-level steps are still driven so the participant context is released.
    verify(participant).prepareRecords(eq("tx-1"), anyLong());
    verify(participant).validateRecords("tx-1");
    verify(participant).commitRecords(eq("tx-1"), anyLong());
  }

  @Test
  void commit_NoWrites_WithWriteOmission_WhenValidateFails_ShouldSkipAbortStateButRollback()
      throws Exception {
    // Arrange — omission enabled, no writes, but validation fails.
    when(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).thenReturn(true);
    consensusCommitCoordinator = new ConsensusCommitCoordinator(coordinatorCommitHandler, config);
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant participant = registeredParticipant("tx-1");
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(participant)
        .validateRecords("tx-1");

    // Act Assert — the validate conflict surfaces as a retriable CommitConflictException, no
    // ABORTED
    // state row is written (an absent row is treated as ABORTED), but the participant records are
    // still rolled back.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    verify(coordinatorCommitHandler, never()).abortState(anyString(), any());
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
  void commit_TwoParticipants_ShouldDriveFullTwoPhaseCommitInRegistrationOrder() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");

    // Act
    consensusCommitCoordinator.commit("tx-1");

    // Assert — prepare and validate on each, COMMITTED state written, then commitRecords on each.
    verify(p1).prepareRecords(eq("tx-1"), anyLong());
    verify(p2).prepareRecords(eq("tx-1"), anyLong());
    verify(p1).validateRecords("tx-1");
    verify(p2).validateRecords("tx-1");
    // The committedAt the handler returns from commitState is the one passed to every participant's
    // commitRecords, so the COMMITTED state row and the records share one timestamp.
    verify(coordinatorCommitHandler).commitState(eq("tx-1"), any());
    verify(p1).commitRecords("tx-1", ANY_COMMITTED_AT);
    verify(p2).commitRecords("tx-1", ANY_COMMITTED_AT);
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
        .prepareRecords(eq("tx-1"), anyLong());

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
        .prepareRecords(eq("tx-1"), anyLong());

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
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new ValidationConflictException("validation conflict", "tx-1"))
        .when(p1)
        .validateRecords("tx-1");

    // Act Assert — the validate conflict surfaces as a retriable CommitConflictException.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationConflictException.class);
    verify(coordinatorCommitHandler).abortState("tx-1", null);
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
        .prepareRecords(eq("tx-1"), anyLong());

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
    // Arrange — a plain (non-conflict) ValidationException.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new ValidationException("validation failed", "tx-1")).when(p1).validateRecords("tx-1");

    // Act Assert — a non-conflict validate failure surfaces as a non-retriable CommitException, and
    // the abort path still runs.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(ValidationException.class);
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
  }

  @Test
  void commit_WhenCommitRecordsThrows_ShouldSwallowAndComplete() throws Exception {
    // Arrange
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
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
        .prepareRecords(eq("tx-1"), anyLong());
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
    // Arrange — both participants commit successfully through prepare/validate/commitState; the
    // first participant's best-effort commitRecords then fails.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
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
    // Arrange — drive the abort path via a validate conflict on the second participant, then make
    // the first participant's best-effort rollbackRecords fail.
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1", "participant-1");
    Participant p2 = registeredParticipant("tx-1", "participant-2");
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
  void commit_WhenAbortStateWriteFailsUnknown_ShouldPropagateUnknownStatusAndNotRollBack()
      throws Exception {
    // Arrange — prepare fails, and writing the ABORTED state genuinely fails with unknown status
    // (a real coordinator failure, not a mere putState conflict, which the handler would resolve to
    // ABORTED).
    consensusCommitCoordinator.begin("tx-1", false, Collections.emptyMap(), null);
    Participant p1 = registeredParticipant("tx-1");
    doThrow(new PreparationConflictException("conflict", "tx-1"))
        .when(p1)
        .prepareRecords(eq("tx-1"), anyLong());
    doThrow(new UnknownTransactionStatusException("unknown", "tx-1"))
        .when(coordinatorCommitHandler)
        .abortState(anyString(), any());

    // Act Assert — the ABORTED write's unknown status surfaces (not the retryable prepare
    // conflict), and records are NOT rolled back: the Coordinator state is the source of truth, so
    // lazy recovery reconciles later. Mirrors CommitHandler#abortStateAndRollbackRecordsIfNeeded.
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(p1, never()).rollbackRecords(anyString());
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
    verify(original).prepareRecords(eq("tx-1"), anyLong());
    verify(duplicate, never()).prepareRecords(anyString(), anyLong());
  }

  @Test
  void
      commit_TwoParticipantsWithWrites_WithWriteOmission_ShouldEncodeWriteSetGroupedByParticipantId()
          throws Exception {
    // Arrange — coordinator-write omission on read-only is enabled, but both participants return a
    // non-empty write set (hasWrites=true), so the COMMITTED state IS written and the
    // per-participant
    // write sets are aggregated and stamped with the owning participant id.
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
  void commit_WithWrites_WithWriteOmission_WhenPrepareFails_ShouldWriteAbortedState()
      throws Exception {
    // Arrange — write omission enabled, the first participant prepares a non-empty write set
    // (hasWrites becomes true) and the second participant then fails to prepare.
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
        .prepareRecords(eq("tx-1"), anyLong());

    // Act Assert — the prepare conflict surfaces as a retriable CommitConflictException ...
    assertThatThrownBy(() -> consensusCommitCoordinator.commit("tx-1"))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(PreparationConflictException.class);

    // ... and because hasWrites is true (p1 prepared a non-empty write set before p2 failed), the
    // ABORTED state row IS written — unlike the write-less case
    // (commit_NoWrites_WithWriteOmission_WhenValidateFails) where it is omitted.
    verify(coordinatorCommitHandler).abortState("tx-1", null);
    verify(p1).rollbackRecords("tx-1");
    verify(p2).rollbackRecords("tx-1");
  }

  private Participant registeredParticipant(String transactionId) throws Exception {
    return registeredParticipant(transactionId, "participant-1");
  }

  private Participant registeredParticipant(String transactionId, String participantId)
      throws Exception {
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn(participantId);
    when(participant.prepareRecords(anyString(), anyLong())).thenReturn(emptyPreparation());
    consensusCommitCoordinator.registerParticipant(transactionId, participant);
    return participant;
  }

  // A PreparationResult carrying no writes. PreparationResult has a single method (getWriteSet), so
  // a method reference to an empty list satisfies it.
  private static PreparationResult emptyPreparation() {
    return Collections::emptyList;
  }

  // Registers a participant whose prepareRecords returns the given (non-empty) write set, so
  // commit()
  // exercises the hasWrites=true aggregation/encoding path.
  private Participant registeredParticipantWithWrites(
      String transactionId, String participantId, WriteSetEntry... entries) throws Exception {
    Participant participant = mock(Participant.class);
    when(participant.getId()).thenReturn(participantId);
    List<WriteSetEntry> writeSet = Arrays.asList(entries);
    when(participant.prepareRecords(anyString(), anyLong())).thenReturn(() -> writeSet);
    consensusCommitCoordinator.registerParticipant(transactionId, participant);
    return participant;
  }

  // A minimal WriteSetEntry stub sufficient for encoding (includeColumns=false, so getColumns is
  // not
  // read). The namespace/table are stubbed non-null because the encoder sets them on the proto.
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
