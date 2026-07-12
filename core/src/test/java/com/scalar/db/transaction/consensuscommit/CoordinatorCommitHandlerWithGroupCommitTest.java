package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import com.scalar.db.util.groupcommit.GroupCommitConflictException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Direct unit tests for {@link CoordinatorCommitHandlerWithGroupCommit} and its inner {@link
 * CoordinatorCommitHandlerWithGroupCommit.Emitter}. These tests pass ids and pre-encoded write sets
 * directly. Tests for inherited methods like {@code forceAbortState} live in {@link
 * CoordinatorCommitHandlerTest}.
 */
class CoordinatorCommitHandlerWithGroupCommitTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;

  @Mock private CoordinatorStateAccessor coordinator;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ConsensusCommitConfig config;

  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  // Builds the pre-encoded write sets the tests feed to the handler.
  private WriteSetEncoder writeSetEncoder;
  private String fullId;
  private CoordinatorGroupCommitter groupCommitter;
  private CoordinatorCommitHandlerWithGroupCommit handler;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    writeSetEncoder = new WriteSetEncoder(tableMetadataManager);
    groupCommitter =
        spy(new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 60000, 10)));
    // The handler's constructor wires the Emitter into the groupCommitter via setEmitter(); the
    // emitter must be in place before reserve() is called so that the slot can be emitted later.
    handler = new CoordinatorCommitHandlerWithGroupCommit(coordinator, groupCommitter, true);
    fullId = groupCommitter.reserve(UUID.randomUUID().toString());
  }

  @AfterEach
  void tearDown() {
    // Release any outstanding reserved slot so groupCommitter.close() does not block on the
    // slot-abort timeout.
    try {
      groupCommitter.remove(fullId);
    } catch (Exception ignored) {
      // Slot already removed by the test; safe.
    }
    groupCommitter.close();
  }

  private Put preparePut() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Snapshot prepareSnapshotWithWrite(String id) throws CrudException {
    Snapshot snapshot = new Snapshot(id, tableMetadataManager, new ParallelExecutor(config));
    Put put = preparePut();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    return snapshot;
  }

  private Snapshot prepareEmptySnapshot(String id) {
    return new Snapshot(id, tableMetadataManager, new ParallelExecutor(config));
  }

  private TransactionContext createContext(String id, Snapshot snapshot, boolean slotReserved) {
    return new TransactionContext(id, snapshot, Isolation.SNAPSHOT, false, false, slotReserved);
  }

  // Builds the group-commit value a caller would buffer: the full id + its pre-encoded write set.
  private CoordinatorGroupCommitValue valueWithWrite(String id) throws CrudException {
    Snapshot snapshot = prepareSnapshotWithWrite(id);
    WriteSet writeSet =
        writeSetEncoder.encodeSingleGroupWriteSet(createContext(id, snapshot, true), false);
    return new CoordinatorGroupCommitValue(id, writeSet);
  }

  private CoordinatorGroupCommitValue valueReadOnly(String id) {
    Snapshot snapshot = prepareEmptySnapshot(id);
    WriteSet writeSet =
        writeSetEncoder.encodeSingleGroupWriteSet(createContext(id, snapshot, true), false);
    return new CoordinatorGroupCommitValue(id, writeSet);
  }

  // ---------- commitState (group-commit path) ----------

  @Test
  void commitState_WhenSuccessful_ShouldRouteThroughGroupCommitterAndReturnEmitCommittedAt()
      throws Exception {
    // Arrange
    long emitCommittedAt = 1234567890123L;
    WriteSet writeSet =
        writeSetEncoder.encodeSingleGroupWriteSet(
            createContext(fullId, prepareSnapshotWithWrite(fullId), true), false);
    // groupCommitter is a spy, so stub with doReturn to avoid invoking the real ready().
    doReturn(emitCommittedAt)
        .when(groupCommitter)
        .ready(eq(fullId), any(CoordinatorGroupCommitValue.class));

    // Act
    long committedAt = handler.commitState(fullId, writeSet);

    // Assert — the group-committer.ready() is the dispatch boundary into the emitter; the
    // (id, writeSet) it is handed is wrapped verbatim into the buffered carrier, and the
    // emit-time committedAt it returns is propagated so the caller stamps the records with it.
    ArgumentCaptor<CoordinatorGroupCommitValue> valueCaptor =
        ArgumentCaptor.forClass(CoordinatorGroupCommitValue.class);
    verify(groupCommitter).ready(eq(fullId), valueCaptor.capture());
    assertThat(valueCaptor.getValue().fullId).isEqualTo(fullId);
    assertThat(valueCaptor.getValue().writeSet).isEqualTo(writeSet);
    assertThat(committedAt).isEqualTo(emitCommittedAt);
  }

  @Test
  void commitState_WhenGroupCommitConflict_ShouldCancelSlotAndHandleConflict() throws Exception {
    // Arrange
    GroupCommitConflictException cause = new GroupCommitConflictException("conflict");
    doThrow(cause).when(groupCommitter).ready(eq(fullId), any(CoordinatorGroupCommitValue.class));
    when(coordinator.getState(anyString())).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(fullId, null))
        .isInstanceOf(CommitConflictException.class);

    // Slot is released on the conflict path. Record rollback is the orchestrator's responsibility
    // (see CommitHandlerTest), not this Coordinator-side handler's.
    verify(groupCommitter).remove(fullId);
  }

  @Test
  void
      commitState_WhenGroupCommitExceptionWithCoordinatorConflictCause_ShouldCancelSlotAndHandleConflict()
          throws Exception {
    // A plain GroupCommitException (not GroupCommitConflictException) whose cause is a
    // CoordinatorConflictException is routed to handleCommitConflict. With the coordinator state
    // absent on re-read, that resolves to a definitive conflict (CommitConflictException).
    // Arrange
    CoordinatorConflictException cause = new CoordinatorConflictException("coordinator conflict");
    GroupCommitException groupCommitException =
        new GroupCommitException("group commit failed", cause);
    doThrow(groupCommitException)
        .when(groupCommitter)
        .ready(eq(fullId), any(CoordinatorGroupCommitValue.class));
    when(coordinator.getState(anyString())).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(fullId, null))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);

    // The slot is released before the conflict is handled.
    verify(groupCommitter).remove(fullId);
  }

  @Test
  void commitState_WhenGroupCommitExceptionWithNonConflictCause_ShouldCancelSlotAndThrowUnknown()
      throws Exception {
    // A plain GroupCommitException whose cause is not a CoordinatorConflictException means the
    // group committer failed to access the coordinator state, so the transaction status is unknown.
    // Arrange
    RuntimeException cause = new RuntimeException("storage unavailable");
    GroupCommitException groupCommitException =
        new GroupCommitException("group commit failed", cause);
    doThrow(groupCommitException)
        .when(groupCommitter)
        .ready(eq(fullId), any(CoordinatorGroupCommitValue.class));

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(fullId, null))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining("The coordinator status is unknown")
        .hasCause(cause);

    // The slot is released even on the unknown-status path.
    verify(groupCommitter).remove(fullId);
    // The state is unknown, so the coordinator state is never re-read to resolve a conflict.
    verify(coordinator, never()).getState(anyString());
  }

  // ---------- cancelGroupCommit (slot release; driven by the orchestrator) ----------

  @Test
  void cancelGroupCommit_ShouldRemoveSlot() {
    // Act
    handler.cancelGroupCommit(fullId);

    // Assert
    verify(groupCommitter).remove(fullId);
  }

  // ---------- handleCommitConflict (group-commit specific) ----------

  @Test
  void
      handleCommitConflict_GroupCommitConflictExceptionGivenAndNoStatePersisted_ShouldThrowCommitConflict()
          throws Exception {
    // An absent coordinator state on re-read resolves to a definitive CommitConflictException,
    // because getState for a group-commit child checks both the parent and full-ID rows.
    // Arrange
    when(coordinator.getState(fullId)).thenReturn(Optional.empty());
    GroupCommitConflictException cause = new GroupCommitConflictException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(fullId, cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(coordinator).getState(fullId);
  }

  // ---------- Emitter ----------

  @Test
  void emitter_emitNormalGroup_WithMultipleWritingChildren_ShouldAggregatePerChild()
      throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId1 = keyManipulator.fullKey(parentId, "child-1");
    String fullTxId2 = keyManipulator.fullKey(parentId, "child-2");

    // Act
    Long committedAt =
        emitter.emitNormalGroup(
            parentId, Arrays.asList(valueWithWrite(fullTxId1), valueWithWrite(fullTxId2)));

    // Assert
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getChildIds()).containsExactly("child-1", "child-2");
    // The emit returns the single committedAt it stamped on the batched row, so every transaction
    // in the group commits its data records with the same value.
    assertThat(committedAt).isEqualTo(capturedState.getCreatedAt());
    assertThat(capturedState.getWriteSet()).isPresent();
    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).hasSize(2);
    assertThat(captured.getEntryGroups(0).getChildId()).isEqualTo("child-1");
    assertThat(captured.getEntryGroups(0).getEntriesList()).isNotEmpty();
    assertThat(captured.getEntryGroups(1).getChildId()).isEqualTo("child-2");
    assertThat(captured.getEntryGroups(1).getEntriesList()).isNotEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WithReadOnlyChildMixed_ShouldOmitReadOnlyEntryGroups()
      throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxIdWriting = keyManipulator.fullKey(parentId, "writing");
    String fullTxIdReadOnly = keyManipulator.fullKey(parentId, "read-only");

    // Act
    emitter.emitNormalGroup(
        parentId, Arrays.asList(valueWithWrite(fullTxIdWriting), valueReadOnly(fullTxIdReadOnly)));

    // Assert — only the writing child contributes an EntryGroup; the read-only child carries no
    // entries so its (empty) write set adds nothing.
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());
    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    // The read-only child contributes no EntryGroup but is still recorded as a parent-row member
    // (child_ids), so its committed state stays resolvable during recovery.
    assertThat(capturedState.getChildIds()).containsExactlyInAnyOrder("writing", "read-only");
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getEntryGroupsList()).hasSize(1);
    assertThat(captured.getEntryGroups(0).getChildId()).isEqualTo("writing");
  }

  @Test
  void emitter_emitNormalGroup_WithAllReadOnlyChildren_ShouldStillSetSchemaVersion()
      throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");

    // Act
    emitter.emitNormalGroup(parentId, Collections.singletonList(valueReadOnly(fullTxId)));

    // Assert
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());
    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).isEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WithEmptyValues_ShouldNotCallPutState() throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();

    // Act — all buffered transactions were manually rolled back, so emitNormalGroup is invoked
    // with an empty value list.
    Long committedAt = emitter.emitNormalGroup(parentId, Collections.emptyList());

    // Assert — putState must not be called; otherwise a spurious COMMITTED parent row would be
    // written. The null return is the contract that commitState's `assert committedAt != null`
    // guard relies on.
    assertThat(committedAt).isNull();
    verify(coordinatorMock, never()).putState(any(CoordinatorStateAccessor.State.class));
  }

  @Test
  void emitter_emitDelayedGroup_WithWritingValue_ShouldPersistStateWithWriteSet() throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");

    // Act
    Long committedAt = emitter.emitDelayedGroup(fullTxId, valueWithWrite(fullTxId));

    // Assert
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    // The emit returns the committedAt it stamped on the row.
    assertThat(committedAt).isEqualTo(capturedState.getCreatedAt());
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).hasSize(1);
    // Delayed group commit emits a single EntryGroup with child_id unset, distinguishing it from
    // a normal group commit row where each EntryGroup carries the child id.
    assertThat(captured.getEntryGroups(0).hasChildId()).isFalse();
    assertThat(captured.getEntryGroups(0).getEntriesList()).isNotEmpty();
  }

  @Test
  void emitter_emitDelayedGroup_WithReadOnlyValue_ShouldPersistStateWithEmptyWriteSet()
      throws Exception {
    // Arrange
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");

    // Act
    Long committedAt = emitter.emitDelayedGroup(fullTxId, valueReadOnly(fullTxId));

    // Assert
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(committedAt).isEqualTo(capturedState.getCreatedAt());
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).isEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WhenWriteSetLoggingDisabled_ShouldPassStateWithoutWriteSet()
      throws Exception {
    // Arrange — emitter constructed with write-set logging disabled.
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, false);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");

    // Act
    emitter.emitNormalGroup(parentId, Collections.singletonList(valueWithWrite(fullTxId)));

    // Assert — the State carries no WriteSet so the BLOB column is not populated (the column is not
    // part of the Coordinator schema in this configuration).
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getWriteSet()).isEmpty();
  }

  @Test
  void emitter_emitDelayedGroup_WhenWriteSetLoggingDisabled_ShouldPassStateWithoutWriteSet()
      throws Exception {
    // Arrange — same opt-in gating, delayed-group path.
    CoordinatorStateAccessor coordinatorMock = mock(CoordinatorStateAccessor.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(coordinatorMock, false);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");

    // Act
    emitter.emitDelayedGroup(fullTxId, valueWithWrite(fullTxId));

    // Assert — the persisted State carries no WriteSet.
    ArgumentCaptor<CoordinatorStateAccessor.State> stateCaptor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    CoordinatorStateAccessor.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getWriteSet()).isEmpty();
  }
}
