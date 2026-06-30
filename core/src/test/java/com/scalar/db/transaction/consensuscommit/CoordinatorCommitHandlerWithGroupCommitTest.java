package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
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
 * CoordinatorCommitHandlerWithGroupCommit.Emitter}. Tests for inherited (unchanged) methods like
 * {@code forceAbortState} and {@code handleCommitConflict} live in {@link
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

  @Mock private Coordinator coordinator;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ConsensusCommitConfig config;

  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private String fullId;
  private CoordinatorGroupCommitter groupCommitter;
  private CoordinatorCommitHandlerWithGroupCommit handler;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    groupCommitter =
        spy(new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 60000, 10)));
    // The handler's constructor wires the Emitter into the groupCommitter via setEmitter(); the
    // emitter must be in place before reserve() is called so that the slot can be emitted later.
    handler =
        new CoordinatorCommitHandlerWithGroupCommit(
            coordinator, new WriteSetEncoder(tableMetadataManager), groupCommitter, true);
    fullId = groupCommitter.reserve(UUID.randomUUID().toString());
  }

  @AfterEach
  void tearDown() {
    // Release any outstanding reserved slot so groupCommitter.close() does not block on the
    // slot-abort timeout. Tests that drive Emitter directly never bind the slot to a result.
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

  // ---------- commitState (group-commit path) ----------

  @Test
  void commitState_WhenSuccessful_ShouldRouteThroughGroupCommitter() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.commitState(context);

    // Assert — the group-committer.ready() is the dispatch boundary into the emitter.
    verify(groupCommitter).ready(eq(fullId), eq(context));
  }

  @Test
  void commitState_WhenGroupCommitConflict_ShouldCancelSlotAndHandleConflict() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    GroupCommitConflictException cause = new GroupCommitConflictException("conflict");
    doThrow(cause).when(groupCommitter).ready(eq(fullId), eq(context));
    when(coordinator.getState(anyString())).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
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
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    CoordinatorConflictException cause = new CoordinatorConflictException("coordinator conflict");
    GroupCommitException groupCommitException =
        new GroupCommitException("group commit failed", cause);
    doThrow(groupCommitException).when(groupCommitter).ready(eq(fullId), eq(context));
    when(coordinator.getState(anyString())).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
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
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    RuntimeException cause = new RuntimeException("storage unavailable");
    GroupCommitException groupCommitException =
        new GroupCommitException("group commit failed", cause);
    doThrow(groupCommitException).when(groupCommitter).ready(eq(fullId), eq(context));

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining("The coordinator status is unknown")
        .hasCause(cause);

    // The slot is released even on the unknown-status path.
    verify(groupCommitter).remove(fullId);
    // The state is unknown, so the coordinator state is never re-read to resolve a conflict.
    verify(coordinator, never()).getState(anyString());
  }

  // ---------- abortState (cancelGroupCommitIfNeeded) ----------

  @Test
  void abortState_WhenSlotReserved_ShouldCancelGroupCommitAndDelegate() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    TransactionState result = handler.abortState(context);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(groupCommitter).remove(fullId);
    verify(coordinator).putState(any(Coordinator.State.class));
  }

  @Test
  void abortState_WhenSlotNotReserved_ShouldNotTouchGroupCommitter() throws Exception {
    // Arrange — release the slot reserved by setUp() so the bare-id context starts clean and
    // groupCommitter.close() does not block on it during teardown.
    groupCommitter.remove(fullId);
    clearInvocations(groupCommitter);

    String bareId = UUID.randomUUID().toString();
    Snapshot snapshot = prepareSnapshotWithWrite(bareId);
    TransactionContext context = createContext(bareId, snapshot, /* slotReserved= */ false);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.abortState(context);

    // Assert
    verify(groupCommitter, never()).remove(anyString());
  }

  // ---------- handleCommitConflict (group-commit specific) ----------

  @Test
  void
      handleCommitConflict_GroupCommitConflictExceptionGivenAndNoStatePersisted_ShouldThrowCommitConflict()
          throws Exception {
    // The group-commit conflict route (commitState catches GroupCommitConflictException and calls
    // handleCommitConflict) must, when the coordinator state is absent on re-read, resolve to a
    // definitive conflict -- throw CommitConflictException -- the same outcome as the normal-commit
    // conflict route. getState for a group-commit child checks both the parent and full-ID rows, so
    // an absent state genuinely means the transaction never committed. Record rollback itself is
    // the orchestrator's responsibility (see CommitHandlerTest).

    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite(fullId);
    TransactionContext context = createContext(fullId, snapshot, /* slotReserved= */ true);
    when(coordinator.getState(fullId)).thenReturn(Optional.empty());
    GroupCommitConflictException cause = new GroupCommitConflictException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(context, cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(coordinator).getState(fullId);
  }

  // ---------- Emitter ----------

  @Test
  void emitter_emitNormalGroup_WithMultipleWritingChildren_ShouldAggregatePerChild()
      throws Exception {
    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId1 = keyManipulator.fullKey(parentId, "child-1");
    String fullTxId2 = keyManipulator.fullKey(parentId, "child-2");

    TransactionContext context1 =
        createContext(fullTxId1, prepareSnapshotWithWrite(fullTxId1), /* slotReserved= */ true);
    TransactionContext context2 =
        createContext(fullTxId2, prepareSnapshotWithWrite(fullTxId2), /* slotReserved= */ true);

    // Act
    emitter.emitNormalGroup(parentId, Arrays.asList(context1, context2));

    // Assert
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getChildIds()).containsExactly("child-1", "child-2");
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
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxIdWriting = keyManipulator.fullKey(parentId, "writing");
    String fullTxIdReadOnly = keyManipulator.fullKey(parentId, "read-only");

    TransactionContext writingContext =
        createContext(
            fullTxIdWriting, prepareSnapshotWithWrite(fullTxIdWriting), /* slotReserved= */ true);
    TransactionContext readOnlyContext =
        createContext(
            fullTxIdReadOnly, prepareEmptySnapshot(fullTxIdReadOnly), /* slotReserved= */ true);

    // Act
    emitter.emitNormalGroup(parentId, Arrays.asList(writingContext, readOnlyContext));

    // Assert
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());
    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getEntryGroupsList()).hasSize(1);
    assertThat(captured.getEntryGroups(0).getChildId()).isEqualTo("writing");
  }

  @Test
  void emitter_emitNormalGroup_WithAllReadOnlyChildren_ShouldStillSetSchemaVersion()
      throws Exception {
    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        createContext(fullTxId, prepareEmptySnapshot(fullTxId), /* slotReserved= */ true);

    // Act
    emitter.emitNormalGroup(parentId, Collections.singletonList(context));

    // Assert
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());
    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).isEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WithEmptyContexts_ShouldNotCallPutState() throws Exception {
    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();

    // Act — all buffered transactions were manually rolled back, so emitNormalGroup is invoked
    // with an empty context list.
    emitter.emitNormalGroup(parentId, Collections.emptyList());

    // Assert — putState must not be called; otherwise a spurious COMMITTED parent row would be
    // written.
    verify(coordinatorMock, never()).putState(any(Coordinator.State.class));
  }

  @Test
  void emitter_emitDelayedGroup_WithWritingContext_ShouldPersistStateWithWriteSet()
      throws Exception {
    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        createContext(fullTxId, prepareSnapshotWithWrite(fullTxId), /* slotReserved= */ true);

    // Act
    emitter.emitDelayedGroup(fullTxId, context);

    // Assert
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
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
  void emitter_emitDelayedGroup_WithReadOnlyContext_ShouldPersistStateWithEmptyWriteSet()
      throws Exception {
    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), true);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        createContext(fullTxId, prepareEmptySnapshot(fullTxId), /* slotReserved= */ true);

    // Act
    emitter.emitDelayedGroup(fullTxId, context);

    // Assert
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getWriteSet()).isPresent();

    WriteSet captured = capturedState.getWriteSet().get();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).isEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WhenWriteSetLoggingDisabled_ShouldPassStateWithoutWriteSet()
      throws Exception {
    // Arrange — emitter constructed with write-set logging disabled.
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), false);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        createContext(fullTxId, prepareSnapshotWithWrite(fullTxId), /* slotReserved= */ true);

    // Act
    emitter.emitNormalGroup(parentId, Collections.singletonList(context));

    // Assert — the State carries no WriteSet so the BLOB column is not populated (the column is not
    // part of the Coordinator schema in this configuration).
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(parentId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getWriteSet()).isEmpty();
  }

  @Test
  void emitter_emitDelayedGroup_WhenWriteSetLoggingDisabled_ShouldPassStateWithoutWriteSet()
      throws Exception {
    // Arrange — same opt-in gating, delayed-group path.
    Coordinator coordinatorMock = mock(Coordinator.class);
    CoordinatorCommitHandlerWithGroupCommit.Emitter emitter =
        new CoordinatorCommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager), false);

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        createContext(fullTxId, prepareSnapshotWithWrite(fullTxId), /* slotReserved= */ true);

    // Act
    emitter.emitDelayedGroup(fullTxId, context);

    // Assert — the persisted State carries no WriteSet.
    ArgumentCaptor<Coordinator.State> stateCaptor =
        ArgumentCaptor.forClass(Coordinator.State.class);
    verify(coordinatorMock).putState(stateCaptor.capture());

    Coordinator.State capturedState = stateCaptor.getValue();
    assertThat(capturedState.getId()).isEqualTo(fullTxId);
    assertThat(capturedState.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(capturedState.getWriteSet()).isEmpty();
  }
}
