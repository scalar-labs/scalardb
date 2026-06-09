package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

class CommitHandlerWithGroupCommitTest extends CommitHandlerTest {
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private String parentKey;
  private String childKey;
  private CoordinatorGroupCommitter groupCommitter;
  @Captor private ArgumentCaptor<List<String>> groupCommitFullIdsArgumentCaptor;

  @Override
  protected void extraInitialize() {
    childKey = UUID.randomUUID().toString();
    String fullKey = groupCommitter.reserve(childKey);
    parentKey = keyManipulator.keysFromFullKey(fullKey).parentKey;
  }

  @Override
  protected String anyId() {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  @Override
  protected void extraCleanup() {
    groupCommitter.close();
  }

  private void createGroupCommitterIfNotExists() {
    if (groupCommitter == null) {
      groupCommitter =
          spy(new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 60000, 10)));
    }
  }

  @Override
  protected CommitHandler createCommitHandler(boolean coordinatorWriteOmissionOnReadOnlyEnabled) {
    createGroupCommitterIfNotExists();
    return new CommitHandlerWithGroupCommit(
        storage,
        coordinator,
        tableMetadataManager,
        parallelExecutor,
        new MutationsGrouper(storageInfoProvider),
        coordinatorWriteOmissionOnReadOnlyEnabled,
        false,
        groupCommitter);
  }

  @Override
  protected CommitHandler createCommitHandlerWithOnePhaseCommit() {
    createGroupCommitterIfNotExists();
    return new CommitHandlerWithGroupCommit(
        storage,
        coordinator,
        tableMetadataManager,
        parallelExecutor,
        mutationsGrouper,
        true,
        true,
        groupCommitter);
  }

  private String anyGroupCommitParentId() {
    return parentKey;
  }

  @Override
  protected void doThrowExceptionWhenCoordinatorPutState(
      TransactionState targetState, Class<? extends Exception> exceptionClass, Snapshot snapshot)
      throws CoordinatorException {

    doThrow(exceptionClass)
        .when(coordinator)
        .putStateForGroupCommit(
            eq(anyGroupCommitParentId()),
            anyList(),
            any(WriteSet.class),
            eq(targetState),
            anyLong());
  }

  @Override
  protected void doNothingWhenCoordinatorPutState() throws CoordinatorException {
    doNothing()
        .when(coordinator)
        .putStateForGroupCommit(
            anyString(), anyList(), any(WriteSet.class), any(TransactionState.class), anyLong());
  }

  @Override
  protected void verifyCoordinatorPutState(
      TransactionState expectedTransactionState, Snapshot snapshot) throws CoordinatorException {

    verify(coordinator)
        .putStateForGroupCommit(
            eq(anyGroupCommitParentId()),
            groupCommitFullIdsArgumentCaptor.capture(),
            any(WriteSet.class),
            eq(expectedTransactionState),
            anyLong());
    List<String> fullIds = groupCommitFullIdsArgumentCaptor.getValue();
    assertThat(fullIds.size()).isEqualTo(1);
    assertThat(fullIds.get(0)).isEqualTo(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    super.commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(
        withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    super.commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    groupCommitter.remove(anyId());
    clearInvocations(groupCommitter);

    super.commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
        withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void
      commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {

    super.commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
        withBeforePreparationHook);

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void
      commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    super
        .commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
            withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @Test
  public void commit_InReadOnlyModeWithWriteOmissionDisabled_ShouldCommitStateViaGroupCommit()
      throws CommitException, UnknownTransactionStatusException, CoordinatorException,
          ExecutionException {
    // Arrange
    // With coordinator write omission disabled, a read-only transaction writes a coordinator state
    // row. ConsensusCommitManager.begin() reserves a group commit slot for it (so its transaction
    // ID is a group commit full key), and the state must be committed through the group commit path
    // like any other state-writing transaction.
    CommitHandler handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doNothingWhenCoordinatorPutState();
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    // The COMMITTED state is written via the group commit path (putStateForGroupCommit).
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(groupCommitter, never()).remove(anyId());
  }

  @Test
  @Override
  public void
      commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException,
              UnknownTransactionStatusException {
    super
        .commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords();

    // Assert
    // abortState is skipped (omission enabled), but the reserved group commit slot is still
    // released once via onFailureBeforeCommit, so it does not leak.
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void
      commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
              InterruptedException, UnknownTransactionStatusException {
    super
        .commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords();

    // Assert
    // abortState is skipped (omission enabled), but the reserved group commit slot is still
    // released once via onFailureBeforeCommit, so it does not leak.
    verify(groupCommitter).remove(anyId());
  }

  @Test
  public void
      commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionEnabledAndBareTransactionId_ShouldNotWriteCoordinatorRowNorTouchGroupCommitter()
          throws ExecutionException, CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    // With coordinator write omission enabled, ConsensusCommitManager.begin() does not reserve a
    // group commit slot for a read-only transaction, so its transaction ID stays a bare UUID rather
    // than a group commit full key. This reproduces that production case (the inherited tests use
    // the full-key anyId() override instead). When the before-preparation hook fails, commit() must
    // still fail with CommitException, write no coordinator row, and leave the group committer
    // untouched: cancelGroupCommitIfNeeded skips remove() for the bare (non-full-key) ID.
    String bareId = UUID.randomUUID().toString();
    CommitHandler handler = spy(createCommitHandler(true));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        new TransactionContext(bareId, snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(coordinator, never()).putState(any());
    verify(coordinator, never())
        .putStateForGroupCommit(anyString(), anyList(), any(WriteSet.class), any(), anyLong());
    verify(handler, never()).abortState(any());
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(any());
    // The bare ID never reserved a slot, so cancelGroupCommitIfNeeded skips remove() entirely.
    verify(groupCommitter, never()).remove(anyString());
  }

  @Test
  @Override
  public void
      commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException {
    super
        .commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords();

    // Assert
    // The reserved group commit slot is released twice — once via onFailureBeforeCommit and once
    // via abortState (omission disabled) — both routing through cancelGroupCommitIfNeeded.
    verify(groupCommitter, times(2)).remove(anyId());
  }

  @Test
  @Override
  public void
      commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords()
          throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
              InterruptedException, UnknownTransactionStatusException {
    super
        .commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords();

    // Assert
    // The reserved group commit slot is released twice — once via onFailureBeforeCommit and once
    // via abortState (omission disabled) — both routing through cancelGroupCommitIfNeeded.
    verify(groupCommitter, times(2)).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_SnapshotIsolationWithReads_ShouldNotValidateRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    super.commit_SnapshotIsolationWithReads_ShouldNotValidateRecords(withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_SerializableIsolationWithReads_ShouldValidateRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    super.commit_SerializableIsolationWithReads_ShouldValidateRecords(withBeforePreparationHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @Test
  @Override
  public void validateRecords_ValidationNotRequired_ShouldNotCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    super.validateRecords_ValidationNotRequired_ShouldNotCallToSerializable();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void validateRecords_ValidationRequired_ShouldCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    super.validateRecords_ValidationRequired_ShouldCallToSerializable();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations()
      throws CommitConflictException, UnknownTransactionStatusException, ExecutionException,
          CrudException {
    super.onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations();

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void
      onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException, CrudException {
    super.onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException();

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void
      onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException, CrudException {
    super
        .onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException();

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void
      onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws ExecutionException, CrudException {
    super
        .onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowUnknownTransactionStatusException();

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    super.canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    super.canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse()
      throws Exception {
    super.canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenMutationsCanBeGrouped_ShouldReturnTrue() throws Exception {
    super.canOnePhaseCommit_WhenMutationsCanBeGrouped_ShouldReturnTrue();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() throws Exception {
    super.canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenSerializableIsolationWithReads_ShouldReturnFalse()
      throws Exception {
    super.canOnePhaseCommit_WhenSerializableIsolationWithReads_ShouldReturnFalse();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void canOnePhaseCommit_WhenSnapshotIsolationWithReads_ShouldReturnTrue() throws Exception {
    super.canOnePhaseCommit_WhenSnapshotIsolationWithReads_ShouldReturnTrue();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void
      canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException()
          throws ExecutionException {
    super
        .canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException();

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @Test
  @Override
  public void commit_OnePhaseCommitted_ShouldNotThrowAnyException()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    super.commit_OnePhaseCommitted_ShouldNotThrowAnyException();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void
      commit_OnePhaseCommitted_UnknownTransactionStatusExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    super
        .commit_OnePhaseCommitted_UnknownTransactionStatusExceptionThrown_ShouldThrowUnknownTransactionStatusException();
    groupCommitter.remove(anyId());
  }

  @Test
  void emitter_emitNormalGroup_WithMultipleWritingChildren_ShouldAggregatePerChild()
      throws Exception {
    // The slot reserved by extraInitialize is unused here — these emitter tests bypass the group
    // committer and drive the Emitter directly — so release it now to keep the @AfterEach
    // groupCommitter.close() from waiting out the slot-abort timeout.
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();
    String fullTxId1 = keyManipulator.fullKey(parentId, "child-1");
    String fullTxId2 = keyManipulator.fullKey(parentId, "child-2");

    TransactionContext context1 =
        new TransactionContext(
            fullTxId1,
            prepareSnapshotWithDifferentPartitionPut(),
            Isolation.SNAPSHOT,
            false,
            false);
    TransactionContext context2 =
        new TransactionContext(
            fullTxId2, prepareSnapshotWithSamePartitionPut(), Isolation.SNAPSHOT, false, false);

    // Act
    emitter.emitNormalGroup(parentId, Arrays.asList(context1, context2));

    // Assert — capture the WriteSet content.
    ArgumentCaptor<WriteSet> writeSetCaptor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorMock)
        .putStateForGroupCommit(
            eq(parentId),
            anyList(),
            writeSetCaptor.capture(),
            eq(TransactionState.COMMITTED),
            anyLong());

    WriteSet captured = writeSetCaptor.getValue();
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
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();
    String fullTxIdWriting = keyManipulator.fullKey(parentId, "writing");
    String fullTxIdReadOnly = keyManipulator.fullKey(parentId, "read-only");

    TransactionContext writingContext =
        new TransactionContext(
            fullTxIdWriting,
            prepareSnapshotWithDifferentPartitionPut(),
            Isolation.SNAPSHOT,
            false,
            false);
    TransactionContext readOnlyContext =
        new TransactionContext(
            fullTxIdReadOnly, prepareSnapshotWithoutWrites(), Isolation.SNAPSHOT, false, false);

    // Act
    emitter.emitNormalGroup(parentId, Arrays.asList(writingContext, readOnlyContext));

    // Assert — only the writing child appears in entry_groups.
    ArgumentCaptor<WriteSet> writeSetCaptor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorMock)
        .putStateForGroupCommit(
            eq(parentId),
            anyList(),
            writeSetCaptor.capture(),
            eq(TransactionState.COMMITTED),
            anyLong());

    WriteSet captured = writeSetCaptor.getValue();
    assertThat(captured.getEntryGroupsList()).hasSize(1);
    assertThat(captured.getEntryGroups(0).getChildId()).isEqualTo("writing");
  }

  @Test
  void emitter_emitNormalGroup_WithAllReadOnlyChildren_ShouldStillSetSchemaVersion()
      throws Exception {
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        new TransactionContext(
            fullTxId, prepareSnapshotWithoutWrites(), Isolation.SNAPSHOT, false, false);

    // Act
    emitter.emitNormalGroup(parentId, Collections.singletonList(context));

    // Assert — WriteSet is still non-null with schema_version set, but has no EntryGroups.
    ArgumentCaptor<WriteSet> writeSetCaptor = ArgumentCaptor.forClass(WriteSet.class);
    verify(coordinatorMock)
        .putStateForGroupCommit(
            eq(parentId),
            anyList(),
            writeSetCaptor.capture(),
            eq(TransactionState.COMMITTED),
            anyLong());

    WriteSet captured = writeSetCaptor.getValue();
    assertThat(captured.getSchemaVersion()).isEqualTo(1);
    assertThat(captured.getEntryGroupsList()).isEmpty();
  }

  @Test
  void emitter_emitNormalGroup_WithEmptyContexts_ShouldNotCallPutStateForGroupCommit()
      throws Exception {
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();

    // Act — all buffered transactions were manually rolled back, so emitNormalGroup is invoked
    // with an empty context list.
    emitter.emitNormalGroup(parentId, Collections.emptyList());

    // Assert — putStateForGroupCommit must not be called; otherwise a spurious COMMITTED parent
    // row would be written.
    verify(coordinatorMock, never())
        .putStateForGroupCommit(
            anyString(), anyList(), any(WriteSet.class), any(TransactionState.class), anyLong());
  }

  @Test
  void emitter_emitDelayedGroup_WithWritingContext_ShouldPersistStateWithWriteSet()
      throws Exception {
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        new TransactionContext(
            fullTxId, prepareSnapshotWithDifferentPartitionPut(), Isolation.SNAPSHOT, false, false);

    // Act
    emitter.emitDelayedGroup(fullTxId, context);

    // Assert — putState is called with a State carrying the encoded WriteSet.
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
    groupCommitter.remove(anyId());

    // Arrange
    Coordinator coordinatorMock = mock(Coordinator.class);
    CommitHandlerWithGroupCommit.Emitter emitter =
        new CommitHandlerWithGroupCommit.Emitter(
            coordinatorMock, new WriteSetEncoder(tableMetadataManager));

    String parentId = keyManipulator.generateParentKey();
    String fullTxId = keyManipulator.fullKey(parentId, "child");
    TransactionContext context =
        new TransactionContext(
            fullTxId, prepareSnapshotWithoutWrites(), Isolation.SNAPSHOT, false, false);

    // Act
    emitter.emitDelayedGroup(fullTxId, context);

    // Assert — WriteSet is still non-null with schema_version set, but has no EntryGroups.
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
}
