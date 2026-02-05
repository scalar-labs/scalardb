package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
import com.scalar.db.util.groupcommit.GroupCommitConfig;
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
      TransactionState targetState, Class<? extends Exception> exceptionClass)
      throws CoordinatorException {

    doThrow(exceptionClass)
        .when(coordinator)
        .putStateForGroupCommit(
            eq(anyGroupCommitParentId()), anyList(), eq(targetState), anyLong());
  }

  @Override
  protected void doNothingWhenCoordinatorPutState() throws CoordinatorException {
    doNothing()
        .when(coordinator)
        .putStateForGroupCommit(anyString(), anyList(), any(TransactionState.class), anyLong());
  }

  @Override
  protected void verifyCoordinatorPutState(TransactionState expectedTransactionState)
      throws CoordinatorException {

    verify(coordinator)
        .putStateForGroupCommit(
            eq(anyGroupCommitParentId()),
            groupCommitFullIdsArgumentCaptor.capture(),
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
}
