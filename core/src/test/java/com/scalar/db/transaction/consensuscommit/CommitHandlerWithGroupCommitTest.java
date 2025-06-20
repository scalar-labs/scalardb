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
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
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
      boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    super.commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(withSnapshotHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    super.commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(withSnapshotHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
      boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    groupCommitter.remove(anyId());
    clearInvocations(groupCommitter);

    super.commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
        withSnapshotHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void
      commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
          boolean withSnapshotHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {

    super.commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
        withSnapshotHook);

    // Assert
    verify(groupCommitter).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void
      commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
          boolean withSnapshotHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    super
        .commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
            withSnapshotHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Override
  public void commit_NoReadsInSnapshot_ShouldNotValidateRecords(boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    super.commit_NoReadsInSnapshot_ShouldNotValidateRecords(withSnapshotHook);

    // Assert
    verify(groupCommitter, never()).remove(anyId());
  }

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenValidationRequired_ShouldReturnFalse() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenMutationsCanBeGrouped_ShouldReturnTrue() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void canOnePhaseCommit_WhenMutationsGrouperThrowsException_ShouldThrowCommitException() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void
      onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void
      onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowCommitException() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void commit_OnePhaseCommitted_ShouldNotThrowAnyException() {}

  @Disabled("Enabling both one-phase commit and group commit is not supported")
  @Override
  @Test
  public void commit_OnePhaseCommitted_CommitExceptionThrown_ShouldThrowCommitException() {}
}
