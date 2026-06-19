package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
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
      groupCommitter = new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 60000, 10));
    }
  }

  @Override
  protected CommitHandler createCommitHandler() {
    createGroupCommitterIfNotExists();
    return new CommitHandlerWithGroupCommit(
        storage, coordinator, tableMetadataManager, parallelExecutor, groupCommitter);
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

  @Test
  @Override
  public void abortStateForRollback_ShouldPutStateForLazyRecoveryRollbackAndReturnAborted()
      throws CoordinatorException, UnknownTransactionStatusException {
    super.abortStateForRollback_ShouldPutStateForLazyRecoveryRollbackAndReturnAborted();

    // abortStateForRollback(id) only writes the coordinator rollback marker; it never goes through
    // the group commit path, so the slot reserved by extraInitialize() is not released. Release it
    // here so the @AfterEach groupCommitter.close() does not block on the slot-abort timeout.
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void abortStateForRollback_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted()
      throws CoordinatorException, UnknownTransactionStatusException {
    super.abortStateForRollback_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void abortStateForRollback_WhenConflictAndNoStatePersisted_ShouldThrowUnknown()
      throws CoordinatorException {
    super.abortStateForRollback_WhenConflictAndNoStatePersisted_ShouldThrowUnknown();
    groupCommitter.remove(anyId());
  }

  @Test
  @Override
  public void abortStateForRollback_WhenCoordinatorExceptionThrown_ShouldThrowUnknown()
      throws CoordinatorException {
    super.abortStateForRollback_WhenCoordinatorExceptionThrown_ShouldThrowUnknown();
    groupCommitter.remove(anyId());
  }
}
