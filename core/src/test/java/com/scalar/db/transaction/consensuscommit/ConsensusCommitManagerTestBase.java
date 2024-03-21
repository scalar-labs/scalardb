package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class ConsensusCommitManagerTestBase {
  @Mock private DistributedStorage storage;
  @Mock private DistributedStorageAdmin admin;
  @Mock private DatabaseConfig databaseConfig;
  @Mock private ConsensusCommitConfig consensusCommitConfig;
  @Mock private Coordinator coordinator;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private RecoveryHandler recovery;
  @Mock private CommitHandler commit;

  private ConsensusCommitManager manager;

  void initialize() {}

  abstract String anyTxIdGivenByClient();

  abstract String anyTxIdAlreadyStarted();

  abstract boolean isGroupCommitEnabled();

  abstract Optional<CoordinatorGroupCommitter> groupCommitter();

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    initialize();

    manager =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recovery,
            commit,
            groupCommitter().orElse(null));

    when(consensusCommitConfig.getIsolation()).thenReturn(Isolation.SNAPSHOT);
    when(consensusCommitConfig.getSerializableStrategy())
        .thenReturn(SerializableStrategy.EXTRA_READ);
  }

  @Test
  public void begin_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @DisabledIf("isGroupCommitEnabled")
  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin(anyTxIdGivenByClient()))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId())
        .isEqualTo(anyTxIdGivenByClient());
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @EnabledIf("isGroupCommitEnabled")
  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdPlusParentIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin(anyTxIdGivenByClient()))
                .getOriginalTransaction();

    // Assert
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    Snapshot snapshot = transaction.getCrudHandler().getSnapshot();
    Keys<String, String, String> groupCommitKeys = keyManipulator.keysFromFullKey(snapshot.getId());
    assertThat(groupCommitKeys.childKey).isEqualTo(anyTxIdGivenByClient());
    assertThat(snapshot.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void begin_CalledTwice_ReturnRespectiveConsensusCommitWithSharedCommitAndRecovery()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction1 =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();
    ConsensusCommit transaction2 =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.begin()).getOriginalTransaction();

    // Assert
    assertThat(transaction1.getCrudHandler()).isNotEqualTo(transaction2.getCrudHandler());
    assertThat(transaction1.getCrudHandler().getSnapshot().getId())
        .isNotEqualTo(transaction2.getCrudHandler().getSnapshot().getId());
    assertThat(transaction1.getCommitHandler())
        .isEqualTo(transaction2.getCommitHandler())
        .isEqualTo(commit);
    assertThat(transaction1.getRecoveryHandler())
        .isEqualTo(transaction2.getRecoveryHandler())
        .isEqualTo(recovery);
  }

  @DisabledIf("isGroupCommitEnabled")
  @Test
  public void begin_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.begin(anyTxIdGivenByClient());
    assertThatThrownBy(() -> manager.begin(anyTxIdGivenByClient()))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.start()).getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @DisabledIf("isGroupCommitEnabled")
  @Test
  public void start_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.start(anyTxIdGivenByClient()))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId())
        .isEqualTo(anyTxIdGivenByClient());
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @EnabledIf("isGroupCommitEnabled")
  @Test
  public void start_TxIdGiven_ReturnWithSpecifiedTxIdPlusParentIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.start(anyTxIdGivenByClient()))
                .getOriginalTransaction();

    // Assert
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    Snapshot snapshot = transaction.getCrudHandler().getSnapshot();
    Keys<String, String, String> groupCommitKeys = keyManipulator.keysFromFullKey(snapshot.getId());
    assertThat(groupCommitKeys.childKey).isEqualTo(anyTxIdGivenByClient());
    assertThat(snapshot.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_SerializableGiven_ReturnConsensusCommitWithSomeTxIdAndSerializable()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction)
                    manager.start(com.scalar.db.api.Isolation.SERIALIZABLE))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void start_NullIsolationGiven_ThrowNullPointerExceptionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.start((com.scalar.db.api.Isolation) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void start_CalledTwice_ReturnRespectiveConsensusCommitWithSharedCommitAndRecovery()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction1 =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.start()).getOriginalTransaction();
    ConsensusCommit transaction2 =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) manager.start()).getOriginalTransaction();

    // Assert
    assertThat(transaction1.getCrudHandler()).isNotEqualTo(transaction2.getCrudHandler());
    assertThat(transaction1.getCrudHandler().getSnapshot().getId())
        .isNotEqualTo(transaction2.getCrudHandler().getSnapshot().getId());
    assertThat(transaction1.getCommitHandler())
        .isEqualTo(transaction2.getCommitHandler())
        .isEqualTo(commit);
    assertThat(transaction1.getRecoveryHandler())
        .isEqualTo(transaction2.getRecoveryHandler())
        .isEqualTo(recovery);
  }

  @DisabledIf("isGroupCommitEnabled")
  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.start(anyTxIdGivenByClient());
    assertThatThrownBy(() -> manager.start(anyTxIdGivenByClient()))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransaction transaction1 = manager.begin(anyTxIdGivenByClient());

    // Act
    DistributedTransaction transaction2 = manager.resume(transaction1.getId());

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.resume(anyTxIdGivenByClient()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(anyTxIdGivenByClient());
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    doThrow(CommitException.class).when(commit).commit(any());

    DistributedTransaction transaction1 = manager.begin(anyTxIdGivenByClient());
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.resume(transaction1.getId());

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(anyTxIdGivenByClient());
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransaction transaction1 = manager.begin(anyTxIdGivenByClient());

    // Act
    DistributedTransaction transaction2 = manager.join(transaction1.getId());

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.join(anyTxIdGivenByClient()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(anyTxIdGivenByClient());
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.join(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    doThrow(CommitException.class).when(commit).commit(any());

    DistributedTransaction transaction1 = manager.begin(anyTxIdGivenByClient());
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.join(transaction1.getId());

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(anyTxIdGivenByClient());
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.join(transaction.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void check_StateReturned_ReturnTheState() throws CoordinatorException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(coordinator.getState(anyTxIdAlreadyStarted()))
        .thenReturn(Optional.of(new State(anyTxIdAlreadyStarted(), expected)));

    // Act
    TransactionState actual = manager.getState(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void check_StateIsEmpty_ReturnUnknown() throws CoordinatorException {
    // Arrange
    when(coordinator.getState(anyTxIdAlreadyStarted())).thenReturn(Optional.empty());

    // Act
    TransactionState actual = manager.getState(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void check_CoordinatorExceptionThrown_ReturnUnknown() throws CoordinatorException {
    // Arrange
    CoordinatorException toThrow = mock(CoordinatorException.class);
    when(coordinator.getState(anyTxIdAlreadyStarted())).thenThrow(toThrow);

    // Act
    TransactionState actual = manager.getState(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void rollback_CommitHandlerReturnsAborted_ShouldReturnTheState()
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortState(anyTxIdAlreadyStarted())).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abortState(anyTxIdAlreadyStarted())).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
    // Arrange
    when(commit.abortState(anyTxIdAlreadyStarted()))
        .thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.rollback(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void abort_CommitHandlerReturnsAborted_ShouldReturnTheState() throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortState(anyTxIdAlreadyStarted())).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abortState(anyTxIdAlreadyStarted())).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
    // Arrange
    when(commit.abortState(anyTxIdAlreadyStarted()))
        .thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.abort(anyTxIdAlreadyStarted());

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }
}
