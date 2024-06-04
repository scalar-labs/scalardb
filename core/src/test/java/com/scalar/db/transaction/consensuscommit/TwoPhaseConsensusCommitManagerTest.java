package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.DecoratedTwoPhaseCommitTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TwoPhaseConsensusCommitManagerTest {
  private static final String ANY_TX_ID = "any_id";

  @Mock private DistributedStorage storage;
  @Mock private DistributedStorageAdmin admin;
  @Mock private ConsensusCommitConfig config;
  @Mock private DatabaseConfig databaseConfig;
  @Mock private Coordinator coordinator;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private RecoveryHandler recovery;
  @Mock private CommitHandler commit;

  private TwoPhaseConsensusCommitManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getIsolation()).thenReturn(Isolation.SNAPSHOT);
    when(config.getSerializableStrategy()).thenReturn(SerializableStrategy.EXTRA_READ);

    manager =
        new TwoPhaseConsensusCommitManager(
            storage,
            admin,
            config,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recovery,
            commit,
            null);
  }

  @Test
  public void begin_NoArgumentGiven_ReturnWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.begin()).getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.begin(ANY_TX_ID))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.shouldManageState).isTrue();
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void
      begin_TxIdGivenWithGroupCommitter_ReturnWithSpecifiedTxIdWithParentIdAndSnapshotIsolation()
          throws TransactionException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    String parentKey = keyManipulator.generateParentKey();
    String fullKey = keyManipulator.fullKey(parentKey, ANY_TX_ID);
    doReturn(fullKey).when(groupCommitter).reserve(anyString());
    TwoPhaseConsensusCommitManager managerWithGroupCommit =
        new TwoPhaseConsensusCommitManager(
            storage,
            admin,
            config,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recovery,
            commit,
            groupCommitter);

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) managerWithGroupCommit.begin(ANY_TX_ID))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.shouldManageState).isTrue();
    assertThat(transaction.getId()).isEqualTo(fullKey);
    Snapshot snapshot = transaction.getCrudHandler().getSnapshot();
    assertThat(snapshot.getId()).isEqualTo(fullKey);
    assertThat(keyManipulator.isFullKey(transaction.getId())).isTrue();
    verify(groupCommitter).reserve(ANY_TX_ID);
    assertThat(snapshot.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void begin_CalledTwice_ReturnRespectiveConsensusCommitWithSharedObjects()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction1 =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.begin()).getOriginalTransaction();
    TwoPhaseConsensusCommit transaction2 =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.begin()).getOriginalTransaction();

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

  @Test
  public void begin_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.begin(ANY_TX_ID);
    assertThatThrownBy(() -> manager.begin(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_NoArgumentGiven_ReturnWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.start()).getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.start(ANY_TX_ID))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_CalledTwice_ReturnRespectiveConsensusCommitWithSharedObjects()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction1 =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.start()).getOriginalTransaction();
    TwoPhaseConsensusCommit transaction2 =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.start()).getOriginalTransaction();

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

  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.start(ANY_TX_ID);
    assertThatThrownBy(() -> manager.start(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void join_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.join(ANY_TX_ID)).getOriginalTransaction();

    // Assert
    assertThat(transaction.shouldManageState).isTrue();
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void join_TxIdGivenWithGroupCommitter_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    String parentKey = keyManipulator.generateParentKey();
    String fullKey = keyManipulator.fullKey(parentKey, ANY_TX_ID);
    doReturn(fullKey).when(groupCommitter).reserve(anyString());
    TwoPhaseConsensusCommitManager managerWithGroupCommit =
        new TwoPhaseConsensusCommitManager(
            storage,
            admin,
            config,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recovery,
            commit,
            groupCommitter);

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) managerWithGroupCommit.join(ANY_TX_ID))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.shouldManageState).isFalse();
    assertThat(transaction.getId()).isEqualTo(ANY_TX_ID);
    Snapshot snapshot = transaction.getCrudHandler().getSnapshot();
    assertThat(snapshot.getId()).isEqualTo(ANY_TX_ID);
    assertThat(keyManipulator.isFullKey(transaction.getId())).isFalse();
    verify(groupCommitter, never()).reserve(anyString());
    assertThat(snapshot.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void join_CalledAfterJoinWithSameTxId_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager.join(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.join(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager.begin(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithJoin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager.join(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBeginOrJoin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.prepare();
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    doThrow(CommitConflictException.class).when(commit).commitState(any());

    TwoPhaseCommitTransaction transaction1 = manager.begin(ANY_TX_ID);
    transaction1.prepare();
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.prepare();
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      resume_CalledWithBeginAndRollback_RollbackExceptionThrown_ThrowTransactionNotFoundException()
          throws TransactionException {
    // Arrange
    doThrow(UnknownTransactionStatusException.class).when(commit).abortState(any());

    TwoPhaseCommitTransaction transaction1 = manager.begin(ANY_TX_ID);
    try {
      transaction1.rollback();
    } catch (RollbackException ignored) {
      // expected
    }

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void check_StateReturned_ReturnTheState() throws CoordinatorException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(new State(ANY_TX_ID, expected)));

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void check_StateIsEmpty_ReturnUnknown() throws CoordinatorException {
    // Arrange
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.empty());

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void check_CoordinatorExceptionThrown_ReturnUnknown() throws CoordinatorException {
    // Arrange
    CoordinatorException toThrow = mock(CoordinatorException.class);
    when(coordinator.getState(ANY_TX_ID)).thenThrow(toThrow);

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void rollback_CommitHandlerReturnsAborted_ShouldReturnTheState()
      throws UnknownTransactionStatusException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws UnknownTransactionStatusException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws UnknownTransactionStatusException {
    // Arrange
    when(commit.abortState(ANY_TX_ID)).thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void abort_CommitHandlerReturnsAborted_ShouldReturnTheState() throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
    // Arrange
    when(commit.abortState(ANY_TX_ID)).thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void get_ShouldGet() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    Result result = mock(Result.class);
    when(transaction.get(get)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = spied.get(get);

    // Assert
    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void
      get_TransactionNotFoundExceptionThrownByTransactionBegin_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager spied = spy(manager);
    doThrow(TransactionNotFoundException.class).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
  }

  @Test
  public void get_TransactionExceptionThrownByTransactionBegin_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager spied = spy(manager);
    doThrow(TransactionException.class).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin();
  }

  @Test
  public void get_CrudExceptionThrownByTransactionGet_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    when(transaction.get(get)).thenThrow(CrudException.class);

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_PreparationConflictExceptionThrownByTransactionPrepare_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(PreparationConflictException.class).when(transaction).prepare();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_ValidationConflictExceptionThrownByTransactionValidate_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(ValidationConflictException.class).when(transaction).validate();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_CommitConflictExceptionThrownByTransactionCommit_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_UnknownTransactionStatusExceptionThrownByTransactionCommit_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(UnknownTransactionStatusException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void get_CommitExceptionThrownByTransactionCommit_ShouldThrowUCrudException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void scan_ShouldScan() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    List<Result> results =
        Arrays.asList(mock(Result.class), mock(Result.class), mock(Result.class));
    when(transaction.scan(scan)).thenReturn(results);

    // Act
    List<Result> actual = spied.scan(scan);

    // Assert
    verify(spied).begin();
    verify(transaction).scan(scan);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void put_ShouldPut() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.put(put);

    // Assert
    verify(spied).begin();
    verify(transaction).put(put);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void put_MultiplePutsGiven_ShouldPut() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    List<Put> puts =
        Arrays.asList(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .intValue("col", 0)
                .build(),
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .intValue("col", 1)
                .build(),
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .intValue("col", 2)
                .build());
    // Act
    spied.put(puts);

    // Assert
    verify(spied).begin();
    verify(transaction).put(puts);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void insert_ShouldInsert() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.insert(insert);

    // Assert
    verify(spied).begin();
    verify(transaction).insert(insert);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void upsert_ShouldUpsert() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Upsert upsert =
        Upsert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.upsert(upsert);

    // Assert
    verify(spied).begin();
    verify(transaction).upsert(upsert);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void update_ShouldUpdate() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.update(update);

    // Assert
    verify(spied).begin();
    verify(transaction).update(update);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void delete_ShouldDelete() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).begin();
    verify(transaction).delete(delete);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void delete_MultipleDeletesGiven_ShouldDelete() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    List<Delete> deletes =
        Arrays.asList(
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .build());
    // Act
    spied.delete(deletes);

    // Assert
    verify(spied).begin();
    verify(transaction).delete(deletes);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void mutate_ShouldMutate() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseCommitTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    List<Mutation> mutations =
        Arrays.asList(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .intValue("col", 0)
                .build(),
            Insert.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .intValue("col", 1)
                .build(),
            Upsert.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .intValue("col", 2)
                .build(),
            Update.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 3))
                .intValue("col", 3)
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 4))
                .build());

    // Act
    spied.mutate(mutations);

    // Assert
    verify(spied).begin();
    verify(transaction).mutate(mutations);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }
}
