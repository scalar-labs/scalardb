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

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.common.ReadOnlyDistributedTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitManagerTest {
  private static final String ANY_TX_ID = "any_id";

  @Mock private DistributedStorage storage;
  @Mock private DistributedStorageAdmin admin;
  @Mock private ConsensusCommitConfig consensusCommitConfig;
  @Mock private DatabaseConfig databaseConfig;
  @Mock private Coordinator coordinator;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private RecoveryExecutor recoveryExecutor;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;

  private ConsensusCommitManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    manager =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit,
            Isolation.SNAPSHOT,
            null);
  }

  @Test
  public void begin_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin();

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isNotNull();
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin(ANY_TX_ID);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
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
    ConsensusCommitManager managerWithGroupCommit =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit,
            Isolation.SNAPSHOT,
            groupCommitter);

    // Act
    ConsensusCommit transaction = (ConsensusCommit) managerWithGroupCommit.begin(ANY_TX_ID);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(fullKey);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
    assertThat(keyManipulator.isFullKey(transaction.getId())).isTrue();
    verify(groupCommitter).reserve(ANY_TX_ID);
  }

  @Test
  public void
      beginReadOnly_TxIdGivenWithGroupCommitter_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
          throws TransactionException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    CoordinatorGroupCommitter groupCommitter = mock(CoordinatorGroupCommitter.class);
    ConsensusCommitManager managerWithGroupCommit =
        new ConsensusCommitManager(
            storage,
            admin,
            consensusCommitConfig,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit,
            Isolation.SNAPSHOT,
            groupCommitter);

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit)
            ((DecoratedDistributedTransaction) managerWithGroupCommit.beginReadOnly(ANY_TX_ID))
                .getOriginalTransaction();

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isTrue();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
    assertThat(keyManipulator.isFullKey(transaction.getId())).isFalse();
    verify(groupCommitter, never()).reserve(ANY_TX_ID);
  }

  @Test
  public void begin_CalledTwice_ShouldReturnTransactionsWithSharedHandlers()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction1 = (ConsensusCommit) manager.begin();
    ConsensusCommit transaction2 = (ConsensusCommit) manager.begin();

    // Assert
    assertThat(transaction1.getCrudHandler()).isSameAs(transaction2.getCrudHandler());
    assertThat(transaction1.getCommitHandler()).isSameAs(transaction2.getCommitHandler());
    assertThat(transaction1.getTransactionContext())
        .isNotSameAs(transaction2.getTransactionContext());
    assertThat(transaction1.getId()).isNotEqualTo(transaction2.getId());
  }

  @Test
  public void begin_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.begin(ANY_TX_ID);
    assertThatThrownBy(() -> manager.begin(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void
      beginReadOnly_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolationInReadOnlyMode()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager spied = spy(manager);

    // Act
    DistributedTransaction transaction = spied.beginReadOnly();

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(false));

    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
  }

  @Test
  public void beginReadOnly_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolationInReadOnlyMode()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager spied = spy(manager);

    // Act
    DistributedTransaction transaction = spied.beginReadOnly(ANY_TX_ID);

    // Assert
    verify(spied).begin(eq(ANY_TX_ID), eq(Isolation.SNAPSHOT), eq(true), eq(false));

    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
  }

  @Test
  public void
      begin_TxIdAndAttributesWithIsolationGiven_ReturnWithSpecifiedTxIdAndSpecifiedIsolation() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    ConsensusCommitOperationAttributes.setTransactionIsolation(attributes, Isolation.SERIALIZABLE);

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin(ANY_TX_ID, attributes);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
  }

  @Test
  public void
      begin_TxIdAndAttributesWithoutIsolationGiven_ReturnWithSpecifiedTxIdAndDefaultIsolation() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin(ANY_TX_ID, attributes);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
  }

  @Test
  public void
      beginReadOnly_TxIdAndAttributesWithIsolationGiven_ReturnWithSpecifiedTxIdAndSpecifiedIsolationInReadOnlyMode() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    ConsensusCommitOperationAttributes.setTransactionIsolation(attributes, Isolation.SERIALIZABLE);

    // Act
    DistributedTransaction transaction = manager.beginReadOnly(ANY_TX_ID, attributes);

    // Assert
    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
    ConsensusCommit inner =
        (ConsensusCommit) ((DecoratedDistributedTransaction) transaction).getOriginalTransaction();
    assertThat(inner.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(inner.getTransactionContext().isolation).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(inner.getTransactionContext().readOnly).isTrue();
  }

  @Test
  public void
      beginReadOnly_TxIdAndAttributesWithoutIsolationGiven_ReturnWithSpecifiedTxIdAndDefaultIsolationInReadOnlyMode() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    DistributedTransaction transaction = manager.beginReadOnly(ANY_TX_ID, attributes);

    // Assert
    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
    ConsensusCommit inner =
        (ConsensusCommit) ((DecoratedDistributedTransaction) transaction).getOriginalTransaction();
    assertThat(inner.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(inner.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(inner.getTransactionContext().readOnly).isTrue();
  }

  @Test
  public void start_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.start();

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isNotNull();
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void start_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.start(ANY_TX_ID);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void start_SerializableGiven_ReturnConsensusCommitWithSomeTxIdAndSerializable() {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit) manager.start(com.scalar.db.api.Isolation.SERIALIZABLE);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isNotNull();
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void start_NullIsolationGiven_ThrowNullPointerExceptionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.start((com.scalar.db.api.Isolation) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void start_CalledTwice_ShouldReturnTransactionsWithSharedHandlers()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction1 = (ConsensusCommit) manager.start();
    ConsensusCommit transaction2 = (ConsensusCommit) manager.start();

    // Assert
    assertThat(transaction1.getCrudHandler()).isSameAs(transaction2.getCrudHandler());
    assertThat(transaction1.getCommitHandler()).isSameAs(transaction2.getCommitHandler());
    assertThat(transaction1.getTransactionContext())
        .isNotSameAs(transaction2.getTransactionContext());
    assertThat(transaction1.getId()).isNotEqualTo(transaction2.getId());
  }

  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.start(ANY_TX_ID);
    assertThatThrownBy(() -> manager.start(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void
      startReadOnly_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolationInReadOnlyMode()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager spied = spy(manager);

    // Act
    DistributedTransaction transaction = spied.startReadOnly();

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(false));

    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
  }

  @Test
  public void startReadOnly_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolationInReadOnlyMode()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager spied = spy(manager);

    // Act
    DistributedTransaction transaction = spied.startReadOnly(ANY_TX_ID);

    // Assert
    verify(spied).begin(eq(ANY_TX_ID), eq(Isolation.SNAPSHOT), eq(true), eq(false));

    assertThat(transaction).isInstanceOf(ReadOnlyDistributedTransaction.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction1 = manager.begin(ANY_TX_ID);

    // Act
    DistributedTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    doThrow(CommitException.class).when(commit).commit(any(TransactionContext.class));

    DistributedTransaction transaction1 = manager.begin(ANY_TX_ID);
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction1 = manager.begin(ANY_TX_ID);

    // Act
    DistributedTransaction transaction2 = manager.join(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    doThrow(CommitException.class).when(commit).commit(any(TransactionContext.class));

    DistributedTransaction transaction1 = manager.begin(ANY_TX_ID);
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.join(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction = manager.begin(ANY_TX_ID);
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_TX_ID))
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
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
    // Arrange
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID))
        .thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void abort_CommitHandlerReturnsAborted_ShouldReturnTheState() throws TransactionException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID)).thenReturn(expected);

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
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
    // Arrange
    when(commit.abortStateWithoutWriteSet(ANY_TX_ID))
        .thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void get_ShouldGet() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    Result result = mock(Result.class);
    when(transaction.get(get)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = spied.get(get);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void get_CrudExceptionThrownByTransactionGet_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    when(transaction.get(get)).thenThrow(CrudException.class);

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_CommitConflictExceptionThrownByTransactionCommit_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_UnknownTransactionStatusExceptionThrownByTransactionCommit_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(UnknownTransactionStatusException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void get_CommitExceptionThrownByTransactionCommit_ShouldThrowUCrudException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void get_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));

    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    Result result = mock(Result.class);
    when(transaction.get(get)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = spied.get(get);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void scan_ShouldScan() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    List<Result> results =
        Arrays.asList(mock(Result.class), mock(Result.class), mock(Result.class));
    when(transaction.scan(scan)).thenReturn(results);

    // Act
    List<Result> actual = spied.scan(scan);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).scan(scan);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void scan_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    List<Result> results =
        Arrays.asList(mock(Result.class), mock(Result.class), mock(Result.class));
    when(transaction.scan(scan)).thenReturn(results);

    // Act
    List<Result> actual = spied.scan(scan);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));
    verify(transaction).scan(scan);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void getScannerAndScannerOne_ShouldReturnScannerAndReturnProperResult() throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThat(actual.one()).hasValue(result1);
    assertThat(actual.one()).hasValue(result2);
    assertThat(actual.one()).hasValue(result3);
    assertThat(actual.one()).isEmpty();
    actual.close();

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerAll_ShouldReturnScannerAndReturnProperResults() throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.all())
        .thenReturn(Arrays.asList(result1, result2, result3))
        .thenReturn(Collections.emptyList());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    List<Result> results = actual.all();
    assertThat(results).containsExactly(result1, result2, result3);
    assertThat(actual.all()).isEmpty();
    actual.close();

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerIterator_ShouldReturnScannerAndReturnProperResults()
      throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);

    Iterator<Result> iterator = actual.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result1);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    actual.close();

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void
      getScanner_CrudExceptionThrownByTransactionGetScanner_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    when(transaction.getScanner(scan)).thenThrow(CrudException.class);

    // Act Assert
    assertThatThrownBy(() -> spied.getScanner(scan)).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerOne_CrudExceptionThrownByScannerOne_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    when(scanner.one()).thenThrow(CrudException.class);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::one).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerAll_CrudExceptionThrownByScannerAll_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);
    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    when(scanner.all()).thenThrow(CrudException.class);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::all).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_CrudExceptionThrownByScannerClose_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);
    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    doThrow(CrudException.class).when(scanner).close();

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_CommitConflictExceptionThrownByTransactionCommit_ShouldRollbackTransactionAndThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    doThrow(CommitConflictException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_UnknownTransactionStatusExceptionByTransactionCommit_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(UnknownTransactionStatusException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
  }

  @Test
  public void
      getScannerAndScannerClose_CommitExceptionThrownByTransactionCommit_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    doThrow(CommitException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void put_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).put(put);
    verify(transaction).commit();
  }

  @Test
  public void put_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    // Act
    spied.put(put);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).put(put);
    verify(transaction).commit();
  }

  @Test
  public void put_MultiplePutsGiven_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).put(puts);
    verify(transaction).commit();
  }

  @Test
  public void insert_ShouldInsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).insert(insert);
    verify(transaction).commit();
  }

  @Test
  public void insert_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    // Act
    spied.insert(insert);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).insert(insert);
    verify(transaction).commit();
  }

  @Test
  public void upsert_ShouldUpsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).upsert(upsert);
    verify(transaction).commit();
  }

  @Test
  public void upsert_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    Upsert upsert =
        Upsert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    // Act
    spied.upsert(upsert);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).upsert(upsert);
    verify(transaction).commit();
  }

  @Test
  public void update_ShouldUpdate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).update(update);
    verify(transaction).commit();
  }

  @Test
  public void update_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    // Act
    spied.update(update);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).update(update);
    verify(transaction).commit();
  }

  @Test
  public void delete_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).delete(delete);
    verify(transaction).commit();
  }

  @Test
  public void delete_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
            .build();

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).delete(delete);
    verify(transaction).commit();
  }

  @Test
  public void delete_MultipleDeletesGiven_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).delete(deletes);
    verify(transaction).commit();
  }

  @Test
  public void mutate_ShouldMutate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));

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
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(false), eq(true));
    verify(transaction).mutate(mutations);
    verify(transaction).commit();
  }

  @Test
  public void mutate_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));

    List<Mutation> mutations =
        Arrays.asList(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .intValue("col", 0)
                .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .build());

    // Act
    spied.mutate(mutations);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(false), eq(true));
    verify(transaction).mutate(mutations);
    verify(transaction).commit();
  }

  @Test
  public void batch_ShouldBatch() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    List<Operation> operations =
        Collections.singletonList(
            Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build());

    @SuppressWarnings("unchecked")
    List<CrudOperable.BatchResult> batchResults = mock(List.class);

    when(transaction.batch(operations)).thenReturn(batchResults);

    // Act
    List<CrudOperable.BatchResult> actual = spied.batch(operations);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).batch(operations);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(batchResults);
  }

  @Test
  public void batch_WithIsolationAttribute_ShouldBeginWithSpecifiedIsolation()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));

    List<Operation> operations =
        Collections.singletonList(
            Get.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .attribute(ConsensusCommitOperationAttributes.TRANSACTION_ISOLATION, "SERIALIZABLE")
                .build());

    @SuppressWarnings("unchecked")
    List<CrudOperable.BatchResult> batchResults = mock(List.class);

    when(transaction.batch(operations)).thenReturn(batchResults);

    // Act
    List<CrudOperable.BatchResult> actual = spied.batch(operations);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SERIALIZABLE), eq(true), eq(true));
    verify(transaction).batch(operations);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(batchResults);
  }

  // ------------------------------------------------------------
  // finishTransaction
  // ------------------------------------------------------------

  private static final String ANY_NAMESPACE = "ns";
  private static final String ANY_TABLE = "tbl";

  private com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet writeSetWithSinglePutEntry() {
    return com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet.newBuilder()
        .setSchemaVersion(1)
        .addEntryGroups(
            com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup.newBuilder()
                .addEntries(putEntry("pk-1")))
        .build();
  }

  private Result preparedRecord(String txId) {
    Result record = mock(Result.class);
    when(record.getText(Attribute.ID)).thenReturn(txId);
    when(record.isNull(Attribute.STATE)).thenReturn(false);
    when(record.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    return record;
  }

  private com.scalar.db.transaction.consensuscommit.proto.v1.Entry putEntry(String pkValue) {
    return com.scalar.db.transaction.consensuscommit.proto.v1.Entry.newBuilder()
        .setEntryType(
            com.scalar.db.transaction.consensuscommit.proto.v1.Entry.EntryType.ENTRY_TYPE_WRITE)
        .setNamespaceName(ANY_NAMESPACE)
        .setTableName(ANY_TABLE)
        .setPartitionKey(
            com.scalar.db.transaction.consensuscommit.proto.v1.Key.newBuilder()
                .addColumns(
                    com.scalar.db.transaction.consensuscommit.proto.v1.Column.newBuilder()
                        .setName("pk")
                        .setTextValue(
                            com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue
                                .newBuilder()
                                .setValue(pkValue))))
        .build();
  }

  @Test
  public void finishTransaction_CommittedTransactionWithSinglePut_ShouldRecoverAndDeleteState()
      throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result record = preparedRecord(ANY_TX_ID);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(record));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(coordinator).getState(ANY_TX_ID);
    verify(storage).get(any(Get.class));
    verify(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_AbortedTransactionWithSinglePut_ShouldRecoverAndDeleteState()
      throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.ABORTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result record = preparedRecord(ANY_TX_ID);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(record));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_GroupCommitWithThreeChildren_ShouldRecoverAllAndDeleteParentState()
      throws Exception {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullChildId1 = keyManipulator.fullKey(parentId, "child-1");
    com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet writeSet =
        com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet.newBuilder()
            .setSchemaVersion(1)
            .addEntryGroups(
                com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup.newBuilder()
                    .setChildId("child-1")
                    .addEntries(putEntry("pk-1a"))
                    .addEntries(putEntry("pk-1b")))
            .addEntryGroups(
                com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup.newBuilder()
                    .setChildId("child-2")
                    .addEntries(putEntry("pk-2a"))
                    .addEntries(putEntry("pk-2b")))
            .addEntryGroups(
                com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup.newBuilder()
                    .setChildId("child-3")
                    .addEntries(putEntry("pk-3a"))
                    .addEntries(putEntry("pk-3b")))
            .build();
    String fullChildId2 = keyManipulator.fullKey(parentId, "child-2");
    String fullChildId3 = keyManipulator.fullKey(parentId, "child-3");
    State state = new State(parentId, writeSet, TransactionState.COMMITTED);
    when(coordinator.getState(fullChildId1)).thenReturn(Optional.of(state));
    Result r1 = preparedRecord(fullChildId1);
    Result r2 = preparedRecord(fullChildId2);
    Result r3 = preparedRecord(fullChildId3);
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(r1))
        .thenReturn(Optional.of(r1))
        .thenReturn(Optional.of(r2))
        .thenReturn(Optional.of(r2))
        .thenReturn(Optional.of(r3))
        .thenReturn(Optional.of(r3));

    // Act
    boolean finished = manager.finishTransaction(fullChildId1);

    // Assert — 6 records recovered (3 children x 2 entries), parent state deleted exactly once.
    assertThat(finished).isTrue();
    verify(recoveryExecutor, org.mockito.Mockito.times(6))
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(parentId);
  }

  @Test
  public void finishTransaction_StateRowMissing_ShouldReturnTrue() throws Exception {
    // Arrange
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.empty());

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert — the state row is already gone, so finishing is a no-op that reports success.
    assertThat(finished).isTrue();
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void finishTransaction_NoWriteSetRecorded_ShouldReturnFalseWithoutThrowing()
      throws Exception {
    // Arrange — State whose tx_write_set is NULL (terminated via
    // DistributedTransactionManager#rollback()/abort(), lazy-recovery abort, pre-feature row,
    // etc.).
    State state = new State(ANY_TX_ID, TransactionState.ABORTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert — the transaction is not applicable (no write set), so the call reports false and
    // leaves the state row for lazy recovery instead of throwing.
    assertThat(finished).isFalse();
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void finishTransaction_NonTerminalState_ShouldThrowAssertionError() throws Exception {
    // Arrange — A persisted State should never be PREPARED. If it is, that's an internal
    // invariant violation that finishTransaction must surface loudly.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.PREPARED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));

    // Act + Assert
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(AssertionError.class);
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void finishTransaction_GetStateFails_ShouldThrowTransactionExceptionWithCause()
      throws Exception {
    // Arrange
    CoordinatorException cause = new CoordinatorException("boom");
    when(coordinator.getState(ANY_TX_ID)).thenThrow(cause);

    // Act + Assert
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(TransactionException.class)
        .hasCause(cause);
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void finishTransaction_StorageGetFails_ShouldThrowTransactionExceptionWithCause()
      throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    com.scalar.db.exception.storage.ExecutionException cause =
        new com.scalar.db.exception.storage.ExecutionException("storage down");
    when(storage.get(any(Get.class))).thenThrow(cause);

    // Act + Assert
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(TransactionException.class)
        .hasCause(cause);
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void finishTransaction_RecordAlreadyGone_ShouldSkipEntryAndDeleteState() throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert — record was missing, so recovery is not invoked, but the state row is still cleaned
    // up because the transaction itself is terminal.
    assertThat(finished).isTrue();
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_ReadOnlyCommit_ShouldSkipRecoveryAndDeleteState() throws Exception {
    // Arrange — A read-only commit persists a non-null WriteSet with an empty entry_groups list
    // (when coordinatorWriteOmissionOnReadOnlyEnabled=false). The no-write-set rejection should
    // not fire, no recovery runs, and the state row is still cleaned up.
    com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet emptyWriteSet =
        com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet.newBuilder()
            .setSchemaVersion(1)
            .build();
    State state = new State(ANY_TX_ID, emptyWriteSet, TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(storage, never()).get(any(Get.class));
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_RecordAlreadyFinalized_ShouldSkipRecoveryAndDeleteState()
      throws Exception {
    // Arrange — Record's tx_state is already COMMITTED (someone else recovered it). The recovery
    // mutation would no-op via NoMutationException, so we filter it out up front.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result finalizedRecord = mock(Result.class);
    when(finalizedRecord.getText(Attribute.ID)).thenReturn(ANY_TX_ID);
    when(finalizedRecord.isNull(Attribute.STATE)).thenReturn(false);
    when(finalizedRecord.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(storage.get(any(Get.class))).thenReturn(Optional.of(finalizedRecord));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_RecordOverwrittenByUnrelatedTx_ShouldSkipRecoveryAndDeleteState()
      throws Exception {
    // Arrange — Record's tx_id no longer matches the transaction we are finishing because an
    // unrelated transaction has since overwritten it. Recovery would no-op via NoMutationException
    // on the tx_id mismatch, so we filter it out up front.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result overwrittenRecord = preparedRecord("some-other-tx-id");
    when(storage.get(any(Get.class))).thenReturn(Optional.of(overwrittenRecord));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(recoveryExecutor, never()).executeSynchronously(any(), any(), any());
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void
      finishTransaction_RecoverFailsWithExecutionException_ShouldThrowTransactionExceptionWithCause()
          throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result record = preparedRecord(ANY_TX_ID);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(record));
    com.scalar.db.exception.storage.ExecutionException cause =
        new com.scalar.db.exception.storage.ExecutionException("recovery storage down");
    doThrow(cause)
        .when(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));

    // Act + Assert
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(TransactionException.class)
        .hasCause(cause);
    verify(coordinator, never()).deleteState(anyString());
  }

  @Test
  public void
      finishTransaction_DeleteStateFailsWithCoordinatorException_ShouldThrowTransactionExceptionWithCause()
          throws Exception {
    // Arrange
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result record = preparedRecord(ANY_TX_ID);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(record));
    CoordinatorException cause = new CoordinatorException("delete failed");
    doThrow(cause).when(coordinator).deleteState(ANY_TX_ID);

    // Act + Assert
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(TransactionException.class)
        .hasCause(cause);
  }

  @Test
  public void
      finishTransaction_CalledTwiceAfterTransientStorageFailure_SecondCallShouldSucceedAndDeleteState()
          throws Exception {
    // Arrange — first call fails on storage.get with a transient error; the state row is left
    // intact. Second call sees the state row again and proceeds to completion.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    com.scalar.db.exception.storage.ExecutionException firstAttemptCause =
        new com.scalar.db.exception.storage.ExecutionException("transient storage hiccup");
    Result record = preparedRecord(ANY_TX_ID);
    when(storage.get(any(Get.class))).thenThrow(firstAttemptCause).thenReturn(Optional.of(record));

    // Act + Assert — first call throws
    assertThatThrownBy(() -> manager.finishTransaction(ANY_TX_ID))
        .isInstanceOf(TransactionException.class)
        .hasCause(firstAttemptCause);
    verify(coordinator, never()).deleteState(anyString());

    // Second call recovers and deletes the state row
    boolean finished = manager.finishTransaction(ANY_TX_ID);
    assertThat(finished).isTrue();
    verify(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void finishTransaction_RecordInDeletedState_ShouldExecuteRecoveryAndDeleteState()
      throws Exception {
    // Arrange — shouldRecover allows DELETED records to proceed to executeSynchronously (the
    // recovery handler rolls them back when the state row is ABORTED, or rolls them forward —
    // composing a Delete — when it is COMMITTED). This test exercises the DELETED-state branch
    // of shouldRecover via the happy COMMITTED-state path.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.COMMITTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result deletedRecord = mock(Result.class);
    when(deletedRecord.getText(Attribute.ID)).thenReturn(ANY_TX_ID);
    when(deletedRecord.isNull(Attribute.STATE)).thenReturn(false);
    when(deletedRecord.getInt(Attribute.STATE)).thenReturn(TransactionState.DELETED.get());
    when(storage.get(any(Get.class))).thenReturn(Optional.of(deletedRecord));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(ANY_TX_ID);
  }

  @Test
  public void
      finishTransaction_RecordInDeletedStateWithAbortedCoordinator_ShouldExecuteRecoveryAndDeleteState()
          throws Exception {
    // Arrange — the rollback counterpart of the previous test: an in-flight Delete left the record
    // in DELETED state while the Coordinator row says ABORTED. shouldRecover passes the DELETED
    // record through to executeSynchronously, where the recovery handler rolls it back to its
    // before-image. This exercises the DELETED-state branch of shouldRecover via the ABORTED-state
    // path.
    State state = new State(ANY_TX_ID, writeSetWithSinglePutEntry(), TransactionState.ABORTED);
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(state));
    Result deletedRecord = mock(Result.class);
    when(deletedRecord.getText(Attribute.ID)).thenReturn(ANY_TX_ID);
    when(deletedRecord.isNull(Attribute.STATE)).thenReturn(false);
    when(deletedRecord.getInt(Attribute.STATE)).thenReturn(TransactionState.DELETED.get());
    when(storage.get(any(Get.class))).thenReturn(Optional.of(deletedRecord));

    // Act
    boolean finished = manager.finishTransaction(ANY_TX_ID);

    // Assert
    assertThat(finished).isTrue();
    verify(recoveryExecutor)
        .executeSynchronously(any(Get.class), any(TransactionResult.class), eq(state));
    verify(coordinator).deleteState(ANY_TX_ID);
  }
}
