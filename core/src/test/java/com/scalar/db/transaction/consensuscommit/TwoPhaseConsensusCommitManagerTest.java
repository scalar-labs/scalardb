package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
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
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
  @Mock private RecoveryExecutor recoveryExecutor;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;

  private TwoPhaseConsensusCommitManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getIsolation()).thenReturn(Isolation.SNAPSHOT);

    manager =
        new TwoPhaseConsensusCommitManager(
            storage,
            admin,
            config,
            databaseConfig,
            coordinator,
            parallelExecutor,
            recoveryExecutor,
            crud,
            commit);
  }

  @Test
  public void begin_NoArgumentGiven_ReturnWithSomeTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction = (TwoPhaseConsensusCommit) manager.begin();

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isNotNull();
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction = (TwoPhaseConsensusCommit) manager.begin(ANY_TX_ID);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void begin_CalledTwice_ShouldReturnTransactionsWithSharedHandlers() {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction1 = (TwoPhaseConsensusCommit) manager.begin();
    TwoPhaseConsensusCommit transaction2 = (TwoPhaseConsensusCommit) manager.begin();

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
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    // Act Assert
    manager.begin(ANY_TX_ID);
    assertThatThrownBy(() -> manager.begin(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void begin_NoArgumentGiven_WithGroupCommitEnabled_ShouldThrowException() {
    // Arrange
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> manager.begin()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void begin_TxIdGiven_WithGroupCommitEnabled_ShouldThrowException() {
    // Arrange
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> manager.begin(ANY_TX_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void start_NoArgumentGiven_ReturnWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction = (TwoPhaseConsensusCommit) manager.start();

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
    TwoPhaseConsensusCommit transaction = (TwoPhaseConsensusCommit) manager.start(ANY_TX_ID);

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void start_CalledTwice_ShouldReturnTransactionsWithSharedHandlers()
      throws TransactionException {
    // Arrange

    // Act
    TwoPhaseConsensusCommit transaction1 = (TwoPhaseConsensusCommit) manager.start();
    TwoPhaseConsensusCommit transaction2 = (TwoPhaseConsensusCommit) manager.start();

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
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    // Act Assert
    manager.start(ANY_TX_ID);
    assertThatThrownBy(() -> manager.start(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_NoArgumentGiven_WithGroupCommitEnabled_ShouldThrowException() {
    // Arrange
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> manager.start()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void start_TxIdGiven_WithGroupCommitEnabled_ShouldThrowException() {
    // Arrange
    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> manager.start(ANY_TX_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void join_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    // Act
    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager.join(ANY_TX_ID)).getOriginalTransaction();

    // Assert
    assertThat(transaction.getTransactionContext().transactionId).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getTransactionContext().isolation).isEqualTo(Isolation.SNAPSHOT);
    assertThat(transaction.getTransactionContext().readOnly).isFalse();
    assertThat(transaction.getCrudHandler()).isEqualTo(crud);
    assertThat(transaction.getCommitHandler()).isEqualTo(commit);
  }

  @Test
  public void join_CalledAfterJoinWithSameTxId_ReturnSameTransactionObject()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    TwoPhaseCommitTransaction transaction1 = manager.join(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.join(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_TxIdGiven_WithGroupCommitEnabled_ShouldThrowException() {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    when(config.isCoordinatorGroupCommitEnabled()).thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_TX_ID)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    TwoPhaseCommitTransaction transaction1 = manager.begin(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithJoin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    TwoPhaseCommitTransaction transaction1 = manager.join(ANY_TX_ID);

    // Act
    TwoPhaseCommitTransaction transaction2 = manager.resume(ANY_TX_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBeginOrJoin_ThrowTransactionNotFoundException() {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_TX_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

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
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

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
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

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
    TwoPhaseCommitTransactionManager manager =
        new ActiveTransactionManagedTwoPhaseCommitTransactionManager(this.manager, -1);

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

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void get_CrudExceptionThrownByTransactionGet_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
      get_PreparationConflictExceptionThrownByTransactionPrepare_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(PreparationConflictException.class).when(transaction).prepare();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_ValidationConflictExceptionThrownByTransactionValidate_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(ValidationConflictException.class).when(transaction).validate();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_CommitConflictExceptionThrownByTransactionCommit_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
  public void scan_ShouldScan() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void getScannerAndScannerOne_ShouldReturnScannerAndReturnProperResult() throws Exception {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerAll_ShouldReturnScannerAndReturnProperResults() throws Exception {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerIterator_ShouldReturnScannerAndReturnProperResults()
      throws Exception {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void
      getScanner_CrudExceptionThrownByTransactionGetScanner_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);
    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);
    TwoPhaseConsensusCommitManager spied = spy(manager);
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
      getScannerAndScannerClose_PreparationConflictExceptionThrownByTransactionPrepare_ShouldRollbackTransactionAndThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    doThrow(PreparationConflictException.class).when(transaction).prepare();

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
      getScannerAndScannerClose_ValidationConflictExceptionThrownByTransactionValidate_ShouldRollbackTransactionAndThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    doThrow(ValidationConflictException.class).when(transaction).validate();

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
      getScannerAndScannerClose_CommitConflictExceptionThrownByTransactionCommit_ShouldRollbackTransactionAndThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void put_MultiplePutsGiven_ShouldPut() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void insert_ShouldInsert() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void upsert_ShouldUpsert() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void update_ShouldUpdate() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void delete_ShouldDelete() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void delete_MultipleDeletesGiven_ShouldDelete() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void mutate_ShouldMutate() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
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
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
  }

  @Test
  public void batch_ShouldBatch() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = mock(TwoPhaseCommitTransaction.class);

    TwoPhaseConsensusCommitManager spied = spy(manager);
    doReturn(transaction)
        .when(spied)
        .begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));

    @SuppressWarnings("unchecked")
    List<Operation> operations = mock(List.class);

    @SuppressWarnings("unchecked")
    List<CrudOperable.BatchResult> batchResults = mock(List.class);

    when(transaction.batch(operations)).thenReturn(batchResults);

    // Act
    List<CrudOperable.BatchResult> actual = spied.batch(operations);

    // Assert
    verify(spied).begin(anyString(), eq(Isolation.SNAPSHOT), eq(true), eq(true));
    verify(transaction).batch(operations);
    verify(transaction).prepare();
    verify(transaction).validate();
    verify(transaction).commit();
    assertThat(actual).isEqualTo(batchResults);
  }
}
