package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
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
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitManagerTest {
  private static final String ANY_TX_ID = "any_id";

  @Mock private DistributedStorage storage;
  @Mock private DistributedStorageAdmin admin;
  @Mock private DatabaseConfig databaseConfig;
  @Mock private ConsensusCommitConfig consensusCommitConfig;
  @Mock private Coordinator coordinator;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private RecoveryHandler recovery;
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
            recovery,
            commit,
            null);

    when(consensusCommitConfig.getIsolation()).thenReturn(Isolation.SNAPSHOT);
    when(consensusCommitConfig.getSerializableStrategy())
        .thenReturn(SerializableStrategy.EXTRA_READ);
  }

  @Test
  public void begin_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void begin_TxIdGiven_ReturnWithSpecifiedTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.begin(ANY_TX_ID);

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void
      begin_TxIdGivenWithGroupCommitter_ReturnWithSpecifiedTxIdWithParentIdAndSnapshotIsolation() {
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
            recovery,
            commit,
            groupCommitter);

    // Act
    ConsensusCommit transaction = (ConsensusCommit) managerWithGroupCommit.begin(ANY_TX_ID);

    // Assert
    assertThat(transaction.getId()).isEqualTo(fullKey);
    Snapshot snapshot = transaction.getCrudHandler().getSnapshot();
    assertThat(snapshot.getId()).isEqualTo(fullKey);
    assertThat(keyManipulator.isFullKey(transaction.getId())).isTrue();
    verify(groupCommitter).reserve(ANY_TX_ID);
    assertThat(snapshot.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void begin_CalledTwice_ReturnRespectiveConsensusCommitWithSharedCommitAndRecovery() {
    // Arrange

    // Act
    ConsensusCommit transaction1 = (ConsensusCommit) manager.begin();
    ConsensusCommit transaction2 = (ConsensusCommit) manager.begin();

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.begin(ANY_TX_ID);
    assertThatThrownBy(() -> manager.begin(ANY_TX_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation()
      throws TransactionException {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.start();

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
    ConsensusCommit transaction = (ConsensusCommit) manager.start(ANY_TX_ID);

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isEqualTo(ANY_TX_ID);
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_SerializableGiven_ReturnConsensusCommitWithSomeTxIdAndSerializable() {
    // Arrange

    // Act
    ConsensusCommit transaction =
        (ConsensusCommit) manager.start(com.scalar.db.api.Isolation.SERIALIZABLE);

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
    ConsensusCommit transaction1 = (ConsensusCommit) manager.start();
    ConsensusCommit transaction2 = (ConsensusCommit) manager.start();

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.start(ANY_TX_ID);
    assertThatThrownBy(() -> manager.start(ANY_TX_ID)).isInstanceOf(TransactionException.class);
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

    doThrow(CommitException.class).when(commit).commit(any());

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

    doThrow(CommitException.class).when(commit).commit(any());

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
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

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
    when(commit.abortState(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.rollback(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void rollback_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws TransactionException {
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
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void get_CrudExceptionThrownByTransactionGet_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
      get_CommitConflictExceptionThrownByTransactionCommit_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void put_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void put_MultiplePutsGiven_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void insert_ShouldInsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void upsert_ShouldUpsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void update_ShouldUpdate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void delete_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).begin();
    verify(transaction).delete(delete);
    verify(transaction).commit();
  }

  @Test
  public void delete_MultipleDeletesGiven_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }

  @Test
  public void mutate_ShouldMutate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    ConsensusCommitManager spied = spy(manager);
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
    verify(transaction).commit();
  }
}
