package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.io.Key;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TwoPhaseCommitBackedDistributedTransactionManagerTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String CANONICAL_ID = "canonical-1";

  @Mock private DatabaseConfig config;
  @Mock private TwoPhaseCommitCoordinator coordinator;
  @Mock private TwoPhaseCommitParticipant participant;

  private TwoPhaseCommitBackedDistributedTransactionManager manager;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.getDefaultNamespaceName()).thenReturn(Optional.empty());
    when(coordinator.begin(any(), anyBoolean(), anyMap(), any())).thenReturn(CANONICAL_ID);
    manager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, participant);
  }

  @Test
  void begin_ShouldBeginViaCoordinatorWithParticipantAndReturnCanonicalId() throws Exception {
    // Act
    DistributedTransaction transaction = manager.begin();

    // Assert — the configured participant is passed to begin so the Coordinator registers it (the
    // Coordinator owns registration and its cleanup-on-failure).
    verify(coordinator).begin(any(), eq(false), anyMap(), eq(participant));
    assertThat(transaction.getId()).isEqualTo(CANONICAL_ID);
  }

  @Test
  void beginReadOnly_ShouldBeginReadOnlyViaCoordinatorWithParticipant() throws Exception {
    manager.beginReadOnly();
    verify(coordinator).begin(any(), eq(true), anyMap(), eq(participant));
  }

  @Test
  void transactionCrud_ShouldDelegateToParticipantWithCanonicalId() throws Exception {
    // Arrange
    DistributedTransaction transaction = manager.begin();
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 10)
            .build();
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    when(participant.get(eq(CANONICAL_ID), any(Get.class))).thenReturn(Optional.empty());

    // Act
    transaction.put(put);
    transaction.get(get);

    // Assert — CRUD is routed to the participant keyed by the canonical transaction ID.
    verify(participant).put(eq(CANONICAL_ID), any(Put.class));
    verify(participant).get(eq(CANONICAL_ID), any(Get.class));
  }

  @Test
  void transactionCrud_WhenParticipantThrowsTransactionNotFound_ShouldThrowCrudConflict()
      throws Exception {
    // Arrange — the transaction body's CRUD (not begin) hits a participant that no longer knows the
    // transaction, e.g. it expired mid-transaction.
    DistributedTransaction transaction = manager.begin();
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    doThrow(new TransactionNotFoundException("expired", CANONICAL_ID))
        .when(participant)
        .get(eq(CANONICAL_ID), any(Get.class));

    // Act Assert — surfaced as a retriable CrudConflictException so the caller restarts the
    // transaction, not as a non-retriable CrudException.
    assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  void batch_WhenAllSelections_ShouldBeginReadOnly() throws Exception {
    // Arrange
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    when(participant.batch(eq(CANONICAL_ID), any())).thenReturn(java.util.Collections.emptyList());

    // Act
    manager.batch(java.util.Collections.singletonList(get));

    // Assert — an all-Selection batch begins the transaction read-only, mirroring
    // ConsensusCommitManager.
    verify(coordinator).begin(any(), eq(true), anyMap(), eq(participant));
  }

  @Test
  void batch_WhenContainsMutation_ShouldBeginWritable() throws Exception {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 10)
            .build();
    when(participant.batch(eq(CANONICAL_ID), any())).thenReturn(java.util.Collections.emptyList());

    // Act
    manager.batch(java.util.Collections.singletonList(put));

    // Assert — a write-bearing batch begins a writable transaction.
    verify(coordinator).begin(any(), eq(false), anyMap(), eq(participant));
  }

  @Test
  void commit_ShouldDriveCoordinatorCommit() throws Exception {
    DistributedTransaction transaction = manager.begin();
    transaction.commit();
    verify(coordinator).commit(CANONICAL_ID);
  }

  @Test
  void commit_WhenCoordinatorThrowsCommitConflict_ShouldPropagate() throws Exception {
    DistributedTransaction transaction = manager.begin();
    // The coordinator already collapses prepare/validate failures into a commit-level exception, so
    // the facade just propagates a CommitConflictException unchanged.
    doThrow(new CommitConflictException("conflict", CANONICAL_ID))
        .when(coordinator)
        .commit(anyString());

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void commit_WhenCoordinatorThrowsTransactionNotFound_ShouldThrowCommitConflict()
      throws Exception {
    DistributedTransaction transaction = manager.begin();
    doThrow(new TransactionNotFoundException("not found", CANONICAL_ID))
        .when(coordinator)
        .commit(anyString());

    // A coordinator that no longer knows the transaction (e.g., it expired) surfaces as a retriable
    // commit conflict rather than an unrecoverable error.
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void oneOperationCrud_WhenBeginThrowsTransactionNotFound_ShouldThrowCrudConflict()
      throws Exception {
    // Arrange — a transient begin failure on the one-operation CRUD path.
    doThrow(new TransactionNotFoundException("transient begin failure", null))
        .when(coordinator)
        .begin(any(), anyBoolean(), anyMap(), any());
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    // Act Assert — surfaced as a retriable CRUD conflict, not a non-retriable CrudException.
    assertThatThrownBy(() -> manager.get(get)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  void getScanner_WhenBeginThrowsTransactionNotFound_ShouldThrowCrudConflict() throws Exception {
    // Arrange — a transient begin failure on the one-operation scan path.
    doThrow(new TransactionNotFoundException("transient begin failure", null))
        .when(coordinator)
        .begin(any(), anyBoolean(), anyMap(), any());
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    // Act Assert — surfaced as a retriable CRUD conflict.
    assertThatThrownBy(() -> manager.getScanner(scan)).isInstanceOf(CrudConflictException.class);
  }

  @Test
  void rollback_ShouldDriveCoordinatorRollback() throws Exception {
    DistributedTransaction transaction = manager.begin();
    transaction.rollback();
    verify(coordinator).rollback(CANONICAL_ID);
  }

  @Test
  void rollbackByTransactionId_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.rollback("tx-1"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void abortByTransactionId_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.abort("tx-1"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getState_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.getState("tx-1"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void finishTransaction_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.finishTransaction("tx-1"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void recoverRecord_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.recoverRecord(NS, TBL, Key.ofInt("pk", 1), null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void resume_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> manager.resume("tx-1"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ---- join: register the participant into a transaction begun elsewhere; CRUD-only ----

  @Test
  void join_ShouldRegisterParticipantAndReturnJoinedTransaction() throws Exception {
    // Act
    DistributedTransaction transaction = manager.join(CANONICAL_ID);

    // Assert — the participant is registered into the existing transaction.
    verify(coordinator).registerParticipant(CANONICAL_ID, participant);
    assertThat(transaction.getId()).isEqualTo(CANONICAL_ID);
  }

  @Test
  void join_WhenCoordinatorThrowsTransactionNotFound_ShouldPropagate() throws Exception {
    doThrow(new TransactionNotFoundException("not found", CANONICAL_ID))
        .when(coordinator)
        .registerParticipant(CANONICAL_ID, participant);

    assertThatThrownBy(() -> manager.join(CANONICAL_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void join_WhenRegisterParticipantFails_ShouldPropagateTransactionException() throws Exception {
    // A non-not-found registration failure propagates as-is, no longer masked as not-found (join
    // now allows TransactionException).
    doThrow(new TransactionException("join failed", CANONICAL_ID))
        .when(coordinator)
        .registerParticipant(CANONICAL_ID, participant);

    assertThatThrownBy(() -> manager.join(CANONICAL_ID))
        .isInstanceOf(TransactionException.class)
        .isNotInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  void joinedTransactionCrud_ShouldDelegateToParticipant() throws Exception {
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    when(participant.get(eq(CANONICAL_ID), any(Get.class))).thenReturn(Optional.empty());

    DistributedTransaction transaction = manager.join(CANONICAL_ID);
    transaction.get(get);

    // A joined transaction participates in CRUD, keyed by the joined transaction ID.
    verify(participant).get(eq(CANONICAL_ID), any(Get.class));
  }

  @Test
  void joinedTransactionCommitAndRollback_ShouldDelegateToCoordinator() throws Exception {
    // A joined transaction behaves the same as a begun one: commit/rollback drive the coordinator.
    manager.join(CANONICAL_ID).commit();
    verify(coordinator).commit(CANONICAL_ID);

    manager.join(CANONICAL_ID).rollback();
    verify(coordinator).rollback(CANONICAL_ID);
  }

  @Test
  void join_WhenNoParticipant_ShouldThrowUnsupportedOperationException() {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);

    assertThatThrownBy(() -> coordinatorOnlyManager.join(CANONICAL_ID))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ---- Coordinator-only manager (no participant): CRUD unsupported, lifecycle still works ----

  @Test
  void begin_WhenNoParticipant_ShouldBeginWithNullParticipant() throws Exception {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);

    DistributedTransaction transaction = coordinatorOnlyManager.begin();

    // begin works, and a null participant is passed through (nothing to register).
    assertThat(transaction.getId()).isEqualTo(CANONICAL_ID);
    verify(coordinator).begin(any(), anyBoolean(), anyMap(), isNull());
  }

  @Test
  void oneOperationCrud_WhenNoParticipant_ShouldThrowUnsupportedOperationException() {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    assertThatThrownBy(() -> coordinatorOnlyManager.get(get))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getScanner_WhenNoParticipant_ShouldThrowUnsupportedOperationException() {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    assertThatThrownBy(() -> coordinatorOnlyManager.getScanner(scan))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void transactionCrud_WhenNoParticipant_ShouldThrowUnsupportedOperationException()
      throws Exception {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);
    DistributedTransaction transaction = coordinatorOnlyManager.begin();
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    assertThatThrownBy(() -> transaction.get(get))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void transactionCommitAndRollback_WhenNoParticipant_ShouldStillWork() throws Exception {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);

    // commit and rollback are Coordinator-only, so they work without a participant.
    coordinatorOnlyManager.begin().commit();
    verify(coordinator).commit(CANONICAL_ID);

    coordinatorOnlyManager.begin().rollback();
    verify(coordinator).rollback(CANONICAL_ID);
  }

  @Test
  void close_ShouldCloseCoordinatorAndParticipant() {
    manager.close();

    verify(coordinator).close();
    verify(participant).close();
  }

  @Test
  void close_WhenNoParticipant_ShouldCloseCoordinatorOnlyWithoutThrowing() {
    TwoPhaseCommitBackedDistributedTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedDistributedTransactionManager(config, coordinator, null);

    // close must not dereference the null participant.
    coordinatorOnlyManager.close();

    verify(coordinator).close();
  }
}
