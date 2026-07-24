package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TwoPhaseCommitBackedGlobalTransactionTest {

  private static final String TX_ID = "tx-1";

  @Mock private TwoPhaseCommitCoordinator coordinator;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private TwoPhaseCommitBackedGlobalTransaction transaction() {
    return new TwoPhaseCommitBackedGlobalTransaction(coordinator, TX_ID);
  }

  @Test
  void getId_ShouldReturnTransactionId() {
    assertThat(transaction().getId()).isEqualTo(TX_ID);
  }

  @Test
  void commit_ShouldDelegateToCoordinatorCommit() throws Exception {
    transaction().commit();

    verify(coordinator).commit(TX_ID);
  }

  @Test
  void commit_WhenCoordinatorThrowsTransactionNotFoundException_ShouldThrowCommitConflictException()
      throws Exception {
    TransactionNotFoundException cause = new TransactionNotFoundException("expired", TX_ID);
    doThrow(cause).when(coordinator).commit(TX_ID);

    assertThatThrownBy(() -> transaction().commit())
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
  }

  @Test
  void rollback_ShouldDelegateToCoordinatorRollback() throws Exception {
    transaction().rollback();

    verify(coordinator).rollback(TX_ID);
  }
}
