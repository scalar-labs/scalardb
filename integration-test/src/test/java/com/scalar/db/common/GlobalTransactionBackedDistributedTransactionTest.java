package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Covers how the adapter ends its branch on each terminal.
 *
 * <p>The branch and the global transaction are mocked here so the two terminals' error handling can
 * be driven directly; {@link GlobalTransactionBackedDistributedTransactionManagerTest} exercises
 * the same adapter over a real branch.
 */
class GlobalTransactionBackedDistributedTransactionTest {

  private static final String TX_ID = "tx-1";

  @Mock private GlobalTransaction global;
  @Mock private BranchTransaction branch;

  private GlobalTransactionBackedDistributedTransaction transaction;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(global.getId()).thenReturn(TX_ID);
    transaction = new GlobalTransactionBackedDistributedTransaction(global, branch);
  }

  @Test
  void commit_ShouldEndTheBranchWithSuccessBeforeCommitting() throws Exception {
    transaction.commit();

    InOrder inOrder = inOrder(branch, global);
    inOrder.verify(branch).end(BranchTransaction.Status.SUCCESS);
    inOrder.verify(global).commit();
  }

  @Test
  void commit_WhenEndingTheBranchConflicts_ShouldThrowCommitConflictAndNotCommit()
      throws Exception {
    CrudConflictException cause = new CrudConflictException("conflict", TX_ID);
    doThrow(cause).when(branch).end(BranchTransaction.Status.SUCCESS);

    // A conflict while ending stays retriable for the caller.
    assertThatThrownBy(transaction::commit)
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(global, never()).commit();
  }

  @Test
  void commit_WhenEndingTheBranchFails_ShouldThrowCommitExceptionAndNotCommit() throws Exception {
    CrudException cause = new CrudException("failed", TX_ID);
    doThrow(cause).when(branch).end(BranchTransaction.Status.SUCCESS);

    assertThatThrownBy(transaction::commit)
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(global, never()).commit();
  }

  @Test
  void rollback_ShouldEndTheBranchWithFailureBeforeRollingBack() throws Exception {
    transaction.rollback();

    InOrder inOrder = inOrder(branch, global);
    inOrder.verify(branch).end(BranchTransaction.Status.FAILURE);
    inOrder.verify(global).rollback();
  }

  @Test
  void rollback_WhenEndingTheBranchFails_ShouldStillRollBackAndNotPropagate() throws Exception {
    doThrow(new CrudException("failed", TX_ID)).when(branch).end(BranchTransaction.Status.FAILURE);

    // A failure while ending must never strand an in-flight transaction, so it is logged and the
    // rollback still runs.
    assertThatCode(transaction::rollback).doesNotThrowAnyException();
    verify(global).rollback();
  }
}
