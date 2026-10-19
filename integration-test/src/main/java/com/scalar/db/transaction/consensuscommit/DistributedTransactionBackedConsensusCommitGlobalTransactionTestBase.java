package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionTestBase;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.DistributedTransactionBackedGlobalTransactionManager;
import com.scalar.db.common.ResumableDistributedTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.service.TransactionFactory;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Runs the {@link GlobalTransactionTestBase} corpus against the consensus-commit implementation
 * with the single-phase {@link DistributedTransactionBackedGlobalTransactionManager} backing.
 *
 * <p>This is the fully-shared deployment: every branch is served by one underlying distributed
 * transaction on a single manager. {@link #manager1} and {@link #manager2} are therefore the same
 * instance, and a branch begun on either resumes (by ID) the one transaction begun on it, through
 * the {@link ActiveTransactionManagedDistributedTransactionManager} that begins it. Contrast with
 * {@link TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase}, where two managers
 * coordinate across two participants via a shared coordinator.
 */
public abstract class DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase
    extends GlobalTransactionTestBase {

  @Override
  protected String getTestName() {
    return "global_tx_cc_sp";
  }

  @Override
  protected final Properties getProperties(String testName) {
    return getProps(testName);
  }

  protected abstract Properties getProps(String testName);

  @Override
  protected void setUpManagers() {
    ResumableDistributedTransactionManager transactionManager =
        new ActiveTransactionManagedDistributedTransactionManager(
            TransactionFactory.create(getProps(getTestName())).getTransactionManager(), -1, -1);
    // The fully-shared backing serves every branch from one underlying distributed transaction on a
    // single manager, so both handles are the same manager instance.
    manager1 = new DistributedTransactionBackedGlobalTransactionManager(transactionManager);
    manager2 = manager1;
  }

  @Test
  public void beginBranchAndRollback_AfterCommitConflicted_ShouldThrowNotFoundAndDoNothing()
      throws TransactionException {
    // Arrange
    putThenCommit(0, 0, INITIAL_BALANCE);
    GlobalTransaction global = manager1.begin();
    BranchTransaction branch = manager1.beginBranch(global.getId());
    int balance = branch.get(prepareGet(0, 0)).get().getInt(BALANCE);
    branch.put(preparePut(0, 0, balance + 100));
    branch.end(BranchTransaction.Status.SUCCESS);

    GlobalTransaction interfering = manager1.begin();
    BranchTransaction interferingBranch = manager1.beginBranch(interfering.getId());
    int interferingBalance = interferingBranch.get(prepareGet(0, 0)).get().getInt(BALANCE);
    interferingBranch.put(preparePut(0, 0, interferingBalance + 1));
    interferingBranch.end(BranchTransaction.Status.SUCCESS);
    interfering.commit();

    assertThatThrownBy(global::commit).isInstanceOf(CommitConflictException.class);

    // Act Assert
    // The failed commit has already ended the global transaction
    assertThatThrownBy(() -> manager1.beginBranch(global.getId()))
        .isInstanceOf(TransactionNotFoundException.class);
    assertThatCode(global::rollback).doesNotThrowAnyException();
  }

  @Override
  protected void tearDownManagers() {
    // manager1 and manager2 are the same instance, so close once.
    if (manager1 != null) {
      manager1.close();
    }
  }
}
