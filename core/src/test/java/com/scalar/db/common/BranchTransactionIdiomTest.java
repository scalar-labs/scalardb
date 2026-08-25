package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Compiles and runs the idioms published in {@link BranchTransaction}'s Javadoc.
 *
 * <p>A Javadoc code block is never compiled, so without this test a published sample can drift from
 * the API, or stop compiling, without anything noticing. The bodies below are those samples, with
 * the values a sample leaves to the reader supplied by helpers here; the control flow and the calls
 * under test are unchanged. The branch is a real {@link TwoPhaseCommitBackedBranchTransaction}
 * rather than a mock, so the {@code end} semantics the samples rely on — notably that {@code
 * FAILURE} after a successful {@code SUCCESS} is a no-op — are exercised for real.
 */
class BranchTransactionIdiomTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX_ID = "tx-1";

  @Mock private TwoPhaseCommitParticipant participant;
  @Mock private GlobalTransactionManager manager;
  @Mock private GlobalTransaction global;

  private TwoPhaseCommitBackedBranchTransaction branch;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    branch = new TwoPhaseCommitBackedBranchTransaction(participant, TX_ID);
    when(manager.beginBranch(TX_ID)).thenReturn(branch);
    when(manager.begin()).thenReturn(global);
    when(global.getId()).thenReturn(TX_ID);
  }

  private static Insert insert() {
    return Insert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", 1))
        .intValue("v", 1)
        .build();
  }

  private void failInsertWith(CrudException cause) throws Exception {
    doThrow(cause).when(participant).insert(anyString(), any(Insert.class));
  }

  // ---------------------------------------------------------------------------------------------
  // The published idioms.
  // ---------------------------------------------------------------------------------------------

  /** Idiom 1: a joining process, in a method that declares a checked exception. */
  private void joiningProcess(String transactionId) throws TransactionException {
    BranchTransaction branch = manager.beginBranch(transactionId);
    try {
      branch.insert(insert());
      branch.end(BranchTransaction.Status.SUCCESS);
    } catch (Exception e) {
      try {
        branch.end(BranchTransaction.Status.FAILURE);
      } catch (RuntimeException | CrudException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  /**
   * Idiom 2: a joining process where the enclosing method cannot declare a checked exception. The
   * absence of a {@code throws} clause here is the point of this form — do not add one.
   */
  private void joiningProcessUnchecked(BranchTransaction branch) {
    try {
      branch.insert(insert());
      branch.end(BranchTransaction.Status.SUCCESS);
    } catch (Exception e) {
      try {
        branch.end(BranchTransaction.Status.FAILURE);
      } catch (RuntimeException | CrudException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e instanceof RuntimeException ? (RuntimeException) e : new IllegalStateException(e);
    }
  }

  /** Idiom 3: a process that also drives the transaction's outcome. */
  private void initiator() throws TransactionException {
    GlobalTransaction global = manager.begin();
    BranchTransaction branch = manager.beginBranch(global.getId());
    try {
      branch.insert(insert());
      branch.end(BranchTransaction.Status.SUCCESS);
      global.commit();
    } catch (Exception e) {
      try {
        branch.end(BranchTransaction.Status.FAILURE);
      } catch (RuntimeException | CrudException suppressed) {
        e.addSuppressed(suppressed);
      }
      try {
        global.rollback();
      } catch (RollbackException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  // ---------------------------------------------------------------------------------------------

  @Test
  void joiningProcess_WhenWorkSucceeds_ShouldEndTheBranchWithSuccess() throws Exception {
    joiningProcess(TX_ID);

    verify(participant).insert(TX_ID, insert());
    // Ended with SUCCESS, so the handle rejects further work.
    assertThatThrownBy(() -> branch.insert(insert())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void joiningProcess_WhenCrudFails_ShouldRethrowTheOriginalAndEndTheBranch() throws Exception {
    CrudException cause = new CrudException("crud failed", TX_ID);
    failInsertWith(cause);

    assertThatThrownBy(() -> joiningProcess(TX_ID)).isSameAs(cause);

    assertThat(cause.getSuppressed()).isEmpty();
    // Ended with FAILURE, so the handle rejects further work.
    assertThatThrownBy(() -> branch.insert(insert())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void joiningProcess_WhenCleanupAlsoFails_ShouldRethrowTheOriginalWithCleanupSuppressed()
      throws Exception {
    CrudException cause = new CrudException("crud failed", TX_ID);
    failInsertWith(cause);
    when(manager.beginBranch(TX_ID)).thenReturn(branchWhoseFailureCleanupThrows());

    assertThatThrownBy(() -> joiningProcess(TX_ID)).isSameAs(cause);

    // The guard is what keeps the cleanup failure from displacing the original.
    assertThat(cause.getSuppressed()).hasSize(1);
    assertThat(cause.getSuppressed()[0]).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void joiningProcessUnchecked_WhenCrudFails_ShouldConvertWithoutLosingTheCause() throws Exception {
    CrudException cause = new CrudException("crud failed", TX_ID);
    failInsertWith(cause);

    assertThatThrownBy(() -> joiningProcessUnchecked(branch))
        .isInstanceOf(IllegalStateException.class)
        .hasCause(cause);
  }

  @Test
  void initiator_WhenCommitFailsAfterEnding_ShouldTreatFailureAsNoOpAndRollBack() throws Exception {
    // Must be an exception commit() actually declares, or Mockito rejects the stub.
    CommitException cause = new CommitException("commit failed", TX_ID);
    doThrow(cause).when(global).commit();

    assertThatThrownBy(this::initiator).isSameAs(cause);

    // The branch was already ended with SUCCESS, so the catch block's end(FAILURE) is a silent
    // no-op rather than a second failure that would displace the original.
    assertThat(cause.getSuppressed()).isEmpty();
    verify(global).rollback();
  }

  @Test
  void initiator_WhenRollbackAlsoFails_ShouldRethrowTheOriginalWithRollbackSuppressed()
      throws Exception {
    CrudException cause = new CrudException("crud failed", TX_ID);
    failInsertWith(cause);
    RollbackException rollbackFailure = new RollbackException("rollback failed", TX_ID);
    doThrow(rollbackFailure).when(global).rollback();

    // The rollback's own failure is cleanup too, so it is suppressed rather than allowed to
    // displace the failure that caused it.
    assertThatThrownBy(this::initiator).isSameAs(cause);

    assertThat(cause.getSuppressed()).containsExactly(rollbackFailure);
  }

  @Test
  void initiator_WhenCrudFails_ShouldEndWithFailureAndRollBackWithoutCommitting() throws Exception {
    CrudException cause = new CrudException("crud failed", TX_ID);
    failInsertWith(cause);

    assertThatThrownBy(this::initiator).isSameAs(cause);

    verify(global).rollback();
    verify(global, never()).commit();
  }

  /** A branch whose failure-path cleanup throws, to prove the idiom's guard actually suppresses. */
  private BranchTransaction branchWhoseFailureCleanupThrows() {
    return new TwoPhaseCommitBackedBranchTransaction(participant, TX_ID) {
      @Override
      public void end(Status status) throws CrudException {
        if (status == Status.FAILURE) {
          throw new IllegalStateException("cleanup failed");
        }
        super.end(status);
      }
    };
  }
}
