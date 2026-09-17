package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction.Status;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TwoPhaseCommitBackedBranchTransactionTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX_ID = "tx-1";

  @Mock private TwoPhaseCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private TwoPhaseCommitBackedBranchTransaction branch() {
    return new TwoPhaseCommitBackedBranchTransaction(participant, TX_ID);
  }

  private static Scan scan() {
    return Scan.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Get get() {
    return Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Insert insert(int pk) {
    return Insert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", pk))
        .intValue("v", 1)
        .build();
  }

  @Test
  void getId_ShouldReturnTransactionId() {
    assertThat(branch().getId()).isEqualTo(TX_ID);
  }

  @Test
  void get_ShouldDelegateToParticipantGet() throws Exception {
    Get get = get();
    when(participant.get(TX_ID, get)).thenReturn(Optional.empty());

    Optional<?> result = branch().get(get);

    verify(participant).get(TX_ID, get);
    assertThat(result).isEmpty();
  }

  @Test
  void insert_ShouldDelegateToParticipantInsert() throws Exception {
    Insert insert = insert(1);

    branch().insert(insert);

    verify(participant).insert(TX_ID, insert);
  }

  @Test
  void mutate_ShouldDelegateToParticipantMutate() throws Exception {
    List<? extends Mutation> mutations = Arrays.asList(insert(1), insert(2));

    branch().mutate(mutations);

    verify(participant).mutate(TX_ID, mutations);
  }

  @Test
  void get_WhenParticipantThrowsTransactionNotFoundException_ShouldThrowCrudConflictException()
      throws Exception {
    Get get = get();
    TransactionNotFoundException cause = new TransactionNotFoundException("expired", TX_ID);
    when(participant.get(TX_ID, get)).thenThrow(cause);

    assertThatThrownBy(() -> branch().get(get))
        .isInstanceOf(CrudConflictException.class)
        .hasCause(cause);
  }

  @Test
  void end_WithSuccess_ShouldNotInteractWithParticipant() throws Exception {
    branch().end(Status.SUCCESS);

    verifyNoInteractions(participant);
  }

  @Test
  void end_WithFailure_ShouldNotInteractWithParticipant() throws Exception {
    branch().end(Status.FAILURE);

    verifyNoInteractions(participant);
  }

  @Test
  void end_WithSuccessTwice_ShouldThrowIllegalStateException() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.SUCCESS);

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithFailureAfterSuccess_ShouldBeNoOp() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.SUCCESS);

    branch.end(Status.FAILURE);

    // No-op means the branch stays ended and nothing reached the participant.
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(participant);
  }

  @Test
  void end_WithFailureTwice_ShouldBeIdempotent() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.FAILURE);

    branch.end(Status.FAILURE);

    // The second call leaves the branch ended rather than resetting it.
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(participant);
  }

  @Test
  void end_WithSuccessAfterFailure_ShouldThrowIllegalStateException() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.FAILURE);

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithNull_ShouldThrowNullPointerExceptionAndLeaveBranchUnended() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();

    assertThatThrownBy(() -> branch.end(null)).isInstanceOf(NullPointerException.class);

    // The branch was left un-ended, so it can still be ended normally.
    branch.end(Status.SUCCESS);
  }

  @Test
  void end_ShouldBeScopedToTheHandle_NotTheTransaction() throws Exception {
    TwoPhaseCommitBackedBranchTransaction first = branch();
    TwoPhaseCommitBackedBranchTransaction second = branch();
    first.end(Status.SUCCESS);

    // A second handle for the same transaction has its own ended state.
    second.end(Status.SUCCESS);
  }

  @Test
  void end_WithSuccessAndOpenScanner_ShouldThrowIllegalStateException_ThenSucceedAfterClose()
      throws Exception {
    TransactionCrudOperable.Scanner delegateScanner = mock(TransactionCrudOperable.Scanner.class);
    when(participant.getScanner(TX_ID, scan())).thenReturn(delegateScanner);
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    TransactionCrudOperable.Scanner scanner = branch.getScanner(scan());

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);

    scanner.close();
    verify(delegateScanner).close();
    branch.end(Status.SUCCESS);
  }

  @Test
  void end_WithFailureAndOpenScanner_ShouldNotThrowAndShouldLeaveScannerOpen() throws Exception {
    TransactionCrudOperable.Scanner delegateScanner = mock(TransactionCrudOperable.Scanner.class);
    when(participant.getScanner(TX_ID, scan())).thenReturn(delegateScanner);
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.getScanner(scan());

    branch.end(Status.FAILURE);

    // The scanner is deliberately left open: closing it here would write into the snapshot's
    // scan/scanner sets, which are re-validated at commit. The owning transaction's rollback
    // closes it instead.
    verify(delegateScanner, never()).close();
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void crud_AfterEndWithSuccess_ShouldThrowIllegalStateExceptionWithoutDelegating()
      throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.SUCCESS);

    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> branch.insert(insert(1))).isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(participant);
  }

  @Test
  void crud_AfterEndWithFailure_ShouldThrowIllegalStateExceptionWithoutDelegating()
      throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end(Status.FAILURE);

    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> branch.insert(insert(1))).isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(participant);
  }
}
