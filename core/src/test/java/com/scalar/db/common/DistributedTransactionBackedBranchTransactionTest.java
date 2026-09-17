package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction.Status;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DistributedTransactionBackedBranchTransactionTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";

  @Mock private DistributedTransaction transaction;
  @Mock private Result result;

  private DistributedTransactionBackedBranchTransaction branch;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    branch = new DistributedTransactionBackedBranchTransaction(transaction);
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
  void getId_ShouldDelegateToTransaction() {
    when(transaction.getId()).thenReturn("tx-1");

    assertThat(branch.getId()).isEqualTo("tx-1");
    verify(transaction).getId();
  }

  @Test
  void get_ShouldDelegateToTransaction() throws Exception {
    Get get = get();
    when(transaction.get(get)).thenReturn(Optional.of(result));

    Optional<Result> actual = branch.get(get);

    assertThat(actual).contains(result);
    verify(transaction).get(get);
  }

  @Test
  void insert_ShouldDelegateToTransaction() throws Exception {
    Insert insert = insert(1);

    branch.insert(insert);

    verify(transaction).insert(insert);
  }

  @Test
  void mutate_ShouldDelegateToTransaction() throws Exception {
    List<? extends Mutation> mutations = Arrays.asList(insert(1), insert(2));

    branch.mutate(mutations);

    verify(transaction).mutate(mutations);
  }

  @Test
  void end_WithSuccess_ShouldNotTouchTransaction() throws Exception {
    branch.end(Status.SUCCESS);

    verifyNoInteractions(transaction);
  }

  @Test
  void end_WithFailure_ShouldNotTouchTransaction() throws Exception {
    branch.end(Status.FAILURE);

    verifyNoInteractions(transaction);
  }

  @Test
  void end_WithSuccessTwice_ShouldThrowIllegalStateException() throws Exception {
    branch.end(Status.SUCCESS);

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithFailureAfterSuccess_ShouldBeNoOp() throws Exception {
    branch.end(Status.SUCCESS);

    branch.end(Status.FAILURE);

    // No-op means nothing reached the underlying transaction and the branch stays ended. Verify
    // the delegate first: the rejection below builds its message from transaction.getId().
    verifyNoInteractions(transaction);
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithFailureTwice_ShouldBeIdempotent() throws Exception {
    branch.end(Status.FAILURE);

    branch.end(Status.FAILURE);

    // The second call leaves the branch ended rather than resetting it. Verify the delegate first:
    // the rejection below builds its message from transaction.getId().
    verifyNoInteractions(transaction);
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithSuccessAfterFailure_ShouldThrowIllegalStateException() throws Exception {
    branch.end(Status.FAILURE);

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void end_WithNull_ShouldThrowNullPointerExceptionAndLeaveBranchUnended() throws Exception {
    assertThatThrownBy(() -> branch.end(null)).isInstanceOf(NullPointerException.class);

    // The branch was left un-ended, so it can still be ended normally.
    branch.end(Status.SUCCESS);
  }

  @Test
  void end_ShouldBeScopedToTheHandle_NotTheTransaction() throws Exception {
    DistributedTransactionBackedBranchTransaction second =
        new DistributedTransactionBackedBranchTransaction(transaction);
    branch.end(Status.SUCCESS);

    // A second handle over the same underlying transaction has its own ended state.
    second.end(Status.SUCCESS);
  }

  @Test
  void end_WithSuccessAndOpenScanner_ShouldThrowIllegalStateException_ThenSucceedAfterClose()
      throws Exception {
    TransactionCrudOperable.Scanner delegateScanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan())).thenReturn(delegateScanner);
    TransactionCrudOperable.Scanner scanner = branch.getScanner(scan());

    assertThatThrownBy(() -> branch.end(Status.SUCCESS)).isInstanceOf(IllegalStateException.class);

    scanner.close();
    verify(delegateScanner).close();
    branch.end(Status.SUCCESS);
  }

  @Test
  void end_WithFailureAndOpenScanner_ShouldNotThrowAndShouldLeaveScannerOpen() throws Exception {
    TransactionCrudOperable.Scanner delegateScanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan())).thenReturn(delegateScanner);
    branch.getScanner(scan());

    branch.end(Status.FAILURE);

    // The scanner is deliberately left open: closing it here would write into the shared snapshot's
    // scan/scanner sets, which are re-validated at commit. The owning transaction's rollback closes
    // it instead.
    verify(delegateScanner, never()).close();
    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void crud_AfterEndWithSuccess_ShouldThrowIllegalStateExceptionWithoutDelegating()
      throws Exception {
    branch.end(Status.SUCCESS);

    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> branch.insert(insert(1))).isInstanceOf(IllegalStateException.class);
    verify(transaction, never()).get(any(Get.class));
    verify(transaction, never()).insert(any(Insert.class));
  }

  @Test
  void crud_AfterEndWithFailure_ShouldThrowIllegalStateExceptionWithoutDelegating()
      throws Exception {
    branch.end(Status.FAILURE);

    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> branch.insert(insert(1))).isInstanceOf(IllegalStateException.class);
    verify(transaction, never()).get(any(Get.class));
    verify(transaction, never()).insert(any(Insert.class));
  }
}
