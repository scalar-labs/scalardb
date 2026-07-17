package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
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
  void end_ShouldBeNoOpAndNotTouchTransaction() throws Exception {
    branch.end();

    verifyNoInteractions(transaction);
  }
}
