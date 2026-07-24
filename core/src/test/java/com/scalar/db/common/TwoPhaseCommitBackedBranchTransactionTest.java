package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
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
  void end_ShouldNotInteractWithParticipant() throws Exception {
    branch().end();

    verifyNoInteractions(participant);
  }

  @Test
  void end_CalledTwice_ShouldThrowIllegalStateException() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end();

    assertThatThrownBy(branch::end).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void crud_AfterEnd_ShouldThrowIllegalStateExceptionWithoutDelegating() throws Exception {
    TwoPhaseCommitBackedBranchTransaction branch = branch();
    branch.end();

    assertThatThrownBy(() -> branch.get(get())).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> branch.insert(insert(1))).isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(participant);
  }
}
