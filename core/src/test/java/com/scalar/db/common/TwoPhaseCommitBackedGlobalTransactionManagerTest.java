package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TwoPhaseCommitBackedGlobalTransactionManagerTest {

  private static final String TX_ID = "tx-1";
  private static final String CANONICAL_ID = "canonical-tx-1";

  @Mock private TwoPhaseCommitCoordinator coordinator;
  @Mock private TwoPhaseCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private TwoPhaseCommitBackedGlobalTransactionManager manager() {
    return new TwoPhaseCommitBackedGlobalTransactionManager(coordinator, participant);
  }

  private static Map<String, String> attrs(String k, String v) {
    Map<String, String> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  @Test
  void beginGlobal_ShouldDelegateToCoordinatorBeginAndReturnGlobalTransactionWithCanonicalId()
      throws Exception {
    Map<String, String> attributes = attrs("k", "v");
    when(coordinator.begin(null, false, attributes)).thenReturn(CANONICAL_ID);

    GlobalTransaction transaction = manager().beginGlobal(attributes);

    verify(coordinator).begin(null, false, attributes);
    assertThat(transaction).isInstanceOf(TwoPhaseCommitBackedGlobalTransaction.class);
    assertThat(transaction.getId()).isEqualTo(CANONICAL_ID);
  }

  @Test
  void beginGlobalReadOnly_ShouldDelegateToCoordinatorBeginWithReadOnlyTrue() throws Exception {
    Map<String, String> attributes = attrs("k", "v");
    when(coordinator.begin(null, true, attributes)).thenReturn(CANONICAL_ID);

    GlobalTransaction transaction = manager().beginGlobalReadOnly(attributes);

    verify(coordinator).begin(null, true, attributes);
    assertThat(transaction).isInstanceOf(TwoPhaseCommitBackedGlobalTransaction.class);
    assertThat(transaction.getId()).isEqualTo(CANONICAL_ID);
  }

  @Test
  void beginBranch_WithEmptyAttributes_ShouldJoinParticipantAndReturnPlainBranchTransaction()
      throws Exception {
    BranchTransaction branch = manager().beginBranch(TX_ID, Collections.emptyMap());

    verify(coordinator).joinParticipant(TX_ID, participant);
    assertThat(branch).isInstanceOf(TwoPhaseCommitBackedBranchTransaction.class);
    assertThat(branch).isNotInstanceOf(AttributePropagatingBranchTransaction.class);
    assertThat(branch.getId()).isEqualTo(TX_ID);
  }

  @Test
  void beginBranch_WithNonEmptyAttributes_ShouldReturnAttributePropagatingBranchTransaction()
      throws Exception {
    BranchTransaction branch = manager().beginBranch(TX_ID, attrs("k", "v"));

    verify(coordinator).joinParticipant(TX_ID, participant);
    assertThat(branch).isInstanceOf(AttributePropagatingBranchTransaction.class);
  }

  @Test
  void beginBranch_WhenParticipantIsNull_ShouldThrowUnsupportedOperationException() {
    TwoPhaseCommitBackedGlobalTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedGlobalTransactionManager(coordinator, null);

    assertThatThrownBy(() -> coordinatorOnlyManager.beginBranch(TX_ID, Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void close_ShouldCloseCoordinatorAndParticipant() {
    manager().close();

    verify(coordinator).close();
    verify(participant).close();
  }

  @Test
  void close_WhenParticipantIsNull_ShouldCloseOnlyCoordinator() {
    TwoPhaseCommitBackedGlobalTransactionManager coordinatorOnlyManager =
        new TwoPhaseCommitBackedGlobalTransactionManager(coordinator, null);

    coordinatorOnlyManager.close();

    verify(coordinator).close();
    verifyNoInteractions(participant);
  }

  @Test
  void close_WhenClosingCoordinatorThrows_ShouldStillCloseParticipant() {
    RuntimeException exception = new RuntimeException("closing failed");
    doThrow(exception).when(coordinator).close();

    assertThatThrownBy(() -> manager().close()).isSameAs(exception);

    verify(participant).close();
  }
}
