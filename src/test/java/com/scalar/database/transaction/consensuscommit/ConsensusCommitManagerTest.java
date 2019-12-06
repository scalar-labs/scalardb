package com.scalar.database.transaction.consensuscommit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Isolation;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.transaction.CoordinatorException;
import com.scalar.database.transaction.consensuscommit.Coordinator.State;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitManagerTest {
  private static final String ANY_TX_ID = "any_id";
  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private RecoveryHandler recovery;
  @Mock private CommitHandler commit;
  @InjectMocks private ConsensusCommitManager manager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void start_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.start();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_SerializableGiven_ReturnConsensusCommitWithSomeTxIdAndSerializable() {
    // Arrange

    // Act
    ConsensusCommit transaction = (ConsensusCommit) manager.start(Isolation.SERIALIZABLE);

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void start_NullIsolationGiven_ThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.start((Isolation) null);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void start_CalledTwice_ReturnRespectiveConsensusCommitWithSharedCommitAndRecovery() {
    // Arrange

    // Act
    ConsensusCommit transaction1 = (ConsensusCommit) manager.start();
    ConsensusCommit transaction2 = (ConsensusCommit) manager.start();

    // Assert
    assertThat(transaction1.getCrudHandler()).isNotEqualTo(transaction2.getCrudHandler());
    assertThat(transaction1.getCrudHandler().getSnapshot().getId())
        .isNotEqualTo(transaction2.getCrudHandler().getSnapshot().getId());
    assertThat(transaction1.getCommitHandler())
        .isEqualTo(transaction2.getCommitHandler())
        .isEqualTo(commit);
    assertThat(transaction1.getRecoveryHandler())
        .isEqualTo(transaction2.getRecoveryHandler())
        .isEqualTo(recovery);
  }

  @Test
  public void check_StateReturned_ReturnTheState() throws CoordinatorException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.of(new State(ANY_TX_ID, expected)));

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void check_StateIsEmpty_ReturnUnknown() throws CoordinatorException {
    // Arrange
    when(coordinator.getState(ANY_TX_ID)).thenReturn(Optional.empty());

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }

  @Test
  public void check_CoordinatorExceptionThrown_ReturnUnknown() throws CoordinatorException {
    // Arrange
    CoordinatorException toThrow = mock(CoordinatorException.class);
    when(coordinator.getState(ANY_TX_ID)).thenThrow(toThrow);

    // Act
    TransactionState actual = manager.getState(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }
}
