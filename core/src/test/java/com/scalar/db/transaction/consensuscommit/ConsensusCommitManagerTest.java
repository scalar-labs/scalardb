package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitManagerTest {
  private static final String ANY_TX_ID = "any_id";

  @Mock
  @SuppressWarnings("unused")
  private DistributedStorage storage;

  @Mock
  @SuppressWarnings("unused")
  private DistributedStorageAdmin admin;

  @Mock private ConsensusCommitConfig config;
  @Mock private Coordinator coordinator;

  @Mock
  @SuppressWarnings("unused")
  private RecoveryHandler recovery;

  @Mock private CommitHandler commit;
  @InjectMocks private ConsensusCommitManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getIsolation()).thenReturn(Isolation.SNAPSHOT);
    when(config.getSerializableStrategy()).thenReturn(SerializableStrategy.EXTRA_READ);
  }

  @Test
  public void start_NoArgumentGiven_ReturnConsensusCommitWithSomeTxIdAndSnapshotIsolation() {
    // Arrange

    // Act
    ConsensusCommit transaction = manager.start();

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SNAPSHOT);
  }

  @Test
  public void start_SerializableGiven_ReturnConsensusCommitWithSomeTxIdAndSerializable() {
    // Arrange

    // Act
    ConsensusCommit transaction = manager.start(com.scalar.db.api.Isolation.SERIALIZABLE);

    // Assert
    assertThat(transaction.getCrudHandler().getSnapshot().getId()).isNotNull();
    assertThat(transaction.getCrudHandler().getSnapshot().getIsolation())
        .isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void start_NullIsolationGiven_ThrowNullPointerExceptionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.start((com.scalar.db.api.Isolation) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void start_CalledTwice_ReturnRespectiveConsensusCommitWithSharedCommit() {
    // Arrange

    // Act
    ConsensusCommit transaction1 = manager.start();
    ConsensusCommit transaction2 = manager.start();

    // Assert
    assertThat(transaction1.getCrudHandler()).isNotEqualTo(transaction2.getCrudHandler());
    assertThat(transaction1.getCrudHandler().getSnapshot().getId())
        .isNotEqualTo(transaction2.getCrudHandler().getSnapshot().getId());
    assertThat(transaction1.getCommitHandler())
        .isEqualTo(transaction2.getCommitHandler())
        .isEqualTo(commit);
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

  @Test
  public void abort_CommitHandlerReturnsAborted_ShouldReturnTheState()
      throws UnknownTransactionStatusException {
    // Arrange
    TransactionState expected = TransactionState.ABORTED;
    when(commit.abort(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerReturnsCommitted_ShouldReturnTheState()
      throws UnknownTransactionStatusException {
    // Arrange
    TransactionState expected = TransactionState.COMMITTED;
    when(commit.abort(ANY_TX_ID)).thenReturn(expected);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void abort_CommitHandlerThrowsUnknownTransactionStatusException_ShouldReturnUnknown()
      throws UnknownTransactionStatusException {
    // Arrange
    when(commit.abort(ANY_TX_ID)).thenThrow(UnknownTransactionStatusException.class);

    // Act
    TransactionState actual = manager.abort(ANY_TX_ID);

    // Assert
    assertThat(actual).isEqualTo(TransactionState.UNKNOWN);
  }
}
