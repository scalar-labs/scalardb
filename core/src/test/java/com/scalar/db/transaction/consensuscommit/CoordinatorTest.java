package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CoordinatorTest {
  private static final String ANY_ID_1 = "anyid1";
  // TODO: Test with IDs for the group commit as well.
  private static final String EMPTY_CHILD_IDS = "";
  private static final long ANY_TIME_1 = 1;

  @Mock private DistributedStorage storage;
  @Mock private ConsensusCommitConfig config;
  private Coordinator coordinator;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    coordinator = new Coordinator(storage, config);
  }

  @Test
  public void getState_TransactionIdGiven_ShouldReturnState()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Result result = mock(Result.class);
    when(result.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, ANY_ID_1)));
    when(result.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, EMPTY_CHILD_IDS)));
    when(result.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, TransactionState.COMMITTED.get())));
    when(result.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<Coordinator.State> state = coordinator.getState(ANY_ID_1);

    // Assert
    assertThat(state.get().getId()).isEqualTo(ANY_ID_1);
    // FIXME: Should be empty.
    assertThat(state.get().getChildIds()).isEqualTo(new String[] {""});
    Assertions.assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
  }

  @Test
  public void getState_TransactionIdGivenAndExceptionThrownInGet_ShouldThrowCoordinatorException()
      throws ExecutionException {
    // Arrange
    String id = ANY_ID_1;
    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(any(Get.class))).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(() -> coordinator.getState(id)).isInstanceOf(CoordinatorException.class);
  }

  @Test
  public void putState_StateGiven_ShouldPutWithCorrectValues()
      throws ExecutionException, CoordinatorException {
    // Arrange
    coordinator = spy(new Coordinator(storage, config));
    long current = System.currentTimeMillis();
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED, current);
    doNothing().when(storage).put(any(Put.class));

    // Act
    coordinator.putState(state);

    // Assert
    verify(coordinator).createPutWith(state);
  }

  @Test
  public void createPutWith_StateGiven_ShouldCreateWithCorrectValues() throws ExecutionException {
    // Arrange
    long current = System.currentTimeMillis();
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED, current);
    doNothing().when(storage).put(any(Put.class));

    // Act
    Put put = coordinator.createPutWith(state);

    // Assert
    assertThat(put.getPartitionKey().get().get(0)).isEqualTo(new TextValue(Attribute.ID, ANY_ID_1));
    assertThat(put.getColumns().get(Attribute.STATE))
        .isEqualTo(ScalarDbUtils.toColumn(Attribute.toStateValue(TransactionState.COMMITTED)));
    assertThat(put.getColumns().get(Attribute.CREATED_AT))
        .isEqualTo(ScalarDbUtils.toColumn(Attribute.toCreatedAtValue(current)));
    assertThat(put.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(put.getCondition().get()).isExactlyInstanceOf(PutIfNotExists.class);
    assertThat(put.forNamespace().get()).isEqualTo(Coordinator.NAMESPACE);
    assertThat(put.forTable().get()).isEqualTo(Coordinator.TABLE);
  }

  @Test
  public void putState_StateGivenAndExceptionThrownInPut_ShouldThrowCoordinatorException()
      throws ExecutionException {
    // Arrange
    coordinator = spy(new Coordinator(storage, config));
    long current = System.currentTimeMillis();
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED, current);
    ExecutionException toThrow = mock(ExecutionException.class);
    doThrow(toThrow).when(storage).put(any(Put.class));

    // Act
    assertThatThrownBy(() -> coordinator.putState(state)).isInstanceOf(CoordinatorException.class);

    // Assert
    verify(coordinator).createPutWith(state);
  }

  @Test
  public void getState_WithCoordinatorNamespaceChanged_ShouldGetWithChangedNamespace()
      throws ExecutionException, CoordinatorException {
    // Arrange
    when(config.getCoordinatorNamespace()).thenReturn(Optional.of("changed_coordinator"));
    coordinator = new Coordinator(storage, config);

    Result result = mock(Result.class);
    when(result.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, ANY_ID_1)));
    when(result.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, EMPTY_CHILD_IDS)));
    when(result.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, TransactionState.COMMITTED.get())));
    when(result.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<Coordinator.State> state = coordinator.getState(ANY_ID_1);

    // Assert
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(captor.getValue().forNamespace().get()).isEqualTo("changed_coordinator");
    assertThat(captor.getValue().forTable().get()).isEqualTo(Coordinator.TABLE);

    assertThat(state.get().getId()).isEqualTo(ANY_ID_1);
    // FIXME: Should be empty.
    assertThat(state.get().getChildIds()).isEqualTo(new String[] {""});
    Assertions.assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
  }

  @Test
  public void putState_WithCoordinatorNamespaceChanged_ShouldPutWithChangedNamespace()
      throws ExecutionException, CoordinatorException {
    // Arrange
    when(config.getCoordinatorNamespace()).thenReturn(Optional.of("changed_coordinator"));
    coordinator = spy(new Coordinator(storage, config));

    long current = System.currentTimeMillis();
    Coordinator.State state = new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED, current);
    doNothing().when(storage).put(any(Put.class));

    // Act
    coordinator.putState(state);

    // Assert
    verify(coordinator).createPutWith(state);

    ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(captor.capture());
    assertThat(captor.getValue().forNamespace().get()).isEqualTo("changed_coordinator");
    assertThat(captor.getValue().forTable().get()).isEqualTo(Coordinator.TABLE);
  }
}
