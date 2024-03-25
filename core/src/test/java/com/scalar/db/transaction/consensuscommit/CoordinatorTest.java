package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Joiner;
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
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CoordinatorTest {
  private static final String ANY_ID_1 = "anyid1";
  private static final String EMPTY_CHILD_IDS = "";
  private static final long ANY_TIME_1 = 1;

  @Mock private DistributedStorage storage;
  @Mock private ConsensusCommitConfig config;
  private Coordinator coordinator;
  @Captor private ArgumentCaptor<State> stateArgumentCaptor;

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
    assertThat(state.get().getChildIds()).isEmpty();
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
    assertThat(state.get().getChildIds()).isEmpty();
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

  // For group commit

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForGroupCommitGivenAndParentIdAndChildIdMatch_ShouldReturnState(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId1 = UUID.randomUUID().toString();
    String fullId1 = keyManipulator.fullKey(parentId, childId1);
    String childId2 = UUID.randomUUID().toString();
    String fullId2 = keyManipulator.fullKey(parentId, childId2);
    List<String> childIds = Arrays.asList(childId1, childId2);

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, parentId)));
    when(resultForGroupCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, Joiner.on(',').join(childIds))));
    when(resultForGroupCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForGroupCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Assuming these states exist:
    //
    //      id   |      child_ids       |  state
    // ----------+----------------------+----------
    //  parentId | [childId1, childId2] | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childId1
    // - parentId:childId2
    when(storage.get(any(Get.class))).thenReturn(Optional.of(resultForGroupCommitState));

    // Act
    Optional<Coordinator.State> state1 = spiedCoordinator.getState(fullId1);
    Optional<Coordinator.State> state2 = spiedCoordinator.getState(fullId2);

    // Assert
    assertThat(state1).isEqualTo(state2);
    assertThat(state1.get().getId()).isEqualTo(parentId);
    assertThat(state1.get().getChildIds()).isEqualTo(childIds);
    Assertions.assertThat(state1.get().getState()).isEqualTo(transactionState);
    assertThat(state1.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId2);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForSingleCommitGivenAndFullIdMatches_ShouldReturnState(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);
    List<String> childIds = Collections.emptyList();

    Result resultForSingleCommitState = mock(Result.class);
    when(resultForSingleCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, fullId)));
    when(resultForSingleCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, EMPTY_CHILD_IDS)));
    when(resultForSingleCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForSingleCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Assuming these states exist:
    //
    //         id        | child_ids |  state
    // ------------------+-----------+----------
    //  parentId:childId |    []     | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childId
    doReturn(
            // The first get with the parent ID shouldn't find a state.
            Optional.empty(),
            // The second get with the full ID should return the state.
            Optional.of(resultForSingleCommitState))
        .when(storage)
        .get(any(Get.class));

    // Act
    Optional<Coordinator.State> state = spiedCoordinator.getState(fullId);

    // Assert
    assertThat(state.get().getId()).isEqualTo(fullId);
    assertThat(state.get().getChildIds()).isEqualTo(childIds);
    Assertions.assertThat(state.get().getState()).isEqualTo(transactionState);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForGroupCommitGivenAndOnlyParentIdMatches_ShouldReturnEmpty(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, parentId)));
    when(resultForGroupCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, Joiner.on(',').join(childIds))));
    when(resultForGroupCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForGroupCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Look up with the same parent ID and a wrong child ID.
    String targetFullId = keyManipulator.fullKey(parentId, UUID.randomUUID().toString());

    // Assuming these states exist:
    //
    //      id   |      child_ids       |  state
    // ----------+----------------------+----------
    //  parentId | [childId1, childId2] | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childIdX
    doReturn(
            // The first get with the parent ID should return a state, but it doesn't contain the
            // child ID.
            Optional.of(resultForGroupCommitState),
            // The second get with the full ID should return empty.
            Optional.empty())
        .when(storage)
        .get(any(Get.class));

    // Act
    Optional<Coordinator.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state).isEmpty();
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void
      getState_TransactionIdForSingleCommitGivenAndOnlyParentIdMatchesButFullIdMatches_ShouldReturnState(
          TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, parentId)));
    when(resultForGroupCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, Joiner.on(',').join(childIds))));
    when(resultForGroupCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForGroupCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Look up with the same parent ID and a wrong child ID.
    // But the full ID matches the single committed state.
    String targetFullId = keyManipulator.fullKey(parentId, UUID.randomUUID().toString());

    Result resultForSingleCommitState = mock(Result.class);
    when(resultForSingleCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, targetFullId)));
    when(resultForSingleCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, EMPTY_CHILD_IDS)));
    when(resultForSingleCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForSingleCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Assuming these states exist:
    //
    //          id        |      child_ids       |  state
    // -------------------+----------------------+----------
    //  parentId          | [childId1, childId2] | COMMITTED
    //  parentId:childIdX | []                   | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childIdX
    doReturn(
            // The first get with the parent ID should return a state, but it doesn't contain the
            // child ID.
            Optional.of(resultForGroupCommitState),
            // The second get with the full ID should return the state.
            Optional.of(resultForSingleCommitState))
        .when(storage)
        .get(any(Get.class));

    // Act
    Optional<Coordinator.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state.get().getId()).isEqualTo(targetFullId);
    assertThat(state.get().getChildIds()).isEmpty();
    Assertions.assertThat(state.get().getState()).isEqualTo(transactionState);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdGivenButNoIdMatches_ShouldReturnEmpty(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getValue(Attribute.ID))
        .thenReturn(Optional.of(new TextValue(Attribute.ID, parentId)));
    when(resultForGroupCommitState.getValue(Attribute.CHILD_IDS))
        .thenReturn(Optional.of(new TextValue(Attribute.CHILD_IDS, Joiner.on(',').join(childIds))));
    when(resultForGroupCommitState.getValue(Attribute.STATE))
        .thenReturn(Optional.of(new IntValue(Attribute.STATE, transactionState.get())));
    when(resultForGroupCommitState.getValue(Attribute.CREATED_AT))
        .thenReturn(Optional.of(new BigIntValue(Attribute.CREATED_AT, ANY_TIME_1)));

    // Look up with the same parent ID and a wrong child ID.
    // Also, the full ID doesn't match any single committed state.
    String targetFullId = keyManipulator.fullKey(parentId, UUID.randomUUID().toString());

    when(storage.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    Optional<Coordinator.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state).isEmpty();
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
  }

  @Test
  public void
      getState_TransactionIdForGroupCommitGivenAndFullIdMatchesAndExceptionThrownInGet_ShouldThrowCoordinatorException()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(any(Get.class))).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(() -> spiedCoordinator.getState(fullId))
        .isInstanceOf(CoordinatorException.class);
    verify(spiedCoordinator).getStateForGroupCommit(fullId);
  }

  @Test
  public void
      getState_TransactionIdForGroupCommitGivenAndParentIdMatchesAndExceptionThrownInGet_ShouldThrowCoordinatorException()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(any(Get.class)))
        //   The first get with the full ID should fail (== not found).
        .thenReturn(Optional.empty())
        //   The second (and later) gets with the parent ID should throw the exception.
        .thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(() -> spiedCoordinator.getState(fullId))
        .isInstanceOf(CoordinatorException.class);
    verify(spiedCoordinator).getStateForGroupCommit(fullId);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void putStateForGroupCommit_ParentIdGiven_ShouldPutWithCorrectValues(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId1 = UUID.randomUUID().toString();
    String childId2 = UUID.randomUUID().toString();
    List<String> fullIds =
        Arrays.asList(
            keyManipulator.fullKey(parentId, childId1), keyManipulator.fullKey(parentId, childId2));
    long current = System.currentTimeMillis();
    doNothing().when(storage).put(any(Put.class));

    // Act
    spiedCoordinator.putStateForGroupCommit(parentId, fullIds, transactionState, current);

    // Assert
    verify(spiedCoordinator).createPutWith(stateArgumentCaptor.capture());
    State state = stateArgumentCaptor.getValue();
    assertThat(state.getId()).isEqualTo(parentId);
    assertThat(state.getChildIds()).hasSize(2);
    assertThat(state.getChildIds().get(0)).isEqualTo(childId1);
    assertThat(state.getChildIds().get(1)).isEqualTo(childId2);
    assertThat(state.getState()).isEqualTo(transactionState);
    assertThat(state.getCreatedAt()).isEqualTo(current);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void putStateForGroupCommit_FullIdGiven_ShouldPutWithCorrectValuesWithEmptyChildIds(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);
    List<String> fullIds = Collections.singletonList(fullId);
    long current = System.currentTimeMillis();
    doNothing().when(storage).put(any(Put.class));

    // Act
    spiedCoordinator.putStateForGroupCommit(fullId, fullIds, transactionState, current);

    // Assert

    // With a full ID as `tx_id`, it's basically same as normal commits and `tx_child_ids` value can
    // be empty.
    verify(spiedCoordinator)
        .createPutWith(
            new Coordinator.State(fullId, Collections.emptyList(), transactionState, current));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void
      putStateForGroupCommit_StateGivenAndExceptionThrownInPut_ShouldThrowCoordinatorException(
          TransactionState transactionState) throws ExecutionException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId1 = UUID.randomUUID().toString();
    String childId2 = UUID.randomUUID().toString();
    List<String> fullIds =
        Arrays.asList(
            keyManipulator.fullKey(parentId, childId1), keyManipulator.fullKey(parentId, childId2));
    long current = System.currentTimeMillis();
    ExecutionException toThrow = mock(ExecutionException.class);
    doThrow(toThrow).when(storage).put(any(Put.class));

    // Act
    // Assert
    assertThatThrownBy(
            () ->
                spiedCoordinator.putStateForGroupCommit(
                    parentId, fullIds, transactionState, current))
        .isInstanceOf(CoordinatorException.class);
    verify(spiedCoordinator).createPutWith(stateArgumentCaptor.capture());

    State state = stateArgumentCaptor.getValue();
    assertThat(state.getId()).isEqualTo(parentId);
    assertThat(state.getChildIds()).hasSize(2);
    assertThat(state.getChildIds().get(0)).isEqualTo(childId1);
    assertThat(state.getChildIds().get(1)).isEqualTo(childId2);
    assertThat(state.getState()).isEqualTo(transactionState);
    assertThat(state.getCreatedAt()).isEqualTo(current);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void
      putStateForGroupCommit_StateWithChildIdsContainingDelimiterGiven_ShouldThrowIllegalArgumentException(
          TransactionState transactionState) throws ExecutionException {
    // Arrange
    Coordinator spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> fullIds =
        Arrays.asList(
            keyManipulator.fullKey(parentId, UUID.randomUUID().toString()),
            keyManipulator.fullKey(parentId, UUID.randomUUID().toString().replaceFirst("-", ",")));
    long current = System.currentTimeMillis();
    ExecutionException toThrow = mock(ExecutionException.class);
    doThrow(toThrow).when(storage).put(any(Put.class));

    // Act
    // Assert
    assertThatThrownBy(
            () ->
                spiedCoordinator.putStateForGroupCommit(
                    parentId, fullIds, transactionState, current))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
