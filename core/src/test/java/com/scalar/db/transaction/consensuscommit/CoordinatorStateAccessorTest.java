package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Joiner;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.CoordinatorStateAccessor.State;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CoordinatorStateAccessorTest {
  private static final String ANY_ID_1 = "anyid1";
  private static final String EMPTY_CHILD_IDS = "";
  private static final long ANY_TIME_1 = 1;

  @Mock private DistributedStorage storage;
  @Mock private ConsensusCommitConfig config;
  private CoordinatorStateAccessor coordinator;
  @Captor private ArgumentCaptor<Get> getArgumentCaptor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    coordinator = new CoordinatorStateAccessor(storage, config);
  }

  @Test
  public void getState_TransactionIdGiven_ShouldReturnState()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.contains(Attribute.CHILD_IDS)).thenReturn(false);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<CoordinatorStateAccessor.State> state = coordinator.getState(ANY_ID_1);

    // Assert
    assertThat(state).isPresent();
    assertThat(state.get().getId()).isEqualTo(ANY_ID_1);
    assertThat(state.get().getChildIds()).isEmpty();
    assertThat(state.get().getChildIdsAsString()).isEmpty();
    Assertions.assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
  }

  @Test
  public void getState_TransactionIdGivenAndExceptionThrownInGet_ShouldThrowCoordinatorException()
      throws ExecutionException {
    // Arrange
    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(any(Get.class))).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(() -> coordinator.getState(ANY_ID_1))
        .isInstanceOf(CoordinatorException.class);
  }

  @Test
  public void getState_ParentIdGiven_ShouldReadParentRowDirectly()
      throws ExecutionException, CoordinatorException {
    // Arrange — parent IDs are non-full keys, so getState routes through the literal read path.
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childIdsStr =
        String.join(
            ",",
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(parentId);
    when(result.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(childIdsStr);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.ABORTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<CoordinatorStateAccessor.State> state = coordinator.getState(parentId);

    // Assert
    assertThat(state).isPresent();
    assertThat(state.get().getId()).isEqualTo(parentId);
    assertThat(state.get().getChildIds()).isEqualTo(Arrays.asList(childIdsStr.split(",")));
    assertThat(state.get().getChildIdsAsString()).isEqualTo(childIdsStr);
    Assertions.assertThat(state.get().getState()).isEqualTo(TransactionState.ABORTED);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
  }

  @Test
  public void putState_StateGiven_ShouldPutWithCorrectValues()
      throws ExecutionException, CoordinatorException {
    // Arrange
    coordinator = spy(new CoordinatorStateAccessor(storage, config));
    long current = System.currentTimeMillis();
    CoordinatorStateAccessor.State state =
        new CoordinatorStateAccessor.State(ANY_ID_1, TransactionState.COMMITTED, current);
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
    CoordinatorStateAccessor.State state =
        new CoordinatorStateAccessor.State(ANY_ID_1, TransactionState.COMMITTED, current);
    doNothing().when(storage).put(any(Put.class));

    // Act
    Put put = coordinator.createPutWith(state);

    // Assert
    assertThat(put.getPartitionKey().getColumnName(0)).isEqualTo(Attribute.ID);
    assertThat(put.getPartitionKey().getTextValue(0)).isEqualTo(ANY_ID_1);
    assertThat(put.getColumns().get(Attribute.STATE))
        .isEqualTo(IntColumn.of(Attribute.STATE, TransactionState.COMMITTED.get()));
    assertThat(put.getColumns().get(Attribute.CREATED_AT))
        .isEqualTo(BigIntColumn.of(Attribute.CREATED_AT, current));
    assertThat(put.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(put.getCondition()).isPresent();
    assertThat(put.getCondition().get()).isExactlyInstanceOf(PutIfNotExists.class);
    assertThat(put.forNamespace()).hasValue(CoordinatorStateAccessor.NAMESPACE);
    assertThat(put.forTable()).hasValue(CoordinatorStateAccessor.TABLE);
  }

  @Test
  public void putState_StateGivenAndExceptionThrownInPut_ShouldThrowCoordinatorException()
      throws ExecutionException {
    // Arrange
    coordinator = spy(new CoordinatorStateAccessor(storage, config));
    long current = System.currentTimeMillis();
    CoordinatorStateAccessor.State state =
        new CoordinatorStateAccessor.State(ANY_ID_1, TransactionState.COMMITTED, current);
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
    coordinator = new CoordinatorStateAccessor(storage, config);

    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));

    // Act
    Optional<CoordinatorStateAccessor.State> state = coordinator.getState(ANY_ID_1);

    // Assert
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(captor.capture());
    assertThat(captor.getValue().forNamespace()).hasValue("changed_coordinator");
    assertThat(captor.getValue().forTable()).hasValue(CoordinatorStateAccessor.TABLE);

    assertThat(state).isPresent();
    assertThat(state.get().getId()).isEqualTo(ANY_ID_1);
    assertThat(state.get().getChildIds()).isEmpty();
    assertThat(state.get().getChildIdsAsString()).isEmpty();
    Assertions.assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
  }

  @Test
  public void putState_WithCoordinatorNamespaceChanged_ShouldPutWithChangedNamespace()
      throws ExecutionException, CoordinatorException {
    // Arrange
    when(config.getCoordinatorNamespace()).thenReturn(Optional.of("changed_coordinator"));
    coordinator = spy(new CoordinatorStateAccessor(storage, config));

    long current = System.currentTimeMillis();
    CoordinatorStateAccessor.State state =
        new CoordinatorStateAccessor.State(ANY_ID_1, TransactionState.COMMITTED, current);
    doNothing().when(storage).put(any(Put.class));

    // Act
    coordinator.putState(state);

    // Assert
    verify(coordinator).createPutWith(state);

    ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(captor.capture());
    assertThat(captor.getValue().forNamespace()).hasValue("changed_coordinator");
    assertThat(captor.getValue().forTable()).hasValue(CoordinatorStateAccessor.TABLE);
  }

  // For group commit

  private void assertGetArgumentCaptorForGetState(
      List<Get> gets, List<String> expectedPartitionIds) {
    assertThat(gets).hasSize(expectedPartitionIds.size());
    for (int i = 0; i < gets.size(); i++) {
      Get get = gets.get(i);
      String expectedPartitionId = expectedPartitionIds.get(i);
      assertThat(get.getPartitionKey().size()).isEqualTo(1);
      assertThat(get.getPartitionKey().getTextValue(0)).isEqualTo(expectedPartitionId);
      assertThat(get.getClusteringKey()).isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForGroupCommitGivenAndParentIdAndChildIdMatch_ShouldReturnState(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId1 = UUID.randomUUID().toString();
    String fullId1 = keyManipulator.fullKey(parentId, childId1);
    String childId2 = UUID.randomUUID().toString();
    String fullId2 = keyManipulator.fullKey(parentId, childId2);
    List<String> childIds = Arrays.asList(childId1, childId2);

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS))
        .thenReturn(Joiner.on(',').join(childIds));
    when(resultForGroupCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    // Assuming these states exist:
    //
    //      id   |      child_ids       |  state
    // ----------+----------------------+----------
    //  parentId | [childId1, childId2] | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childId1
    // - parentId:childId2
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));

    // Act
    Optional<CoordinatorStateAccessor.State> state1 = spiedCoordinator.getState(fullId1);
    Optional<CoordinatorStateAccessor.State> state2 = spiedCoordinator.getState(fullId2);

    // Assert
    assertThat(state1).isEqualTo(state2);
    assertThat(state1).isPresent();
    assertThat(state1.get().getId()).isEqualTo(parentId);
    assertThat(state1.get().getChildIds()).isEqualTo(childIds);
    assertThat(state1.get().getChildIdsAsString()).isEqualTo(String.join(",", childIds));
    Assertions.assertThat(state1.get().getState()).isEqualTo(transactionState);
    assertThat(state1.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId2);
    verify(storage, times(2)).get(getArgumentCaptor.capture());
    assertGetArgumentCaptorForGetState(
        getArgumentCaptor.getAllValues(), Arrays.asList(parentId, parentId));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForSingleCommitGivenAndFullIdMatches_ShouldReturnState(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);
    List<String> childIds = Collections.emptyList();
    String dummyChildId1 = UUID.randomUUID().toString();
    String dummyChildId2 = UUID.randomUUID().toString();
    List<String> dummyChildIds = Arrays.asList(dummyChildId1, dummyChildId2);

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS))
        .thenReturn(Joiner.on(',').join(dummyChildIds));
    when(resultForGroupCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    Result resultForSingleCommitState = mock(Result.class);
    when(resultForSingleCommitState.getText(Attribute.ID)).thenReturn(fullId);
    when(resultForSingleCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForSingleCommitState.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(resultForSingleCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForSingleCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    // Assuming these states exist:
    //
    //         id        |       child_ids      |  state
    // ------------------+----------------------+----------
    //  parentId:childId | [childId1, childId2] | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childId
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));
    doReturn(Optional.of(resultForSingleCommitState))
        .when(storage)
        .get(coordinator.createGetWith(fullId));

    // Act
    Optional<CoordinatorStateAccessor.State> state = spiedCoordinator.getState(fullId);

    // Assert
    assertThat(state).isPresent();
    assertThat(state.get().getId()).isEqualTo(fullId);
    assertThat(state.get().getChildIds()).isEqualTo(childIds);
    assertThat(state.get().getChildIdsAsString()).isEqualTo(String.join(",", childIds));
    Assertions.assertThat(state.get().getState()).isEqualTo(transactionState);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(fullId);
    verify(storage, times(2)).get(getArgumentCaptor.capture());
    assertGetArgumentCaptorForGetState(
        getArgumentCaptor.getAllValues(), Arrays.asList(parentId, fullId));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdForGroupCommitGivenAndOnlyParentIdMatches_ShouldReturnEmpty(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS))
        .thenReturn(Joiner.on(',').join(childIds));
    when(resultForGroupCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

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
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));

    doReturn(Optional.empty()).when(storage).get(coordinator.createGetWith(targetFullId));

    // Act
    Optional<CoordinatorStateAccessor.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state).isEmpty();
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
    verify(storage, times(2)).get(getArgumentCaptor.capture());
    assertGetArgumentCaptorForGetState(
        getArgumentCaptor.getAllValues(), Arrays.asList(parentId, targetFullId));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void
      getState_TransactionIdForSingleCommitGivenAndOnlyParentIdMatchesButFullIdMatches_ShouldReturnState(
          TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    // Look up with the same parent ID and a wrong child ID.
    // But the full ID matches the single committed state.
    String targetFullId = keyManipulator.fullKey(parentId, UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS))
        .thenReturn(Joiner.on(',').join(childIds));
    when(resultForGroupCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    Result resultForSingleCommitState = mock(Result.class);
    when(resultForSingleCommitState.getText(Attribute.ID)).thenReturn(targetFullId);
    when(resultForSingleCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForSingleCommitState.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(resultForSingleCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForSingleCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    // Assuming these states exist:
    //
    //          id        |      child_ids       |  state
    // -------------------+----------------------+----------
    //  parentId          | [childId1, childId2] | COMMITTED
    //  parentId:childIdX | []                   | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childIdX
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));
    doReturn(Optional.of(resultForSingleCommitState))
        .when(storage)
        .get(coordinator.createGetWith(targetFullId));

    // Act
    Optional<CoordinatorStateAccessor.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state).isPresent();
    assertThat(state.get().getId()).isEqualTo(targetFullId);
    assertThat(state.get().getChildIds()).isEmpty();
    assertThat(state.get().getChildIdsAsString()).isEmpty();
    Assertions.assertThat(state.get().getState()).isEqualTo(transactionState);
    assertThat(state.get().getCreatedAt()).isEqualTo(ANY_TIME_1);
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
    verify(storage, times(2)).get(getArgumentCaptor.capture());
    assertGetArgumentCaptorForGetState(
        getArgumentCaptor.getAllValues(), Arrays.asList(parentId, targetFullId));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  public void getState_TransactionIdGivenButNoIdMatches_ShouldReturnEmpty(
      TransactionState transactionState) throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    List<String> childIds =
        Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS))
        .thenReturn(Joiner.on(',').join(childIds));
    when(resultForGroupCommitState.getInt(Attribute.STATE)).thenReturn(transactionState.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);

    // Look up with the same parent ID and a wrong child ID.
    // Also, the full ID doesn't match any single committed state.
    String targetFullId = keyManipulator.fullKey(parentId, UUID.randomUUID().toString());

    // Assuming these states exist:
    //
    //          id        |      child_ids       |  state
    // -------------------+----------------------+----------
    //  parentId          | [childId1, childId2] | COMMITTED
    //  parentId:childIdX | []                   | COMMITTED
    //
    // The IDs used to find the state are:
    // - parentId:childIdY
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));
    doReturn(Optional.empty()).when(storage).get(coordinator.createGetWith(targetFullId));

    // Act
    Optional<CoordinatorStateAccessor.State> state = spiedCoordinator.getState(targetFullId);

    // Assert
    assertThat(state).isEmpty();
    verify(spiedCoordinator).getStateForGroupCommit(targetFullId);
    verify(storage, times(2)).get(getArgumentCaptor.capture());
    assertGetArgumentCaptorForGetState(
        getArgumentCaptor.getAllValues(), Arrays.asList(parentId, targetFullId));
  }

  @Test
  public void
      getState_TransactionIdForGroupCommitGivenAndFullIdMatchesAndExceptionThrownInGet_ShouldThrowCoordinatorException()
          throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
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
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
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

  @Test
  void forceAbort_NormalIdGiven_ShouldCallPutState() throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);

    // Act
    spiedCoordinator.forceAbort(ANY_ID_1);

    // Assert
    verify(spiedCoordinator).putState(argThat(stateMatcher(ANY_ID_1, TransactionState.ABORTED)));
  }

  @Test
  void
      forceAbort_FullIdGivenWhenTransactionIsInGroupCommitWhenGroupCommitIsNotCommitted_ShouldInsertTwoRecordsWithParentIdAndFullId()
          throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    // Act
    spiedCoordinator.forceAbort(fullId);

    // Assert
    // The parent-ID conflict marker must be written before the full-ID ABORTED record. This way the
    // abort conflicts with, and wins against, an in-flight normal group commit, which writes the
    // COMMITTED state under the parent ID.
    InOrder inOrder = inOrder(spiedCoordinator);
    inOrder.verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    inOrder
        .verify(spiedCoordinator)
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @Test
  void
      forceAbort_FullIdGivenWhenTransactionIsInGroupCommitWhenGroupCommitIsCommitted_ShouldThrowCoordinatorConflictException()
          throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(
            Optional.of(
                new State(
                    parentId,
                    Collections.singletonList(ANY_ID_1),
                    null,
                    TransactionState.COMMITTED,
                    System.currentTimeMillis())))
        .when(spiedCoordinator)
        .getState(parentId);

    // Act
    assertThatThrownBy(() -> spiedCoordinator.forceAbort(fullId))
        .isInstanceOf(CoordinatorConflictException.class);

    // Assert
    verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    verify(spiedCoordinator, never())
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @Test
  void forceAbort_FullIdGivenWhenTransactionIsInGroupCommitWhenGroupCommitIsAbort_ShouldDoNothing()
      throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(
            Optional.of(
                new State(
                    parentId,
                    Collections.singletonList(ANY_ID_1),
                    null,
                    TransactionState.ABORTED,
                    System.currentTimeMillis())))
        .when(spiedCoordinator)
        .getState(parentId);

    // Act
    spiedCoordinator.forceAbort(fullId);

    // Assert
    verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    verify(spiedCoordinator, never())
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  void
      forceAbort_FullIdGivenWhenTransactionIsInDelayedGroupCommitWhenGroupCommitFinished_ShouldInsertRecordWithFullId(
          TransactionState transactionState) throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(
            Optional.of(
                new State(
                    parentId,
                    Collections.singletonList("other-id"),
                    null,
                    transactionState,
                    System.currentTimeMillis())))
        .when(spiedCoordinator)
        .getState(parentId);

    // Act
    spiedCoordinator.forceAbort(fullId);

    // Assert
    // The parent-ID conflict marker must be written before the full-ID ABORTED record (see
    // forceAbort_...WhenGroupCommitIsNotCommitted_... above).
    InOrder inOrder = inOrder(spiedCoordinator);
    inOrder.verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    inOrder
        .verify(spiedCoordinator)
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionState.class,
      names = {"COMMITTED", "ABORTED"})
  void
      forceAbort_FullIdGivenWhenTransactionIsInDelayedGroupCommitWhenGroupCommitAndDelayedGroupCommitFinished_ShouldCoordinatorConflictException(
          TransactionState transactionState) throws CoordinatorException {
    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(
            Optional.of(
                new State(
                    parentId,
                    Collections.singletonList("other-id"),
                    null,
                    transactionState,
                    System.currentTimeMillis())))
        .when(spiedCoordinator)
        .getState(parentId);
    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));

    // Act
    assertThatThrownBy(() -> spiedCoordinator.forceAbort(fullId))
        .isInstanceOf(CoordinatorConflictException.class);

    // Assert
    // The parent-ID conflict marker must be written before the full-ID ABORTED record (see
    // forceAbort_...WhenGroupCommitIsNotCommitted_... above).
    InOrder inOrder = inOrder(spiedCoordinator);
    inOrder.verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    inOrder
        .verify(spiedCoordinator)
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @Test
  void
      forceAbort_FullIdGivenWhenParentRowAlreadyCleanedUpAndNoFullIdRecord_ShouldInsertRecordWithFullId()
          throws CoordinatorException {
    // The parent-id insert conflicts because a finished group-commit row existed, but the
    // Coordinator state cleanup process removed it before we re-read it. With no full-ID record,
    // this transaction is genuinely uncommitted (e.g. a delayed group commit that never landed), so
    // we fall through to insert the full-ID ABORTED record instead of crashing with AssertionError.

    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(Optional.empty()).when(spiedCoordinator).getState(parentId);

    // Act
    spiedCoordinator.forceAbort(fullId);

    // Assert
    verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    verify(spiedCoordinator).putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @Test
  void
      forceAbort_FullIdGivenWhenParentRowAlreadyCleanedUpAndTransactionCommittedWithFullId_ShouldThrowCoordinatorConflictException()
          throws CoordinatorException {
    // The parent row was cleaned up after our parent-id insert conflicted, but this transaction
    // actually committed via a full-ID record (a delayed, standalone commit). Falling through to
    // insert the full-ID ABORTED record conflicts with that committed record, which propagates so
    // the caller re-reads the committed outcome instead of treating the transaction as aborted.

    // Arrange
    CoordinatorStateAccessor spiedCoordinator = spy(coordinator);
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String fullId = keyManipulator.fullKey(parentId, ANY_ID_1);

    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(
            argThat(
                s ->
                    s != null
                        && !new CoordinatorGroupCommitKeyManipulator().isFullKey(s.getId())
                        && s.getChildIds().isEmpty()
                        && s.getState() == TransactionState.ABORTED));
    doReturn(Optional.empty()).when(spiedCoordinator).getState(parentId);
    doThrow(CoordinatorConflictException.class)
        .when(spiedCoordinator)
        .putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));

    // Act
    assertThatThrownBy(() -> spiedCoordinator.forceAbort(fullId))
        .isInstanceOf(CoordinatorConflictException.class);

    // Assert
    verify(spiedCoordinator).putState(argThat(parentMarkerMatcher(parentId)));
    verify(spiedCoordinator).putState(argThat(stateMatcher(fullId, TransactionState.ABORTED)));
  }

  @Test
  public void state_WriteSetSerializationRoundTrip_ShouldPreserveContent()
      throws CoordinatorException {
    // Arrange — build a State that carries a populated WriteSet.
    Entry writeEntry =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName("ns")
            .setTableName("tbl")
            .setPartitionKey(
                Key.newBuilder()
                    .addColumns(
                        Column.newBuilder()
                            .setName("pk")
                            .setTextValue(Column.TextValue.newBuilder().setValue("p1")))
                    .build())
            .build();
    WriteSet originalWriteSet =
        WriteSet.newBuilder()
            .setSchemaVersion(1)
            .addEntryGroups(EntryGroup.newBuilder().addEntries(writeEntry))
            .build();
    State state =
        new State(
            ANY_ID_1, originalWriteSet, TransactionState.COMMITTED, System.currentTimeMillis());

    // Serialize via createPutWith
    Put put = coordinator.createPutWith(state);
    byte[] serializedBytes = put.getColumns().get(Attribute.WRITE_SET).getBlobValueAsBytes();
    assertThat(serializedBytes).isNotNull();

    // Deserialize via State(Result)
    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(result.contains(Attribute.WRITE_SET)).thenReturn(true);
    when(result.isNull(Attribute.WRITE_SET)).thenReturn(false);
    when(result.getBlobAsBytes(Attribute.WRITE_SET)).thenReturn(serializedBytes);
    State parsedState = new State(result);

    // Assert — round-trip preserves the WriteSet (including schema_version)
    assertThat(parsedState.getWriteSet()).isPresent();
    WriteSet parsedWriteSet = parsedState.getWriteSet().get();
    assertThat(parsedWriteSet.getSchemaVersion()).isEqualTo(1);
    assertThat(parsedWriteSet).isEqualTo(originalWriteSet);
  }

  @Test
  public void state_NullWriteSet_ShouldNotPersistColumn() {
    // Arrange — State with null writeSet (lazy-recovery abort, etc.)
    State state = new State(ANY_ID_1, TransactionState.ABORTED, System.currentTimeMillis());

    // Act
    Put put = coordinator.createPutWith(state);

    // Assert — the WRITE_SET column should not be populated.
    assertThat(put.getColumns()).doesNotContainKey(Attribute.WRITE_SET);
  }

  @Test
  public void state_WriteSetColumnAbsentFromResult_ShouldParseAsNoWriteSet()
      throws CoordinatorException {
    // Arrange — when the opt-in write-set logging config is disabled, the Coordinator table schema
    // does not include the WRITE_SET column at all. Result.contains returns false for the column,
    // which must be treated as "no info" and reduce to a null WriteSet.
    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(result.contains(Attribute.WRITE_SET)).thenReturn(false);

    // Act
    State parsedState = new State(result);

    // Assert
    assertThat(parsedState.getWriteSet()).isEmpty();
    // The parser must not have attempted to read the BLOB column when it is absent.
    verify(result, never()).isNull(Attribute.WRITE_SET);
    verify(result, never()).getBlobAsBytes(Attribute.WRITE_SET);
  }

  @Test
  public void state_WithChildIds_ShouldHoldChildIdsAndNoWriteSet() {
    // Arrange
    List<String> childIds = Arrays.asList("child-1", "child-2");

    // Act
    State state =
        new State(ANY_ID_1, childIds, null, TransactionState.COMMITTED, System.currentTimeMillis());

    // Assert
    assertThat(state.getId()).isEqualTo(ANY_ID_1);
    assertThat(state.getChildIds()).containsExactlyElementsOf(childIds);
    assertThat(state.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(state.getWriteSet()).isEmpty();
  }

  @Test
  public void state_EmptyWriteSet_ShouldPersistColumnWithNonEmptyBytes()
      throws CoordinatorException {
    // Arrange — State with an empty (but non-null) WriteSet that explicitly carries
    // schema_version, mirroring what WriteSetEncoder#encodeSingleGroupWriteSet emits for
    // read-only commits.
    WriteSet emptyWriteSet = WriteSet.newBuilder().setSchemaVersion(1).build();
    State state =
        new State(ANY_ID_1, emptyWriteSet, TransactionState.COMMITTED, System.currentTimeMillis());

    // Serialize
    Put put = coordinator.createPutWith(state);
    byte[] serializedBytes = put.getColumns().get(Attribute.WRITE_SET).getBlobValueAsBytes();
    assertThat(serializedBytes).isNotEmpty();

    // Deserialize
    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(result.contains(Attribute.WRITE_SET)).thenReturn(true);
    when(result.isNull(Attribute.WRITE_SET)).thenReturn(false);
    when(result.getBlobAsBytes(Attribute.WRITE_SET)).thenReturn(serializedBytes);
    State parsedState = new State(result);

    // Assert — empty WriteSet survives the round trip and is distinguishable from null.
    assertThat(parsedState.getWriteSet()).isPresent();
    assertThat(parsedState.getWriteSet().get().getSchemaVersion()).isEqualTo(1);
    assertThat(parsedState.getWriteSet().get().getEntryGroupsList()).isEmpty();
  }

  @Test
  public void state_CorruptWriteSetBytes_ShouldThrowCoordinatorException() {
    // Arrange — Result returns non-proto garbage bytes for tx_write_set.
    Result result = mock(Result.class);
    when(result.getText(Attribute.ID)).thenReturn(ANY_ID_1);
    when(result.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(result.contains(Attribute.WRITE_SET)).thenReturn(true);
    when(result.isNull(Attribute.WRITE_SET)).thenReturn(false);
    when(result.getBlobAsBytes(Attribute.WRITE_SET))
        .thenReturn(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});

    // Act Assert
    assertThatThrownBy(() -> new State(result)).isInstanceOf(CoordinatorException.class);
  }

  @Test
  public void state_EqualityWithDifferentWriteSet_ShouldNotBeEqual() {
    // Arrange
    WriteSet writeSet1 = WriteSet.newBuilder().setSchemaVersion(1).build();
    WriteSet writeSet2 =
        WriteSet.newBuilder()
            .setSchemaVersion(1)
            .addEntryGroups(EntryGroup.newBuilder().setChildId("child-1"))
            .build();
    long createdAt = System.currentTimeMillis();
    State stateWithNullWriteSet = new State(ANY_ID_1, TransactionState.COMMITTED, createdAt);
    State stateWithWriteSet1 =
        new State(ANY_ID_1, writeSet1, TransactionState.COMMITTED, createdAt);
    State stateWithWriteSet1Again =
        new State(ANY_ID_1, writeSet1, TransactionState.COMMITTED, createdAt);
    State stateWithWriteSet2 =
        new State(ANY_ID_1, writeSet2, TransactionState.COMMITTED, createdAt);

    // Assert
    assertThat(stateWithNullWriteSet).isNotEqualTo(stateWithWriteSet1);
    assertThat(stateWithWriteSet1).isNotEqualTo(stateWithWriteSet2);
    assertThat(stateWithWriteSet1).isEqualTo(stateWithWriteSet1Again);
    assertThat(stateWithWriteSet1.hashCode()).isEqualTo(stateWithWriteSet1Again.hashCode());
  }

  @Test
  public void state_EqualityWithDifferentCreatedAt_ShouldNotBeEqual() {
    // Arrange — same id, childIds, writeSet, state but different createdAt
    State s1 = new State(ANY_ID_1, TransactionState.COMMITTED, 100L);
    State s2 = new State(ANY_ID_1, TransactionState.COMMITTED, 200L);

    // Assert — createdAt is part of equality
    assertThat(s1).isNotEqualTo(s2);
    assertThat(s1.hashCode()).isNotEqualTo(s2.hashCode());
  }

  @Test
  public void state_EqualityWithSameCreatedAt_ShouldBeEqual() {
    // Arrange — same fields including createdAt
    long createdAt = 12345L;
    State s1 = new State(ANY_ID_1, TransactionState.COMMITTED, createdAt);
    State s2 = new State(ANY_ID_1, TransactionState.COMMITTED, createdAt);

    // Assert
    assertThat(s1).isEqualTo(s2);
    assertThat(s1.hashCode()).isEqualTo(s2.hashCode());
  }

  @Test
  public void newState_ChildIdContainingDelimiterGiven_ShouldThrowIllegalArgumentException() {
    // Act + Assert
    assertThatThrownBy(
            () ->
                new CoordinatorStateAccessor.State(
                    ANY_ID_1,
                    Collections.singletonList("child" + "," + "id"),
                    null,
                    TransactionState.COMMITTED,
                    ANY_TIME_1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void deleteState_IdGiven_ShouldDeleteWithCorrectValues()
      throws ExecutionException, CoordinatorException {
    // Arrange
    doNothing().when(storage).delete(any(Delete.class));

    // Act
    coordinator.deleteState(ANY_ID_1);

    // Assert
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    Delete delete = captor.getValue();
    assertThat(delete.forNamespace()).hasValue(CoordinatorStateAccessor.NAMESPACE);
    assertThat(delete.forTable()).hasValue(CoordinatorStateAccessor.TABLE);
    assertThat(delete.getPartitionKey().getColumnName(0)).isEqualTo(Attribute.ID);
    assertThat(delete.getPartitionKey().getTextValue(0)).isEqualTo(ANY_ID_1);
    assertThat(delete.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(delete.getCondition()).isEmpty();
  }

  @Test
  public void deleteState_WithCoordinatorNamespaceChanged_ShouldDeleteWithChangedNamespace()
      throws ExecutionException, CoordinatorException {
    // Arrange
    when(config.getCoordinatorNamespace()).thenReturn(Optional.of("changed_coordinator"));
    coordinator = new CoordinatorStateAccessor(storage, config);
    doNothing().when(storage).delete(any(Delete.class));

    // Act
    coordinator.deleteState(ANY_ID_1);

    // Assert
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    assertThat(captor.getValue().forNamespace()).hasValue("changed_coordinator");
    assertThat(captor.getValue().forTable()).hasValue(CoordinatorStateAccessor.TABLE);
  }

  @Test
  public void deleteState_ExecutionExceptionThrown_ShouldThrowCoordinatorException()
      throws ExecutionException {
    // Arrange
    ExecutionException toThrow = mock(ExecutionException.class);
    doThrow(toThrow).when(storage).delete(any(Delete.class));

    // Act + Assert
    assertThatThrownBy(() -> coordinator.deleteState(ANY_ID_1))
        .isInstanceOf(CoordinatorException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void deleteState_FullKeyGivenAndCommittedInNormalGroup_ShouldDeleteAtParentKey()
      throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    // The transaction was committed in a normal group, so its state lives under the parent key with
    // the child ID listed in tx_child_ids.
    Result resultForGroupCommitState = mock(Result.class);
    when(resultForGroupCommitState.getText(Attribute.ID)).thenReturn(parentId);
    when(resultForGroupCommitState.contains(Attribute.CHILD_IDS)).thenReturn(true);
    when(resultForGroupCommitState.getText(Attribute.CHILD_IDS)).thenReturn(childId);
    when(resultForGroupCommitState.getInt(Attribute.STATE))
        .thenReturn(TransactionState.COMMITTED.get());
    when(resultForGroupCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(resultForGroupCommitState.isNull(Attribute.WRITE_SET)).thenReturn(true);
    doReturn(Optional.of(resultForGroupCommitState))
        .when(storage)
        .get(coordinator.createGetWith(parentId));
    doNothing().when(storage).delete(any(Delete.class));

    // Act
    coordinator.deleteState(fullId);

    // Assert — the delete must target the parent key (where the row actually lives), not the full
    // key the caller passed.
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    assertThat(captor.getValue().getPartitionKey().getTextValue(0)).isEqualTo(parentId);
  }

  @Test
  public void deleteState_FullKeyGivenAndCommittedAsDelayedGroup_ShouldDeleteAtFullKey()
      throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    // The transaction was delayed-committed (or force-aborted) under its own full key. There is no
    // row under the parent key, so getState falls back to the full key.
    Result resultForSingleCommitState = mock(Result.class);
    when(resultForSingleCommitState.getText(Attribute.ID)).thenReturn(fullId);
    when(resultForSingleCommitState.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(resultForSingleCommitState.getInt(Attribute.STATE))
        .thenReturn(TransactionState.COMMITTED.get());
    when(resultForSingleCommitState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(resultForSingleCommitState.isNull(Attribute.WRITE_SET)).thenReturn(true);
    doReturn(Optional.empty()).when(storage).get(coordinator.createGetWith(parentId));
    doReturn(Optional.of(resultForSingleCommitState))
        .when(storage)
        .get(coordinator.createGetWith(fullId));
    doNothing().when(storage).delete(any(Delete.class));

    // Act
    coordinator.deleteState(fullId);

    // Assert — the delete must target the full key.
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    assertThat(captor.getValue().getPartitionKey().getTextValue(0)).isEqualTo(fullId);
  }

  @Test
  public void
      deleteState_FullKeyGivenAndDelayedCommittedWhileSiblingForceAbortedParentMarkerExists_ShouldDeleteAtFullKey()
          throws ExecutionException, CoordinatorException {
    // Scenario: a group {A, B} under parentId. Sibling A was force-aborted by full ID, which writes
    // a parent-key conflict marker (ABORTED, empty child_ids) plus a full-key ABORTED row for A.
    // The marker blocks the normal group commit, but B can still be delayed-committed under its own
    // full key. So both a parent marker row and B's full-key COMMITTED row coexist.
    //
    //      id     |  child_ids  |   state    |  write_set
    // ------------+-------------+------------+------------
    //  parentId   |     []      |  ABORTED   |    null      <- A's forceAbort parent marker
    //  fullId (B) |     []      |  COMMITTED |  present      <- B's delayed commit
    //
    // finishTransaction(fullId_B) resolves to B's full-key row and deletes it; the parent marker
    // must not misroute the delete.
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    // Parent marker row written by the sibling's forceAbort: ABORTED with empty child_ids.
    Result parentMarker = mock(Result.class);
    when(parentMarker.getText(Attribute.ID)).thenReturn(parentId);
    when(parentMarker.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(parentMarker.getInt(Attribute.STATE)).thenReturn(TransactionState.ABORTED.get());
    when(parentMarker.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(parentMarker.isNull(Attribute.WRITE_SET)).thenReturn(true);

    // B's own delayed-commit row under the full key.
    Result fullKeyState = mock(Result.class);
    when(fullKeyState.getText(Attribute.ID)).thenReturn(fullId);
    when(fullKeyState.getText(Attribute.CHILD_IDS)).thenReturn(EMPTY_CHILD_IDS);
    when(fullKeyState.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(fullKeyState.getBigInt(Attribute.CREATED_AT)).thenReturn(ANY_TIME_1);
    when(fullKeyState.isNull(Attribute.WRITE_SET)).thenReturn(true);

    doReturn(Optional.of(parentMarker)).when(storage).get(coordinator.createGetWith(parentId));
    doReturn(Optional.of(fullKeyState)).when(storage).get(coordinator.createGetWith(fullId));
    doNothing().when(storage).delete(any(Delete.class));

    // Act + Assert — getState skips the parent marker (childId not listed) and resolves to the full
    // key, so the delete must target the full key, not the parent marker.
    assertThat(coordinator.getState(fullId).map(CoordinatorStateAccessor.State::getId))
        .hasValue(fullId);
    coordinator.deleteState(fullId);

    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    assertThat(captor.getValue().getPartitionKey().getTextValue(0)).isEqualTo(fullId);
  }

  @Test
  public void deleteState_FullKeyGivenAndNoStateRowExists_ShouldDeleteAtFullKey()
      throws ExecutionException, CoordinatorException {
    // Arrange
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    String parentId = keyManipulator.generateParentKey();
    String childId = UUID.randomUUID().toString();
    String fullId = keyManipulator.fullKey(parentId, childId);

    // No row exists under either key (e.g. concurrent cleanup already removed it). getState returns
    // empty, so deleteState falls back to the literal full key and the delete is a benign no-op.
    doReturn(Optional.empty()).when(storage).get(coordinator.createGetWith(parentId));
    doReturn(Optional.empty()).when(storage).get(coordinator.createGetWith(fullId));
    doNothing().when(storage).delete(any(Delete.class));

    // Act
    coordinator.deleteState(fullId);

    // Assert
    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(storage).delete(captor.capture());
    assertThat(captor.getValue().getPartitionKey().getTextValue(0)).isEqualTo(fullId);
  }

  /**
   * Mockito matcher that compares CoordinatorStateAccessor.State by id and TransactionState only —
   * ignoring createdAt. Used to verify putState calls without relying on the production code's
   * wall-clock timestamp.
   */
  private static org.mockito.ArgumentMatcher<State> stateMatcher(
      String id, TransactionState state) {
    return actual -> actual != null && id.equals(actual.getId()) && actual.getState() == state;
  }

  // The parent-ID conflict marker written by the first step of forceAbort's two-step protocol must
  // carry empty childIds: a non-empty list would make getState route an unrelated full-key lookup
  // to this marker via getChildIds().contains(childKey).
  private static org.mockito.ArgumentMatcher<State> parentMarkerMatcher(String parentId) {
    return actual ->
        actual != null
            && parentId.equals(actual.getId())
            && actual.getState() == TransactionState.ABORTED
            && actual.getChildIds().isEmpty();
  }
}
