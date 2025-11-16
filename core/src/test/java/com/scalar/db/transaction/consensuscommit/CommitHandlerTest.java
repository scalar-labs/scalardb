package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CommitHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TEXT_5 = "text5";
  private static final String ANY_TEXT_6 = "text6";
  private static final String ANY_ID = "id";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.TEXT)
              .addColumn(ANY_NAME_4, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .addSecondaryIndex(ANY_NAME_4)
              .build());

  @Mock protected DistributedStorage storage;
  @Mock protected Coordinator coordinator;
  @Mock protected TransactionTableMetadataManager tableMetadataManager;
  @Mock protected StorageInfoProvider storageInfoProvider;
  @Mock protected ConsensusCommitConfig config;
  @Mock protected BeforePreparationHook beforePreparationHook;
  @Mock protected Future<Void> beforePreparationHookFuture;

  private CommitHandler handler;
  protected ParallelExecutor parallelExecutor;
  protected MutationsGrouper mutationsGrouper;

  protected String anyId() {
    return ANY_ID;
  }

  protected void extraInitialize() {}

  protected void extraCleanup() {}

  protected CommitHandler createCommitHandler(boolean coordinatorWriteOmissionOnReadOnlyEnabled) {
    return new CommitHandler(
        storage,
        coordinator,
        tableMetadataManager,
        parallelExecutor,
        new MutationsGrouper(storageInfoProvider),
        coordinatorWriteOmissionOnReadOnlyEnabled,
        false);
  }

  protected CommitHandler createCommitHandlerWithOnePhaseCommit() {
    return new CommitHandler(
        storage, coordinator, tableMetadataManager, parallelExecutor, mutationsGrouper, true, true);
  }

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    parallelExecutor = new ParallelExecutor(config);
    handler = spy(createCommitHandler(true));
    mutationsGrouper = spy(new MutationsGrouper(storageInfoProvider));

    extraInitialize();

    // Arrange
    when(storageInfoProvider.getStorageInfo(ANY_NAMESPACE_NAME))
        .thenReturn(
            new StorageInfoImpl(
                "storage1", StorageInfo.MutationAtomicityUnit.PARTITION, Integer.MAX_VALUE, false));
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    when(tableMetadataManager.getTransactionTableMetadata(any(), any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  @AfterEach
  void tearDown() {
    extraCleanup();

    parallelExecutor.close();
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_4);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Get prepareAnotherGet() {
    Key partitionKey = Key.ofText(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = Key.ofText(ANY_NAME_6, ANY_TEXT_6);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Scan prepareScan() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .start(clusteringKey)
        .build();
  }

  private Get prepareGetWithIndex() {
    Key indexKey = Key.ofText(ANY_NAME_4, ANY_TEXT_1);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .indexKey(indexKey)
        .build();
  }

  private Scan prepareScanWithLimit(int limit) {
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .limit(limit)
        .build();
  }

  private Scan prepareScanWithIndex() {
    Key indexKey = Key.ofText(ANY_NAME_4, ANY_TEXT_1);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .indexKey(indexKey)
        .build();
  }

  private Put preparePut() {
    return preparePut(ANY_TEXT_1, ANY_TEXT_2);
  }

  private Put preparePut(String partitionKeyColumnValue, String clusteringKeyColumnValue) {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, partitionKeyColumnValue))
        .clusteringKey(Key.ofText(ANY_NAME_2, clusteringKeyColumnValue))
        .textValue(ANY_NAME_3, ANY_TEXT_3)
        .textValue(ANY_NAME_4, ANY_TEXT_4)
        .build();
  }

  private Put preparePut1() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Put preparePut2() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_3);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_4);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Put preparePut3() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_3);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Delete prepareDelete() {
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .build();
  }

  private TransactionResult prepareResult(String txId) {
    return prepareResult(txId, ANY_TEXT_1, ANY_TEXT_2);
  }

  private TransactionResult prepareResult(
      String txId, String partitionKeyColumnValue, String clusteringKeyColumnValue) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, partitionKeyColumnValue))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, clusteringKeyColumnValue))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, txId))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult prepareResultWithNullMetadata() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.ofNull(Attribute.ID))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private Snapshot prepareSnapshotWithDifferentPartitionPut() {
    Snapshot snapshot = prepareSnapshot();

    // different partition
    Put put1 = preparePut1();
    Put put2 = preparePut2();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put2), put2);

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  private Snapshot prepareSnapshotWithSamePartitionPut() {
    Snapshot snapshot = prepareSnapshot();

    // same partition
    Put put1 = preparePut1();
    Put put3 = preparePut3();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put3), put3);

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  private Snapshot prepareSnapshotWithoutWrites() {
    Snapshot snapshot = prepareSnapshot();

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  private Snapshot prepareSnapshotWithoutReads() {
    Snapshot snapshot = prepareSnapshot();

    // same partition
    Put put1 = preparePut1();
    Put put3 = preparePut3();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put3), put3);

    return snapshot;
  }

  private Snapshot prepareSnapshot() {
    return new Snapshot(anyId(), tableMetadataManager);
  }

  private void setBeforePreparationHookIfNeeded(boolean withBeforePreparationHook) {
    if (withBeforePreparationHook) {
      doReturn(beforePreparationHookFuture).when(beforePreparationHook).handle(any(), any());
      handler.setBeforePreparationHook(beforePreparationHook);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_ShouldNotPrepareRecordsAndAbortStateAndRollbackRecords(
          boolean withBeforePreparationHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doThrow(ValidationConflictException.class).when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndRollbackRecordsButShouldAbortState(
          boolean withBeforePreparationHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doThrow(ValidationConflictException.class).when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(handler).toSerializable(any(), eq(anyId()));
    verify(coordinator).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_NoReadsInSnapshot_ShouldNotValidateRecords(boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutReads());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doNothing().when(handler).toSerializable(any(), anyString());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(handler, never()).toSerializable(any(), eq(anyId()));
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_NoMutationExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_RetriableExecutionExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ExceptionThrownInPrepareRecords_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ValidationConflictExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ValidationConflictException.class).when(handler).toSerializable(any(), anyString());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(handler).toSerializable(any(), anyString());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(handler).toSerializable(any(), anyString());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(handler).toSerializable(any(), anyString());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(handler).toSerializable(any(), anyString());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(handler).toSerializable(any(), anyString());
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenCommittedReturnedInGetState_ShouldBeIgnored()
          throws ExecutionException, CoordinatorException, CommitException,
              UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_ExceptionThrownInCoordinatorCommit_ShouldThrowUnknown()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_BeforePreparationHookGiven_ShouldWaitBeforePreparationHookFinishesBeforeCommitState()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);
    // Lambda can't be spied...
    BeforePreparationHook delayedBeforePreparationHook =
        spy(
            new BeforePreparationHook() {
              @Override
              public Future<Void> handle(
                  TransactionTableMetadataManager tableMetadataManager,
                  TransactionContext context) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(2));
                return beforePreparationHookFuture;
              }
            });
    handler.setBeforePreparationHook(delayedBeforePreparationHook);
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Instant start = Instant.now();
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);
    Instant end = Instant.now();

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(context);
    verify(delayedBeforePreparationHook).handle(tableMetadataManager, context);
    // This means `commit()` waited until the callback was completed before throwing
    // an exception from `commitState()`.
    assertThat(Duration.between(start, end)).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingBeforePreparationHookGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingBeforePreparationHookFutureGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
          InterruptedException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshot();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenValidationRequired_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    when(validationInfo.isActuallyValidationRequired()).thenReturn(true);
    Snapshot snapshot = prepareSnapshot();
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    Snapshot snapshot = prepareSnapshotWithoutWrites();
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse()
      throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    Snapshot snapshot = prepareSnapshot();

    // Setup a delete with no corresponding record in read set
    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.empty());

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenMutationsCanBeGrouped_ShouldReturnTrue() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doReturn(true).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isTrue();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doReturn(false).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(validationInfo, context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void
      canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException()
          throws ExecutionException {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    CommitHandler.ValidationInfo validationInfo = mock(CommitHandler.ValidationInfo.class);
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doThrow(ExecutionException.class).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.canOnePhaseCommit(validationInfo, context))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations()
      throws CommitConflictException, UnknownTransactionStatusException, ExecutionException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.onePhaseCommitRecords(context);

    // Assert
    verify(storage).mutate(anyList());
    verify(snapshot).to(any(OnePhaseCommitMutationComposer.class));
  }

  @Test
  public void
      onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  public void
      onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void commit_OnePhaseCommitted_ShouldNotThrowAnyException()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true)
        .when(handler)
        .canOnePhaseCommit(any(CommitHandler.ValidationInfo.class), any(TransactionContext.class));
    doNothing().when(handler).onePhaseCommitRecords(any(TransactionContext.class));

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(handler).canOnePhaseCommit(new CommitHandler.ValidationInfo(), context);
    verify(handler).onePhaseCommitRecords(context);
  }

  @Test
  public void
      commit_OnePhaseCommitted_UnknownTransactionStatusExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws CommitException, UnknownTransactionStatusException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true)
        .when(handler)
        .canOnePhaseCommit(any(CommitHandler.ValidationInfo.class), any(TransactionContext.class));
    doThrow(UnknownTransactionStatusException.class)
        .when(handler)
        .onePhaseCommitRecords(any(TransactionContext.class));

    TransactionContext context =
        new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void toSerializable_ReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(anyId());
    TransactionResult txResult = new TransactionResult(result);
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(get, Optional.of(txResult));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(put)));

    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ReadSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(anyId());
    TransactionResult changedTxResult = prepareResult(anyId() + "x");
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(get, Optional.of(txResult));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(put)));

    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(changedTxResult));

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ReadSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Get get = prepareAnotherGet();
    Put put = preparePut();
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(get, Optional.empty());
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(put)));

    TransactionResult txResult = prepareResult(anyId());
    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_GetSetWithGetWithIndex_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult txResult = prepareResult(anyId() + "x");
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(getWithIndex, Optional.of(txResult));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptySet());

    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetSetWithGetWithIndex_RecordInserted_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "xx", ANY_TEXT_1, ANY_TEXT_3);
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(getWithIndex, Optional.of(result1));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptySet());

    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetSetWithGetWithIndex_RecordInsertedByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId(), ANY_TEXT_1, ANY_TEXT_3);
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(getWithIndex, Optional.of(result1));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut(ANY_TEXT_1, ANY_TEXT_3))));

    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(anyId() + "x");
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.singletonMap(key, txResult));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(anyId());
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.singletonMap(key, txResult));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    TransactionResult changedTxResult = prepareResult(anyId() + "x");
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(changedTxResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetUpdatedByMyself_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(anyId());
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.singletonMap(key, txResult));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    TransactionResult changedTxResult = prepareResult(anyId());
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut())));

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(changedTxResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(anyId() + "x");
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.emptyMap());
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsExtended_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(anyId() + "xx", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key2, result2));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetExtendedByMyself_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(anyId());
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.emptyMap());
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut())));

    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsExtendedByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(anyId(), ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key2, result2));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut(ANY_TEXT_1, ANY_TEXT_2))));

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetDeleted_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(anyId());
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.singletonMap(key, txResult));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsDeleted_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(anyId() + "xx", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_MultipleScansInScanSetExist_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan1 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1))
            .build();

    Result result1 =
        new TransactionResult(
            new ResultImpl(
                ImmutableMap.of(
                    ANY_NAME_1,
                    TextColumn.of(ANY_NAME_1, ANY_TEXT_1),
                    ANY_NAME_2,
                    TextColumn.of(ANY_NAME_2, ANY_TEXT_2),
                    Attribute.ID,
                    TextColumn.of(Attribute.ID, "id1")),
                TABLE_METADATA));

    Result result2 =
        new TransactionResult(
            new ResultImpl(
                ImmutableMap.of(
                    ANY_NAME_1,
                    TextColumn.of(ANY_NAME_1, ANY_TEXT_2),
                    ANY_NAME_2,
                    TextColumn.of(ANY_NAME_2, ANY_TEXT_1),
                    Attribute.ID,
                    TextColumn.of(Attribute.ID, "id2")),
                TABLE_METADATA));

    Snapshot.Key key1 = new Snapshot.Key(scan1, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan2, result2, TABLE_METADATA);

    List<Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>>> scanSetEntries =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>(
                scan1,
                Maps.newLinkedHashMap(
                    Collections.singletonMap(key1, new TransactionResult(result1)))),
            new AbstractMap.SimpleEntry<>(
                scan2,
                Maps.newLinkedHashMap(
                    Collections.singletonMap(key2, new TransactionResult(result2)))));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            scanSetEntries,
            Collections.emptyList(),
            Collections.emptySet());

    Scanner scanner1 = mock(Scanner.class);
    when(scanner1.one()).thenReturn(Optional.of(result1)).thenReturn(Optional.empty());
    Scan scan1ForStorage =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    when(storage.scan(scan1ForStorage)).thenReturn(scanner1);

    Scanner scanner2 = mock(Scanner.class);
    when(scanner2.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());
    Scan scan2ForStorage =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    when(storage.scan(scan2ForStorage)).thenReturn(scanner2);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();
  }

  @Test
  public void toSerializable_NullMetadataInReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult txResult = new TransactionResult(result);
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(get, Optional.of(result));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(put)));

    Get getForStorage = Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_NullMetadataInReadSetChanged_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult changedResult = prepareResult(anyId());
    Map.Entry<Get, Optional<TransactionResult>> getSetEntry =
        new AbstractMap.SimpleEntry<>(get, Optional.of(result));
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.singletonList(getSetEntry),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(put)));

    Get getForStorage = Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(changedResult));

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ScanWithLimitInScanSet_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(anyId() + "x");
    TransactionResult result2 = prepareResult(anyId() + "x");
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(Collections.singletonMap(key1, result1));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingFirstRecordIntoScanRange_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_4);
    TransactionResult insertedResult = prepareResult(anyId() + "xx", ANY_TEXT_1, ANY_TEXT_2);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(insertedResult))
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingFirstRecordIntoScanRangeByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_4);
    TransactionResult insertedResult = prepareResult(anyId(), ANY_TEXT_1, ANY_TEXT_2);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut(ANY_TEXT_1, ANY_TEXT_2))));

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(insertedResult))
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingLastRecordIntoScanRange_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithLimit(3);
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult insertedResult = prepareResult(anyId() + "xx", ANY_TEXT_1, ANY_TEXT_4);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(insertedResult))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingLastRecordIntoScanRangeByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithLimit(3);
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult insertedResult = prepareResult(anyId(), ANY_TEXT_1, ANY_TEXT_4);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.singleton(new Snapshot.Key(preparePut(ANY_TEXT_1, ANY_TEXT_4))));

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(insertedResult))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenUpdatingRecords_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(anyId() + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            Collections.emptySet());

    // Simulate that the first and third records were updated by another transaction
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();

    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.toSerializable(validationInfo, anyId()))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenUpdatingRecordsByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(anyId() + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);

    // Simulate that the first and third records were updated by myself
    Set<Snapshot.Key> updatedRecordKeys = new HashSet<>();
    updatedRecordKeys.add(key1);
    updatedRecordKeys.add(key3);

    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            updatedRecordKeys);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();

    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenDeletingRecordsByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(anyId() + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA);
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3));
    Map.Entry<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSetEntry =
        new AbstractMap.SimpleEntry<>(scan, scanResults);

    // Simulate that the first and third records were deleted by myself
    Set<Snapshot.Key> updatedRecordKeys = new HashSet<>();
    updatedRecordKeys.add(key1);
    updatedRecordKeys.add(key3);

    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.singletonList(scanSetEntry),
            Collections.emptyList(),
            updatedRecordKeys);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScannerSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(anyId() + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults =
        Maps.newLinkedHashMap(ImmutableMap.of(key1, result1));
    Snapshot.ScannerInfo scannerInfo = new Snapshot.ScannerInfo(scan, scanResults);
    CommitHandler.ValidationInfo validationInfo =
        new CommitHandler.ValidationInfo(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(scannerInfo),
            Collections.emptySet());

    Scan scanForStorage = Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> handler.toSerializable(validationInfo, anyId()))
        .doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  protected void doThrowExceptionWhenCoordinatorPutState(
      TransactionState targetState, Class<? extends Exception> exceptionClass)
      throws CoordinatorException {
    doThrow(exceptionClass).when(coordinator).putState(new Coordinator.State(anyId(), targetState));
  }

  protected void doNothingWhenCoordinatorPutState() throws CoordinatorException {
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
  }

  protected void verifyCoordinatorPutState(TransactionState expectedTransactionState)
      throws CoordinatorException {
    verify(coordinator).putState(new Coordinator.State(anyId(), expectedTransactionState));
  }

  private void verifyBeforePreparationHook(
      boolean withBeforePreparationHook, TransactionContext context) {
    if (withBeforePreparationHook) {
      verify(beforePreparationHook).handle(eq(tableMetadataManager), eq(context));
    } else {
      verify(beforePreparationHook, never()).handle(any(), any());
    }
  }
}
