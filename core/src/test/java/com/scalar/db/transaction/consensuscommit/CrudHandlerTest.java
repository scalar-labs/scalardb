package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionBuilder.column;
import static com.scalar.db.api.ConditionBuilder.deleteIfExists;
import static com.scalar.db.api.ConditionBuilder.putIfExists;
import static com.scalar.db.api.ConditionSetBuilder.condition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CrudHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TEXT_5 = "text5";

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.TEXT)
              .addColumn(ANY_NAME_4, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .addSecondaryIndex(ANY_NAME_3)
              .build());

  private CrudHandler handler;
  @Mock private DistributedStorage storage;
  @Mock private RecoveryExecutor recoveryExecutor;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private MutationConditionsValidator mutationConditionsValidator;
  @Mock private ParallelExecutor parallelExecutor;

  @Mock private Snapshot snapshot;
  @Mock private Scanner scanner;
  @Mock private Result result;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    handler =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            false,
            mutationConditionsValidator,
            parallelExecutor);

    // Arrange
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    when(tableMetadataManager.getTransactionTableMetadata(any(), any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Get toGetForStorageFrom(Get get) {
    return Get.newBuilder(get).clearProjections().consistency(Consistency.LINEARIZABLE).build();
  }

  private Scan prepareScan() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private Scan prepareCrossPartitionScan() {
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .all()
        .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
        .build();
  }

  private Scan toScanForStorageFrom(Scan scan) {
    return Scan.newBuilder(scan).clearProjections().consistency(Consistency.LINEARIZABLE).build();
  }

  private TransactionResult prepareResult(TransactionState state) {
    return prepareResult(ANY_TEXT_1, ANY_TEXT_2, state);
  }

  private TransactionResult prepareResult(
      String partitionKeyColumnValue, String clusteringKeyColumnValue, TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, partitionKeyColumnValue))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, clusteringKeyColumnValue))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID_2))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, state.get()))
            .put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, 2))
            .put(Attribute.BEFORE_ID, TextColumn.of(Attribute.BEFORE_ID, ANY_ID_1))
            .put(
                Attribute.BEFORE_STATE,
                IntColumn.of(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get()))
            .put(Attribute.BEFORE_VERSION, IntColumn.of(Attribute.BEFORE_VERSION, 1))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void get_GetExistsInSnapshot_ShouldReturnFromSnapshot() throws CrudException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    Snapshot.Key key = new Snapshot.Key(get);
    Optional<TransactionResult> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(true);
    when(snapshot.getResult(key, getForStorage)).thenReturn(expected);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> actual = handler.get(get, context);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(expected);
    when(snapshot.getResult(key, getForStorage)).thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot).putIntoReadSet(key, Optional.of((TransactionResult) expected.get()));
    verify(snapshot).putIntoGetSet(getForStorage, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_InReadOnlyMode_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(expected);
    when(snapshot.getResult(key, getForStorage)).thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoGetSet(getForStorage, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_InOneOperationMode_ValidationNotRequired_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(expected);
    when(snapshot.mergeResult(key, transactionResult, getForStorage.getConjunctions()))
        .thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, true, true);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoGetSet(any(), any());
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_InOneOperationMode_ValidationRequired_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(expected);
    when(snapshot.mergeResult(key, transactionResult, getForStorage.getConjunctions()))
        .thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SERIALIZABLE, true, true);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoGetSet(getForStorage, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void
      get_GetWithConjunction_GetNotExistsInSnapshotAndRecordInStorageCommitted_InOneOperationMode_ValidationRequired_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    ConditionalExpression condition = column(ANY_NAME_3).isEqualToText(ANY_TEXT_3);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(any())).thenReturn(expected);
    when(snapshot.mergeResult(
            key, transactionResult, Collections.singleton(Selection.Conjunction.of(condition))))
        .thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SERIALIZABLE, true, true);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage)
        .get(
            Get.newBuilder(getForStorage)
                .clearConditions()
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .build());
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoGetSet(getForStorage, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_ReadCommittedIsolation_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = Get.newBuilder(prepareGet()).build();
    Get getForStorage = toGetForStorageFrom(get);
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(any())).thenReturn(expected);
    when(snapshot.mergeResult(key, transactionResult, getForStorage.getConjunctions()))
        .thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot).putIntoReadSet(key, Optional.of((TransactionResult) expected.get()));
    verify(snapshot, never()).putIntoGetSet(any(), any());
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageCommitted_ReadCommittedIsolation_InReadOnlyMode_ShouldReturnFromStorageAndNotUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = Get.newBuilder(prepareGet()).build();
    Get getForStorage = toGetForStorageFrom(get);

    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Optional<TransactionResult> transactionResult = expected.map(e -> (TransactionResult) e);
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(any())).thenReturn(expected);
    when(snapshot.mergeResult(key, transactionResult, getForStorage.getConjunctions()))
        .thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, true, false);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoGetSet(any(), any());
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageNotCommitted_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    Get getForStorage = toGetForStorageFrom(get);
    result = prepareResult(TransactionState.PREPARED);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);

    TransactionResult expected = mock(TransactionResult.class);
    when(expected.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(expected.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    when(snapshot.getResult(key, getForStorage)).thenReturn(Optional.of(expected));

    TransactionResult recoveredResult = mock(TransactionResult.class);
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> actual = handler.get(get, context);

    // Assert
    verify(storage).get(getForStorage);
    verify(recoveryExecutor)
        .execute(
            key,
            getForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot).putIntoGetSet(getForStorage, Optional.of(recoveredResult));

    assertThat(actual)
        .isEqualTo(
            Optional.of(
                new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false)));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageNotCommitted_ReadCommittedIsolation_ShouldCallRecoveryExecutorWithReturnCommittedResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    Get getForStorage = toGetForStorageFrom(get);
    result = prepareResult(TransactionState.PREPARED);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);

    TransactionResult expected = mock(TransactionResult.class);
    when(expected.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(expected.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    TransactionResult transactionResult = new TransactionResult(result);

    TransactionResult recoveredResult = mock(TransactionResult.class);
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getForStorage,
            transactionResult,
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    when(snapshot.mergeResult(key, Optional.of(recoveredResult), getForStorage.getConjunctions()))
        .thenReturn(Optional.of(expected));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    Optional<Result> actual = handler.get(get, context);

    // Assert
    verify(storage).get(getForStorage);
    verify(recoveryExecutor)
        .execute(
            key,
            getForStorage,
            transactionResult,
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot, never()).putIntoGetSet(any(), any());

    assertThat(actual)
        .isEqualTo(
            Optional.of(
                new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false)));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageNotCommitted_ReadCommittedIsolation_InReadOnlyMode_ShouldCallRecoveryExecutorWithReturnCommittedResultAndNotRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    Get getForStorage = toGetForStorageFrom(get);
    result = prepareResult(TransactionState.PREPARED);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);

    TransactionResult expected = mock(TransactionResult.class);
    when(expected.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(expected.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    TransactionResult transactionResult = new TransactionResult(result);

    TransactionResult recoveredResult = mock(TransactionResult.class);
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getForStorage,
            transactionResult,
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    when(snapshot.mergeResult(key, Optional.of(recoveredResult), getForStorage.getConjunctions()))
        .thenReturn(Optional.of(expected));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, true, false);

    // Act
    Optional<Result> actual = handler.get(get, context);

    // Assert
    verify(storage).get(getForStorage);
    verify(recoveryExecutor)
        .execute(
            key,
            getForStorage,
            transactionResult,
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoGetSet(any(), any());

    assertThat(actual)
        .isEqualTo(
            Optional.of(
                new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false)));
  }

  @Test
  public void get_GetNotExistsInSnapshotAndRecordNotExistsInStorage_ShouldReturnEmpty()
      throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(Optional.empty());
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> result = handler.get(get, context);

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void get_GetNotExistsInSnapshotAndExceptionThrownInStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(getForStorage)).thenThrow(toThrow);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.get(get, context))
        .isInstanceOf(CrudException.class)
        .hasCause(toThrow);
  }

  @Test
  public void get_CalledTwice_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Get originalGet = prepareGet();
    Get getForStorage = toGetForStorageFrom(originalGet);
    Get get1 = prepareGet();
    Get get2 = prepareGet();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false).thenReturn(true);
    when(snapshot.getResult(key, getForStorage)).thenReturn(expected).thenReturn(expected);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> results1 = handler.get(get1, context);
    Optional<Result> results2 = handler.get(get2, context);

    // Assert
    verify(snapshot).putIntoReadSet(key, expected);
    assertThat(results1)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    assertThat(results1).isEqualTo(results2);
    verify(storage, never()).get(originalGet);
    verify(storage).get(getForStorage);
  }

  @Test
  public void get_CalledTwice_ReadCommittedIsolation_BothShouldReturnFromStorage()
      throws ExecutionException, CrudException {
    // Arrange
    Get originalGet = prepareGet();
    Get getForStorage = toGetForStorageFrom(originalGet);
    Get get1 = prepareGet();
    Get get2 = prepareGet();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    Snapshot.Key key = new Snapshot.Key(getForStorage);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(snapshot.mergeResult(
            key, Optional.of(new TransactionResult(result)), getForStorage.getConjunctions()))
        .thenReturn(expected);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    Optional<Result> results1 = handler.get(get1, context);
    Optional<Result> results2 = handler.get(get2, context);

    // Assert
    verify(storage, times(2)).get(getForStorage);
    verify(snapshot, times(2)).putIntoReadSet(key, expected);
    assertThat(results1)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    assertThat(results1).isEqualTo(results2);
  }

  @Test
  public void get_CalledTwiceUnderRealSnapshot_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Get originalGet = prepareGet();
    Get getForStorage = toGetForStorageFrom(originalGet);
    Get get1 = prepareGet();
    Get get2 = prepareGet();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    snapshot = new Snapshot(ANY_ID_1, Isolation.SNAPSHOT, tableMetadataManager, parallelExecutor);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> results1 = handler.get(get1, context);
    Optional<Result> results2 = handler.get(get2, context);

    // Assert
    assertThat(results1)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    assertThat(results1).isEqualTo(results2);
    verify(storage, never()).get(originalGet);
    verify(storage).get(getForStorage);
  }

  @Test
  public void get_CalledTwiceUnderRealSnapshot_ReadCommittedIsolation_BothShouldReturnFromStorage()
      throws ExecutionException, CrudException {
    // Arrange
    Get originalGet = prepareGet();
    Get getForStorage = toGetForStorageFrom(originalGet);
    Get get1 = prepareGet();
    Get get2 = prepareGet();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    snapshot =
        new Snapshot(ANY_ID_1, Isolation.READ_COMMITTED, tableMetadataManager, parallelExecutor);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    Optional<Result> results1 = handler.get(get1, context);
    Optional<Result> results2 = handler.get(get2, context);

    // Assert
    verify(storage, times(2)).get(getForStorage);
    assertThat(results1)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    assertThat(results1).isEqualTo(results2);
  }

  @Test
  public void get_ForNonExistingTable_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();
    Get getForStorage = toGetForStorageFrom(get);

    when(tableMetadataManager.getTransactionTableMetadata(getForStorage)).thenReturn(null);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.get(get, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_DifferentGetButSameRecordReturned_ShouldNotOverwriteReadSet()
      throws ExecutionException, CrudException {
    // Arrange
    Get get1 = prepareGet();
    Get get2 = Get.newBuilder(get1).where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3)).build();
    Get getForStorage1 = toGetForStorageFrom(get1);
    Get getForStorage2 =
        Get.newBuilder(get2)
            .clearProjections()
            .clearConditions()
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    Snapshot.Key key = new Snapshot.Key(getForStorage1);
    when(snapshot.getResult(any(), any())).thenReturn(expected).thenReturn(expected);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false).thenReturn(true);
    when(storage.get(any())).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Optional<Result> results1 = handler.get(get1, context);
    Optional<Result> results2 = handler.get(get2, context);

    // Assert
    assertThat(results1)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    assertThat(results2).isEqualTo(results1);
    verify(storage).get(getForStorage1);
    verify(storage).get(getForStorage2);
    verify(snapshot).putIntoReadSet(key, expected);
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_ResultGivenFromStorage_ShouldUpdateSnapshotAndReturn(ScanType scanType)
      throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);
    TransactionResult expected = new TransactionResult(result);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(expected));
    verify(snapshot)
        .putIntoScanSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, expected)));
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key, expected));
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_ResultGivenFromStorage_InReadOnlyMode_ShouldUpdateSnapshotAndReturn(
      ScanType scanType) throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);
    TransactionResult expected = new TransactionResult(result);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot)
        .putIntoScanSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, expected)));
    verify(snapshot, never()).verifyNoOverlap(any(), any());
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_ResultGivenFromStorage_InOneOperationMode_ValidationNotRequired_ShouldUpdateSnapshotAndReturn(
          ScanType scanType) throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    TransactionResult expected = new TransactionResult(result);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, true, true);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoScanSet(any(), any());
    verify(snapshot, never()).verifyNoOverlap(any(), any());
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_ResultGivenFromStorage_InOneOperationMode_ValidationRequired_ShouldUpdateSnapshotAndReturn(
          ScanType scanType) throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);
    TransactionResult expected = new TransactionResult(result);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SERIALIZABLE, true, true);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot)
        .putIntoScanSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, expected)));
    verify(snapshot, never()).verifyNoOverlap(any(), any());
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_PreparedResultGivenFromStorage_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);

    result = prepareResult(TransactionState.PREPARED);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(recoveredResult.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            scanForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot)
        .putIntoScanSet(
            scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, recoveredResult)));
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key, recoveredResult));

    assertThat(results)
        .containsExactly(
            new FilteredResult(recoveredResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_PreparedResultGivenFromStorage_ReadCommittedIsolation_ShouldCallRecoveryExecutorWithReturnCommittedResultAndRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);

    result = prepareResult(TransactionState.PREPARED);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(recoveredResult.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            scanForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot, never()).putIntoScanSet(any(), any());
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key, recoveredResult));

    assertThat(results)
        .containsExactly(
            new FilteredResult(recoveredResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_PreparedResultGivenFromStorage_ReadCommittedIsolation_InReadOnlyMode_ShouldCallRecoveryExecutorWithReturnCommittedResultAndNotRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);

    result = prepareResult(TransactionState.PREPARED);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(recoveredResult.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            scanForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.READ_COMMITTED, true, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoScanSet(any(), any());
    verify(snapshot, never()).verifyNoOverlap(any(), any());

    assertThat(results)
        .containsExactly(
            new FilteredResult(recoveredResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_CalledTwice_SecondTimeShouldReturnTheSameFromSnapshot(ScanType scanType)
      throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan originalScan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(originalScan);
    Scan scan1 = prepareScan();
    Scan scan2 = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    TransactionResult expected = new TransactionResult(result);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    Snapshot.Key key = new Snapshot.Key(scanForStorage, result, TABLE_METADATA);
    when(snapshot.getResults(scanForStorage))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(Maps.newLinkedHashMap(ImmutableMap.of(key, expected))));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results1 = scanOrGetScanner(scan1, scanType, context);
    List<Result> results2 = scanOrGetScanner(scan2, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(expected));
    verify(snapshot)
        .putIntoScanSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, expected)));
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
    assertThat(results1).isEqualTo(results2);
    verify(storage, never()).scan(originalScan);
    verify(storage).scan(scanForStorage);
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scan_CalledTwiceUnderRealSnapshot_SecondTimeShouldReturnTheSameFromSnapshot(
      ScanType scanType) throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan originalScan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(originalScan);
    Scan scan1 = prepareScan();
    Scan scan2 = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    TransactionResult expected = new TransactionResult(result);
    snapshot = new Snapshot(ANY_ID_1, Isolation.SNAPSHOT, tableMetadataManager, parallelExecutor);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results1 = scanOrGetScanner(scan1, scanType, context);
    List<Result> results2 = scanOrGetScanner(scan2, scanType, context);

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
    assertThat(results1).isEqualTo(results2);

    verify(scanner).close();
    verify(storage, never()).scan(originalScan);
    verify(storage).scan(scanForStorage);
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_GetCalledAfterScan_ShouldReturnFromStorage(ScanType scanType)
      throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    Get getForStorage = toGetForStorageFrom(get);
    Optional<TransactionResult> transactionResult = Optional.of(new TransactionResult(result));
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    when(snapshot.getResult(key, getForStorage)).thenReturn(transactionResult);
    when(snapshot.getResult(key)).thenReturn(transactionResult);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);
    Optional<Result> result = handler.get(get, context);

    // Assert
    verify(storage).scan(scanForStorage);
    verify(storage).get(getForStorage);
    verify(scanner).close();

    assertThat(results.size()).isEqualTo(1);
    assertThat(result).isPresent();
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_GetCalledAfterScanUnderRealSnapshot_ShouldReturnFromStorage(
      ScanType scanType) throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = toScanForStorageFrom(prepareScan());
    result = prepareResult(TransactionState.COMMITTED);
    snapshot = new Snapshot(ANY_ID_1, Isolation.SNAPSHOT, tableMetadataManager, parallelExecutor);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scan)).thenReturn(scanner);
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);
    Optional<Result> result = handler.get(get, context);

    // Assert
    verify(storage).scan(scan);
    verify(storage).get(getForStorage);
    verify(scanner).close();

    assertThat(results.size()).isEqualTo(1);
    assertThat(result).isPresent();
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_CalledAfterDeleteUnderRealSnapshot_ShouldThrowIllegalArgumentException(
      ScanType scanType) throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);

    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_3))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID_2))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, TransactionState.COMMITTED.get()))
            .put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, 2))
            .put(Attribute.BEFORE_ID, TextColumn.of(Attribute.BEFORE_ID, ANY_ID_1))
            .put(
                Attribute.BEFORE_STATE,
                IntColumn.of(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get()))
            .put(Attribute.BEFORE_VERSION, IntColumn.of(Attribute.BEFORE_VERSION, 1))
            .build();
    Result result2 = new ResultImpl(columns, TABLE_METADATA);

    ConcurrentMap<Snapshot.Key, Optional<TransactionResult>> readSet = new ConcurrentHashMap<>();
    Map<Snapshot.Key, Delete> deleteSet = new HashMap<>();
    snapshot =
        new Snapshot(
            ANY_ID_1,
            Isolation.SNAPSHOT,
            tableMetadataManager,
            parallelExecutor,
            readSet,
            new ConcurrentHashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            deleteSet,
            new ArrayList<>());
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Arrays.asList(result, result2).iterator());
    } else {
      when(scanner.one())
          .thenReturn(Optional.of(result))
          .thenReturn(Optional.of(result2))
          .thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_3))
            .build();

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    handler.delete(delete, context);

    // check the delete set
    assertThat(deleteSet.size()).isEqualTo(1);
    assertThat(deleteSet).containsKey(new Snapshot.Key(delete));

    assertThatThrownBy(() -> scanOrGetScanner(scan, scanType, context))
        .isInstanceOf(IllegalArgumentException.class);

    verify(scanner).close();
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_CrossPartitionScanAndResultFromStorageGiven_ShouldUpdateSnapshotAndVerifyNoOverlapThenReturn(
          ScanType scanType) throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = prepareCrossPartitionScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(any(ScanAll.class))).thenReturn(scanner);
    TransactionResult transactionResult = new TransactionResult(result);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scan, scanType, context);

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(transactionResult));
    verify(snapshot)
        .putIntoScanSet(
            scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, transactionResult)));
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key, transactionResult));
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(
            new FilteredResult(transactionResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_CrossPartitionScanAndPreparedResultFromStorageGiven_RecoveredRecordMatchesConjunction_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareCrossPartitionScan();
    Scan scanForStorage = toScanForStorageFrom(scan);

    result = prepareResult(TransactionState.PREPARED);
    Snapshot.Key key = new Snapshot.Key(scanForStorage, result, TABLE_METADATA);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(any(ScanAll.class))).thenReturn(scanner);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_3);
    when(recoveredResult.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)));

    when(snapshot.getResult(key)).thenReturn(Optional.of(new TransactionResult(result)));

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            scanForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scanForStorage, scanType, context);

    // Assert
    verify(storage)
        .scan(
            Scan.newBuilder(scanForStorage)
                .clearConditions()
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .build());
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot)
        .putIntoScanSet(
            scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key, recoveredResult)));
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key, recoveredResult));

    assertThat(results)
        .containsExactly(
            new FilteredResult(recoveredResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_CrossPartitionScanAndPreparedResultFromStorageGiven_RecoveredRecordDoesNotMatchConjunction_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareCrossPartitionScan();
    Scan scanForStorage = toScanForStorageFrom(scan);

    result = prepareResult(TransactionState.PREPARED);
    Snapshot.Key key = new Snapshot.Key(scanForStorage, result, TABLE_METADATA);
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(any(ScanAll.class))).thenReturn(scanner);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_4);
    when(recoveredResult.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_4)));

    when(snapshot.getResult(key)).thenReturn(Optional.of(new TransactionResult(result)));

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            scanForStorage,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scanForStorage, scanType, context);

    // Assert
    verify(storage)
        .scan(
            Scan.newBuilder(scanForStorage)
                .clearConditions()
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .build());
    verify(scanner).close();
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoScanSet(scanForStorage, Maps.newLinkedHashMap());
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of());

    assertThat(results).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_WithLimit_ShouldReturnLimitedResults(ScanType scanType)
      throws CrudException, ExecutionException, IOException {
    // Arrange
    Scan scanWithoutLimit = prepareScan();
    Scan scanWithoutLimitForStorage = toScanForStorageFrom(scanWithoutLimit);
    Scan scanWithLimit = Scan.newBuilder(scanWithoutLimit).limit(2).build();
    Scan scanWithLimitForStorage = toScanForStorageFrom(scanWithLimit);

    Result result1 = prepareResult(ANY_TEXT_1, ANY_TEXT_2, TransactionState.COMMITTED);
    Result result2 = prepareResult(ANY_TEXT_1, ANY_TEXT_3, TransactionState.COMMITTED);

    Snapshot.Key key1 = new Snapshot.Key(scanWithLimit, result1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scanWithLimit, result2, TABLE_METADATA);

    TransactionResult transactionResult1 = new TransactionResult(result1);
    TransactionResult transactionResult2 = new TransactionResult(result2);

    // Set up mock scanner to return two results
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Arrays.asList(result1, result2).iterator());
    } else {
      when(scanner.one())
          .thenReturn(Optional.of(result1))
          .thenReturn(Optional.of(result2))
          .thenReturn(Optional.empty());
    }
    when(storage.scan(scanWithoutLimitForStorage)).thenReturn(scanner);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scanWithLimit, scanType, context);

    // Assert
    assertThat(results).hasSize(2);
    assertThat(results.get(0))
        .isEqualTo(
            new FilteredResult(transactionResult1, Collections.emptyList(), TABLE_METADATA, false));
    assertThat(results.get(1))
        .isEqualTo(
            new FilteredResult(transactionResult2, Collections.emptyList(), TABLE_METADATA, false));

    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key1, Optional.of(transactionResult1));
    verify(snapshot).putIntoReadSet(key2, Optional.of(transactionResult2));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<LinkedHashMap<Snapshot.Key, TransactionResult>> resultsCaptor =
        ArgumentCaptor.forClass(LinkedHashMap.class);
    verify(snapshot).putIntoScanSet(eq(scanWithLimitForStorage), resultsCaptor.capture());

    LinkedHashMap<Snapshot.Key, TransactionResult> capturedResults = resultsCaptor.getValue();
    assertThat(capturedResults).hasSize(2);
    assertThat(capturedResults).containsKeys(key1, key2);
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void scanOrGetScanner_WithLimitExceedingAvailableResults_ShouldReturnAllAvailableResults(
      ScanType scanType) throws CrudException, ExecutionException, IOException {
    // Arrange
    Scan scanWithoutLimit = prepareScan();
    Scan scanWithLimit =
        Scan.newBuilder(scanWithoutLimit).limit(5).build(); // Limit higher than available results
    Scan scanForStorage = toScanForStorageFrom(scanWithoutLimit);

    Result result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key1 = new Snapshot.Key(scanWithLimit, result, TABLE_METADATA);
    TransactionResult transactionResult1 = new TransactionResult(result);

    // Set up mock scanner to return one result (less than limit)
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    } else {
      when(scanner.one()).thenReturn(Optional.of(result)).thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scanWithLimit, scanType, context);

    // Assert
    assertThat(results).hasSize(1);
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key1, Optional.of(transactionResult1));
  }

  @ParameterizedTest
  @EnumSource(ScanType.class)
  void
      scanOrGetScanner_WithLimit_UncommittedResult_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover(
          ScanType scanType) throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scanWithoutLimit = prepareScan();
    Scan scanWithLimit = Scan.newBuilder(scanWithoutLimit).limit(2).build();
    Scan scanForStorageWithLimit = toScanForStorageFrom(scanWithLimit);
    Scan scanForStorageWithoutLimit = toScanForStorageFrom(scanWithoutLimit);

    Result uncommittedResult1 = prepareResult(ANY_TEXT_1, ANY_TEXT_2, TransactionState.DELETED);
    Result uncommittedResult2 = prepareResult(ANY_TEXT_1, ANY_TEXT_3, TransactionState.PREPARED);
    Result uncommittedResult3 = prepareResult(ANY_TEXT_1, ANY_TEXT_4, TransactionState.PREPARED);

    Snapshot.Key key1 = new Snapshot.Key(scanWithLimit, uncommittedResult1, TABLE_METADATA);
    Snapshot.Key key2 = new Snapshot.Key(scanWithLimit, uncommittedResult2, TABLE_METADATA);
    Snapshot.Key key3 = new Snapshot.Key(scanWithLimit, uncommittedResult3, TABLE_METADATA);

    // Set up mock scanner to return one committed and one uncommitted result
    if (scanType == ScanType.SCAN) {
      when(scanner.iterator())
          .thenReturn(
              Arrays.asList(uncommittedResult1, uncommittedResult2, uncommittedResult3).iterator());
    } else {
      when(scanner.one())
          .thenReturn(Optional.of(uncommittedResult1))
          .thenReturn(Optional.of(uncommittedResult2))
          .thenReturn(Optional.of(uncommittedResult3))
          .thenReturn(Optional.empty());
    }
    when(storage.scan(scanForStorageWithoutLimit)).thenReturn(scanner);

    TransactionResult recoveredResult1 = mock(TransactionResult.class);
    when(recoveredResult1.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult1.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_3);
    when(recoveredResult1.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)));

    TransactionResult recoveredResult2 = mock(TransactionResult.class);
    when(recoveredResult1.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult1.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_4);
    when(recoveredResult1.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_4)));

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key1,
            scanForStorageWithLimit,
            new TransactionResult(uncommittedResult1),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key1, Optional.empty(), recoveryFuture));
    when(recoveryExecutor.execute(
            key2,
            scanForStorageWithLimit,
            new TransactionResult(uncommittedResult2),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(
            new RecoveryExecutor.Result(key2, Optional.of(recoveredResult1), recoveryFuture));
    when(recoveryExecutor.execute(
            key3,
            scanForStorageWithLimit,
            new TransactionResult(uncommittedResult3),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(
            new RecoveryExecutor.Result(key3, Optional.of(recoveredResult2), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    List<Result> results = scanOrGetScanner(scanWithLimit, scanType, context);

    // Assert
    verify(storage).scan(scanForStorageWithoutLimit);
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key2, Optional.of(recoveredResult1));
    verify(snapshot).putIntoReadSet(key3, Optional.of(recoveredResult2));
    verify(snapshot)
        .putIntoScanSet(
            scanForStorageWithLimit,
            Maps.newLinkedHashMap(ImmutableMap.of(key2, recoveredResult1, key3, recoveredResult2)));
    verify(snapshot)
        .verifyNoOverlap(
            scanForStorageWithLimit,
            ImmutableMap.of(key2, recoveredResult1, key3, recoveredResult2));

    assertThat(results)
        .containsExactly(
            new FilteredResult(recoveredResult1, Collections.emptyList(), TABLE_METADATA, false),
            new FilteredResult(recoveredResult2, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void
      scan_RuntimeExceptionCausedByExecutionExceptionThrownByIteratorHasNext_ShouldThrowCrudException()
          throws ExecutionException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    @SuppressWarnings("unchecked")
    Iterator<Result> iterator = mock(Iterator.class);
    ExecutionException executionException = mock(ExecutionException.class);
    RuntimeException runtimeException = mock(RuntimeException.class);
    when(runtimeException.getCause()).thenReturn(executionException);
    when(iterator.hasNext()).thenThrow(runtimeException);
    when(scanner.iterator()).thenReturn(iterator);
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.scan(scan, context))
        .isInstanceOf(CrudException.class)
        .hasCause(executionException);

    verify(scanner).close();
  }

  @Test
  public void scan_RuntimeExceptionThrownByIteratorHasNext_ShouldThrowCrudException()
      throws ExecutionException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    @SuppressWarnings("unchecked")
    Iterator<Result> iterator = mock(Iterator.class);
    RuntimeException runtimeException = mock(RuntimeException.class);
    when(iterator.hasNext()).thenThrow(runtimeException);
    when(scanner.iterator()).thenReturn(iterator);
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.scan(scan, context))
        .isInstanceOf(CrudException.class)
        .hasCause(runtimeException);

    verify(scanner).close();
  }

  @Test
  public void getScanner_ExecutionExceptionThrownByScannerOne_ShouldThrowCrudException()
      throws ExecutionException, IOException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    ExecutionException executionException = mock(ExecutionException.class);
    when(scanner.one()).thenThrow(executionException);
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    TransactionCrudOperable.Scanner actualScanner = handler.getScanner(scan, context);
    assertThatThrownBy(actualScanner::one)
        .isInstanceOf(CrudException.class)
        .hasCause(executionException);

    verify(scanner).close();
  }

  @Test
  public void
      getScanner_ScannerNotFullyScanned_ValidationRequired_ShouldPutReadSetAndScannerSetInSnapshotAndVerifyScan()
          throws ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    Result result1 = prepareResult(TransactionState.COMMITTED);
    Result result2 = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    TransactionResult txResult1 = new TransactionResult(result1);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    TransactionCrudOperable.Scanner actualScanner = handler.getScanner(scan, context);
    Optional<Result> actualResult = actualScanner.one();
    actualScanner.close();

    // Assert
    verify(scanner).close();
    verify(snapshot).putIntoReadSet(key1, Optional.of(txResult1));
    verify(snapshot)
        .putIntoScannerSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key1, txResult1)));
    verify(snapshot).verifyNoOverlap(scanForStorage, ImmutableMap.of(key1, txResult1));

    assertThat(actualResult)
        .hasValue(new FilteredResult(txResult1, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void
      getScanner_ScannerNotFullyScanned_InOneOperationMode_ValidationNotRequired_ShouldUpdateSnapshotProperly()
          throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    Result result1 = prepareResult(TransactionState.COMMITTED);
    Result result2 = prepareResult(TransactionState.COMMITTED);
    TransactionResult txResult1 = new TransactionResult(result1);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, true, true);

    // Act
    TransactionCrudOperable.Scanner actualScanner = handler.getScanner(scan, context);
    Optional<Result> actualResult = actualScanner.one();
    actualScanner.close();

    // Assert
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot, never()).putIntoScannerSet(any(), any());
    verify(snapshot, never()).verifyNoOverlap(any(), any());

    assertThat(actualResult)
        .hasValue(new FilteredResult(txResult1, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void
      getScanner_ScannerNotFullyScanned_InOneOperationMode_ValidationRequired_ShouldUpdateSnapshotProperly()
          throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    Result result1 = prepareResult(TransactionState.COMMITTED);
    Result result2 = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA);
    TransactionResult txResult1 = new TransactionResult(result1);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SERIALIZABLE, true, true);

    // Act
    TransactionCrudOperable.Scanner actualScanner = handler.getScanner(scan, context);
    Optional<Result> actualResult = actualScanner.one();
    actualScanner.close();

    // Assert
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot)
        .putIntoScannerSet(scanForStorage, Maps.newLinkedHashMap(ImmutableMap.of(key1, txResult1)));
    verify(snapshot, never()).verifyNoOverlap(any(), any());

    assertThat(actualResult)
        .hasValue(new FilteredResult(txResult1, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void put_PutWithoutConditionGiven_ShouldCallAppropriateMethods() throws CrudException {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "foo")).build();
    CrudHandler spied = spy(handler);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    spied.put(put, context);

    // Assert
    verify(spied, never()).readUnread(any(), any(), any());
    verify(snapshot, never()).getResult(any());
    verify(mutationConditionsValidator, never())
        .checkIfConditionIsSatisfied(any(Put.class), any(), any());
    verify(snapshot).putIntoWriteSet(new Snapshot.Key(put), put);
  }

  @Test
  public void
      put_PutWithConditionAndImplicitPreReadEnabledGiven_WithResultInReadSet_ShouldCallAppropriateMethods()
          throws CrudException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(putIfExists())
            .enableImplicitPreRead()
            .build();
    Snapshot.Key key = new Snapshot.Key(put);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(true);
    TransactionResult result = mock(TransactionResult.class);
    when(result.isCommitted()).thenReturn(true);
    when(snapshot.getResult(key)).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    spied.put(put, context);

    // Assert
    verify(spied, never()).readUnread(key, getForKey, context);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result, context);
    verify(snapshot).putIntoWriteSet(key, put);
  }

  @Test
  public void
      put_PutWithConditionAndImplicitPreReadEnabledGiven_WithoutResultInReadSet_ShouldCallAppropriateMethods()
          throws CrudException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(putIfExists())
            .enableImplicitPreRead()
            .build();
    Snapshot.Key key = new Snapshot.Key(put);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false);
    TransactionResult result = mock(TransactionResult.class);
    when(result.isCommitted()).thenReturn(true);
    when(snapshot.getResult(key)).thenReturn(Optional.of(result));

    Get getForKey =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(key.getNamespace())
                .table(key.getTable())
                .partitionKey(key.getPartitionKey())
                .build());

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    doReturn(Optional.empty()).when(spied).getFromStorage(getForKey, context);

    // Act
    spied.put(put, context);

    // Assert
    verify(spied).read(key, getForKey, context);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result, context);
    verify(snapshot).putIntoWriteSet(key, put);
  }

  @Test
  public void
      put_PutWithConditionAndImplicitPreReadDisabledGiven_WithResultInReadSet_ShouldCallAppropriateMethods()
          throws CrudException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(putIfExists())
            .build();
    Snapshot.Key key = new Snapshot.Key(put);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(true);
    TransactionResult result = mock(TransactionResult.class);
    when(result.isCommitted()).thenReturn(true);
    when(snapshot.getResult(key)).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    spied.put(put, context);

    // Assert
    verify(spied, never()).readUnread(key, getForKey, context);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result, context);
    verify(snapshot).putIntoWriteSet(key, put);
  }

  @Test
  public void
      put_PutWithConditionAndImplicitPreReadDisabledGiven_WithoutResultInReadSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(putIfExists())
            .build();
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.put(put, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void delete_DeleteWithoutConditionGiven_ShouldCallAppropriateMethods()
      throws CrudException {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .build();

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    spied.delete(delete, context);

    // Assert
    verify(spied, never()).readUnread(any(), any(), any());
    verify(snapshot, never()).getResult(any());
    verify(mutationConditionsValidator, never())
        .checkIfConditionIsSatisfied(any(Delete.class), any(), any());
    verify(snapshot).putIntoDeleteSet(new Snapshot.Key(delete), delete);
  }

  @Test
  public void delete_DeleteWithConditionGiven_WithResultInReadSet_ShouldCallAppropriateMethods()
      throws CrudException {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(deleteIfExists())
            .build();
    Snapshot.Key key = new Snapshot.Key(delete);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(true);
    TransactionResult result = mock(TransactionResult.class);
    when(result.isCommitted()).thenReturn(true);
    when(snapshot.getResult(key)).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    spied.delete(delete, context);

    // Assert
    verify(spied, never()).readUnread(key, getForKey, context);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(delete, result, context);
    verify(snapshot).putIntoDeleteSet(key, delete);
  }

  @Test
  public void delete_DeleteWithConditionGiven_WithoutResultInReadSet_ShouldCallAppropriateMethods()
      throws CrudException {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("c1", "foo"))
            .condition(deleteIfExists())
            .build();
    Snapshot.Key key = new Snapshot.Key(delete);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false);
    when(snapshot.getResult(key)).thenReturn(Optional.empty());

    Get getForKey =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(key.getNamespace())
                .table(key.getTable())
                .partitionKey(key.getPartitionKey())
                .build());

    CrudHandler spied = spy(handler);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    doReturn(Optional.empty()).when(spied).getFromStorage(getForKey, context);

    // Act
    spied.delete(delete, context);

    // Assert
    verify(spied).read(key, getForKey, context);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(delete, null, context);
    verify(snapshot).putIntoDeleteSet(key, delete);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readUnread_GetContainedInGetSet_ShouldCallAppropriateMethods()
      throws CrudException, ExecutionException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));
    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(true);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage, never()).get(any());
    verify(snapshot, never()).putIntoReadSet(any(Snapshot.Key.class), any(Optional.class));
    verify(snapshot, never()).putIntoGetSet(any(Get.class), any(Optional.class));
  }

  @Test
  public void
      readUnread_GetNotContainedInGetSet_EmptyResultReturnedByStorage_ShouldCallAppropriateMethods()
          throws CrudException, ExecutionException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));
    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);
    when(storage.get(any())).thenReturn(Optional.empty());
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage).get(any());
    verify(snapshot).putIntoReadSet(key, Optional.empty());
    verify(snapshot).putIntoGetSet(getForKey, Optional.empty());
  }

  @Test
  public void
      readUnread_GetWithConjunctionsNotContainedInGetSet_EmptyResultReturnedByStorage_ShouldCallAppropriateMethods()
          throws CrudException, ExecutionException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));
    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_1))
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);
    when(storage.get(any())).thenReturn(Optional.empty());
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(key.getNamespace())
                .table(key.getTable())
                .partitionKey(key.getPartitionKey())
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_1))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_1))
                .build());
    verify(snapshot, never()).putIntoReadSet(key, Optional.empty());
    verify(snapshot).putIntoGetSet(getForKey, Optional.empty());
  }

  @Test
  public void
      readUnread_GetNotContainedInGetSet_CommittedRecordReturnedByStorage_ShouldCallAppropriateMethods()
          throws CrudException, ExecutionException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage).get(any());
    verify(snapshot).putIntoReadSet(key, Optional.of(new TransactionResult(result)));
    verify(snapshot).putIntoGetSet(getForKey, Optional.of(new TransactionResult(result)));
  }

  @Test
  public void
      readUnread_GetNotContainedInGetSet_UncommittedRecordReturnedByStorage_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);

    TransactionResult recoveredResult = mock(TransactionResult.class);
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getForKey,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage).get(getForKey);
    verify(recoveryExecutor)
        .execute(
            key,
            getForKey,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot).putIntoGetSet(getForKey, Optional.of(recoveredResult));
  }

  @Test
  public void
      readUnread_GetNotContainedInGetSet_UncommittedRecordReturnedByStorage_RecoveredRecordIsEmpty_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);

    Optional<TransactionResult> recoveredRecord = Optional.empty();
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getForKey,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, recoveredRecord, recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getForKey, context);

    // Assert
    verify(storage).get(getForKey);
    verify(recoveryExecutor)
        .execute(
            key,
            getForKey,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, recoveredRecord);
    verify(snapshot).putIntoGetSet(getForKey, recoveredRecord);
  }

  @Test
  public void
      readUnread_GetWithConjunctionGiven_GetNotContainedInGetSet_UncommittedRecordReturnedByStorage_RecoveredRecordMatchesConjunction_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getWithConjunction =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build();
    when(snapshot.containsKeyInGetSet(getWithConjunction)).thenReturn(false);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_3);
    when(recoveredResult.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3)));

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getWithConjunction,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getWithConjunction, context);

    // Assert
    verify(storage)
        .get(
            Get.newBuilder(getWithConjunction)
                .clearConditions()
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .build());
    verify(recoveryExecutor)
        .execute(
            key,
            getWithConjunction,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot).putIntoGetSet(getWithConjunction, Optional.of(recoveredResult));
  }

  @Test
  public void
      readUnread_GetWithConjunctionGiven_GetNotContainedInGetSet_UncommittedRecordReturnedByStorage_RecoveredRecordDoesNotMatchConjunction_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getWithConjunction =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build();
    when(snapshot.containsKeyInGetSet(getWithConjunction)).thenReturn(false);

    TransactionResult recoveredResult = mock(TransactionResult.class);

    when(recoveredResult.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_3));
    when(recoveredResult.getAsObject(ANY_NAME_3)).thenReturn(ANY_TEXT_4);
    when(recoveredResult.getColumns())
        .thenReturn(ImmutableMap.of(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_4)));

    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getWithConjunction,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getWithConjunction, context);

    // Assert
    verify(storage)
        .get(
            Get.newBuilder(getWithConjunction)
                .clearConditions()
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .build());
    verify(recoveryExecutor)
        .execute(
            key,
            getWithConjunction,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoGetSet(getWithConjunction, Optional.empty());
  }

  @Test
  public void
      readUnread_NullKeyAndGetWithIndexNotContainedInGetSet_EmptyResultReturnedByStorage_ShouldCallAppropriateMethods()
          throws CrudException, ExecutionException {
    // Arrange
    Get getWithIndex =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_3, ANY_TEXT_1))
            .build();
    when(snapshot.containsKeyInGetSet(getWithIndex)).thenReturn(false);
    when(storage.get(any())).thenReturn(Optional.empty());
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(null, getWithIndex, context);

    // Assert
    verify(storage).get(any());
    verify(snapshot, never()).putIntoReadSet(any(), any());
    verify(snapshot).putIntoGetSet(getWithIndex, Optional.empty());
  }

  @Test
  public void
      readUnread_NullKeyAndGetWithIndexNotContainedInGetSet_CommittedRecordReturnedByStorage_ShouldCallAppropriateMethods()
          throws CrudException, ExecutionException {
    // Arrange
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getColumns())
        .thenReturn(
            ImmutableMap.of(
                ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1),
                ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2)));
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getWithIndex =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_3, ANY_TEXT_1))
            .build();
    when(snapshot.containsKeyInGetSet(getWithIndex)).thenReturn(false);

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(null, getWithIndex, context);

    // Assert
    verify(storage).get(any());
    verify(snapshot)
        .putIntoReadSet(
            new Snapshot.Key(getWithIndex, result, TABLE_METADATA),
            Optional.of(new TransactionResult(result)));
    verify(snapshot).putIntoGetSet(getWithIndex, Optional.of(new TransactionResult(result)));
  }

  @Test
  public void
      readUnread_NullKeyAndGetWithIndexNotContainedInGetSet_UncommittedRecordReturnedByStorage_ShouldCallRecoveryExecutorWithReturnLatestResultAndRecover()
          throws ExecutionException, CrudException {
    // Arrange
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(result.getColumns())
        .thenReturn(
            ImmutableMap.of(
                ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1),
                ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2)));
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getWithIndex =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_3, ANY_TEXT_1))
            .build();
    when(snapshot.containsKeyInGetSet(getWithIndex)).thenReturn(false);

    Snapshot.Key key = new Snapshot.Key(getWithIndex, result, TABLE_METADATA);

    TransactionResult recoveredResult = mock(TransactionResult.class);
    @SuppressWarnings("unchecked")
    Future<Void> recoveryFuture = mock(Future.class);

    when(recoveryExecutor.execute(
            key,
            getWithIndex,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .thenReturn(new RecoveryExecutor.Result(key, Optional.of(recoveredResult), recoveryFuture));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readUnread(key, getWithIndex, context);

    // Assert
    verify(storage).get(getWithIndex);
    verify(recoveryExecutor)
        .execute(
            key,
            getWithIndex,
            new TransactionResult(result),
            ANY_ID_1,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    verify(snapshot).putIntoReadSet(key, Optional.of(recoveredResult));
    verify(snapshot).putIntoGetSet(getWithIndex, Optional.of(recoveredResult));
  }

  @Test
  public void readIfImplicitPreReadEnabled_ShouldCallAppropriateMethods()
      throws CrudException, ExecutionException, ValidationConflictException {
    // Arrange
    Key partitionKey1 = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key partitionKey2 = Key.ofText(ANY_NAME_1, ANY_TEXT_2);
    Key partitionKey3 = Key.ofText(ANY_NAME_1, ANY_TEXT_3);
    Key partitionKey4 = Key.ofText(ANY_NAME_1, ANY_TEXT_4);
    Key partitionKey5 = Key.ofText(ANY_NAME_1, ANY_TEXT_5);

    Put put1 = mock(Put.class);
    when(put1.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put1.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put1.getPartitionKey()).thenReturn(partitionKey1);
    when(put1.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .thenReturn(Optional.of("true"));

    Put put2 = mock(Put.class);
    when(put2.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put2.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put2.getPartitionKey()).thenReturn(partitionKey2);
    when(put2.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .thenReturn(Optional.of("true"));

    Put put3 = mock(Put.class);
    when(put3.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put3.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put3.getPartitionKey()).thenReturn(partitionKey3);

    Map<Snapshot.Key, Put> writeSet =
        ImmutableMap.of(
            new Snapshot.Key(put1), put1,
            new Snapshot.Key(put2), put2,
            new Snapshot.Key(put3), put3);
    when(snapshot.getWriteSet()).thenReturn(writeSet.entrySet());

    Delete delete1 = mock(Delete.class);
    when(delete1.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(delete1.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete1.getPartitionKey()).thenReturn(partitionKey4);

    Delete delete2 = mock(Delete.class);
    when(delete2.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(delete2.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete2.getPartitionKey()).thenReturn(partitionKey5);

    Map<Snapshot.Key, Delete> deleteSet =
        ImmutableMap.of(
            new Snapshot.Key(delete1), delete1,
            new Snapshot.Key(delete2), delete2);
    when(snapshot.getDeleteSet()).thenReturn(deleteSet.entrySet());

    Get get1 =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(partitionKey1)
                .build());

    Get get2 =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(partitionKey2)
                .build());

    Get get3 =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(partitionKey4)
                .build());

    Get get4 =
        toGetForStorageFrom(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(partitionKey5)
                .build());

    Result result1 = mock(Result.class);
    when(result1.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result1.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(result1.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_1);

    Result result2 = mock(Result.class);
    when(result2.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result2.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(result2.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_2);

    Result result3 = mock(Result.class);
    when(result3.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result3.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(result3.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_3);

    Result result4 = mock(Result.class);
    when(result4.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result4.getContainedColumnNames()).thenReturn(Collections.singleton(ANY_NAME_1));
    when(result4.getAsObject(ANY_NAME_1)).thenReturn(ANY_TEXT_4);

    when(storage.get(get1)).thenReturn(Optional.of(result1));
    when(storage.get(get2)).thenReturn(Optional.of(result2));
    when(storage.get(get3)).thenReturn(Optional.of(result3));
    when(storage.get(get4)).thenReturn(Optional.of(result4));

    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.readIfImplicitPreReadEnabled(context);

    // Assert
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ParallelExecutor.ParallelExecutorTask>> tasksCaptor =
        ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> transactionIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(parallelExecutor)
        .executeImplicitPreRead(tasksCaptor.capture(), transactionIdCaptor.capture());

    List<ParallelExecutor.ParallelExecutorTask> tasks = tasksCaptor.getValue();
    assertThat(tasks.size()).isEqualTo(4);

    for (ParallelExecutor.ParallelExecutorTask task : tasks) {
      task.run();
    }

    verify(storage).get(get1);
    verify(storage).get(get2);
    verify(storage).get(get3);
    verify(storage).get(get4);

    verify(snapshot)
        .putIntoReadSet(new Snapshot.Key(get1), Optional.of(new TransactionResult(result1)));
    verify(snapshot)
        .putIntoReadSet(new Snapshot.Key(get2), Optional.of(new TransactionResult(result2)));
    verify(snapshot)
        .putIntoReadSet(new Snapshot.Key(get3), Optional.of(new TransactionResult(result3)));
    verify(snapshot)
        .putIntoReadSet(new Snapshot.Key(get4), Optional.of(new TransactionResult(result4)));

    verify(snapshot).putIntoGetSet(get1, Optional.of(new TransactionResult(result1)));
    verify(snapshot).putIntoGetSet(get2, Optional.of(new TransactionResult(result2)));
    verify(snapshot).putIntoGetSet(get3, Optional.of(new TransactionResult(result3)));
    verify(snapshot).putIntoGetSet(get4, Optional.of(new TransactionResult(result4)));

    assertThat(transactionIdCaptor.getValue()).isEqualTo(ANY_ID_1);
  }

  @Test
  public void get_WithConjunctions_ShouldConvertConjunctions()
      throws CrudException, ExecutionException {
    // Arrange
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build(),
        context);
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .and(column(ANY_NAME_4).isEqualToInt(10))
            .build(),
        context);
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .or(column(ANY_NAME_4).isEqualToInt(20))
            .build(),
        context);
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(
                condition(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_3))
                    .and(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_4))
                    .build())
            .or(
                condition(column(ANY_NAME_4).isGreaterThanInt(30))
                    .and(column(ANY_NAME_4).isLessThanOrEqualToInt(40))
                    .build())
            .build(),
        context);
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(
                condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .or(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                    .build())
            .and(
                condition(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                    .or(column(ANY_NAME_4).isGreaterThanInt(60))
                    .build())
            .build(),
        context);
    handler.get(
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .where(column(ANY_NAME_3).isLikeText(ANY_TEXT_3))
            .or(column(ANY_NAME_3).isLikeText(ANY_TEXT_4))
            .build(),
        context);

    // Assert
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isEqualToInt(10))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isEqualToInt(10))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(ANY_NAME_4).isEqualToInt(20))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isEqualToInt(20))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(
                    condition(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_4))
                        .build())
                .or(
                    condition(column(ANY_NAME_4).isGreaterThanInt(30))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(40))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3)
                                .isNotEqualToText(ANY_TEXT_3))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3)
                                .isNotEqualToText(ANY_TEXT_4))
                        .build())
                .or(
                    condition(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(30))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(40))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .get(
            Get.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .where(column(ANY_NAME_3).isLikeText(ANY_TEXT_3))
                .or(column(ANY_NAME_3).isLikeText(ANY_TEXT_4))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isLikeText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isLikeText(ANY_TEXT_4))
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void scan_WithConjunctions_ShouldConvertConjunctions()
      throws CrudException, ExecutionException {
    // Arrange
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(result.getColumns())
        .thenReturn(
            ImmutableMap.of(
                ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1),
                ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2)));
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(any())).thenReturn(scanner);
    TransactionContext context =
        new TransactionContext(ANY_ID_1, snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .and(column(ANY_NAME_4).isEqualToInt(10))
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .or(column(ANY_NAME_4).isEqualToInt(20))
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(
                condition(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_3))
                    .and(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_4))
                    .build())
            .or(
                condition(column(ANY_NAME_4).isGreaterThanInt(30))
                    .and(column(ANY_NAME_4).isLessThanOrEqualToInt(40))
                    .build())
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(
                condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .or(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                    .build())
            .and(
                condition(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                    .or(column(ANY_NAME_4).isGreaterThanInt(60))
                    .build())
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(column(ANY_NAME_3).isLikeText(ANY_TEXT_3))
            .or(column(ANY_NAME_3).isLikeText(ANY_TEXT_4))
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(column(ANY_NAME_1).isGreaterThanText(ANY_TEXT_3))
            .and(column(ANY_NAME_2).isLessThanOrEqualToText(ANY_TEXT_4))
            .build(),
        context);
    handler.scan(
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(column(ANY_NAME_1).isGreaterThanText(ANY_TEXT_3))
            .and(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
            .build(),
        context);

    // Assert
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isEqualToInt(10))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isEqualToInt(10))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(ANY_NAME_4).isEqualToInt(20))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isEqualToInt(20))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(
                    condition(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_3).isNotEqualToText(ANY_TEXT_4))
                        .build())
                .or(
                    condition(column(ANY_NAME_4).isGreaterThanInt(30))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(40))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3)
                                .isNotEqualToText(ANY_TEXT_3))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3)
                                .isNotEqualToText(ANY_TEXT_4))
                        .build())
                .or(
                    condition(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(30))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(40))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isLessThanOrEqualToInt(50))
                        .build())
                .or(
                    condition(
                            column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_4).isGreaterThanInt(60))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .where(column(ANY_NAME_3).isLikeText(ANY_TEXT_3))
                .or(column(ANY_NAME_3).isLikeText(ANY_TEXT_4))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isLikeText(ANY_TEXT_3))
                .or(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isLikeText(ANY_TEXT_4))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .all()
                .where(column(ANY_NAME_1).isGreaterThanText(ANY_TEXT_3))
                .and(column(ANY_NAME_2).isLessThanOrEqualToText(ANY_TEXT_4))
                .consistency(Consistency.LINEARIZABLE)
                .build());
    verify(storage)
        .scan(
            Scan.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .all()
                .where(
                    condition(column(ANY_NAME_1).isGreaterThanText(ANY_TEXT_3))
                        .and(column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .build())
                .or(
                    condition(column(ANY_NAME_1).isGreaterThanText(ANY_TEXT_3))
                        .and(column(Attribute.BEFORE_PREFIX + ANY_NAME_3).isEqualToText(ANY_TEXT_4))
                        .build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  private List<Result> scanOrGetScanner(Scan scan, ScanType scanType, TransactionContext context)
      throws CrudException {
    if (scanType == ScanType.SCAN) {
      return handler.scan(scan, context);
    }

    try (TransactionCrudOperable.Scanner scanner = handler.getScanner(scan, context)) {
      switch (scanType) {
        case SCANNER_ONE:
          List<Result> results = new ArrayList<>();
          while (true) {
            Optional<Result> result = scanner.one();
            if (!result.isPresent()) {
              return results;
            }
            results.add(result.get());
          }
        case SCANNER_ALL:
          return scanner.all();
        default:
          throw new AssertionError();
      }
    }
  }

  enum ScanType {
    SCAN,
    SCANNER_ONE,
    SCANNER_ALL
  }
}
