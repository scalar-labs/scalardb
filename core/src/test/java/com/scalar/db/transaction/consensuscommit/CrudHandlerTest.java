package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CrudHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TX_ID = "tx_id";

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());
  private static final TransactionTableMetadata TRANSACTION_TABLE_METADATA =
      new TransactionTableMetadata(TABLE_METADATA);

  private CrudHandler handler;
  @Mock private DistributedStorage storage;
  @Mock private Snapshot snapshot;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private Scanner scanner;
  @Mock private Result result;
  @Mock private MutationConditionsValidator mutationConditionsValidator;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    handler =
        new CrudHandler(
            storage,
            snapshot,
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
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get toGetForStorageFrom(Get get) {
    return Get.newBuilder(get)
        .clearProjections()
        .projections(TRANSACTION_TABLE_METADATA.getAfterImageColumnNames())
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private Scan prepareCrossPartitionScan() {
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .all()
        .where(ConditionBuilder.column("column").isEqualToInt(10))
        .build();
  }

  private Scan toScanForStorageFrom(Scan scan) {
    return Scan.newBuilder(scan)
        .clearProjections()
        .projections(TRANSACTION_TABLE_METADATA.getAfterImageColumnNames())
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private TransactionResult prepareResult(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID_2)))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(Attribute.toStateValue(state)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(2)))
            .put(Attribute.BEFORE_ID, ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
            .put(
                Attribute.BEFORE_STATE,
                ScalarDbUtils.toColumn(Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
            .put(
                Attribute.BEFORE_VERSION, ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(1)))
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

    // Act
    Optional<Result> actual = handler.get(get);

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

    // Act
    Optional<Result> result = handler.get(get);

    // Assert
    assertThat(result)
        .isEqualTo(
            Optional.of(
                new FilteredResult(
                    expected.get(), Collections.emptyList(), TABLE_METADATA, false)));
    verify(storage).get(getForStorage);
    verify(snapshot).putIntoReadSet(key, Optional.of((TransactionResult) expected.get()));
    verify(snapshot).putIntoGetSet(get, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void
      get_GetNotExistsInSnapshotAndRecordInStorageNotCommitted_ShouldThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    result = prepareResult(TransactionState.PREPARED);
    Optional<Result> expected = Optional.of(result);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(expected);

    // Act Assert
    assertThatThrownBy(() -> handler.get(get))
        .isInstanceOf(UncommittedRecordException.class)
        .satisfies(
            e -> {
              UncommittedRecordException exception = (UncommittedRecordException) e;
              assertThat(exception.getSelection()).isEqualTo(get);
              assertThat(exception.getResults().size()).isEqualTo(1);
              assertThat(exception.getResults().get(0)).isEqualTo(result);
            });

    verify(snapshot, never()).putIntoReadSet(any(), ArgumentMatchers.any());
    verify(snapshot, never()).putIntoGetSet(any(), ArgumentMatchers.any());
  }

  @Test
  public void get_GetNotExistsInSnapshotAndRecordNotExistsInStorage_ShouldReturnEmpty()
      throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Get getForStorage = toGetForStorageFrom(get);
    when(snapshot.containsKeyInGetSet(getForStorage)).thenReturn(false);
    when(storage.get(getForStorage)).thenReturn(Optional.empty());

    // Act
    Optional<Result> result = handler.get(get);

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

    // Act Assert
    assertThatThrownBy(() -> handler.get(get)).isInstanceOf(CrudException.class).hasCause(toThrow);
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

    // Act
    Optional<Result> results1 = handler.get(get1);
    Optional<Result> results2 = handler.get(get2);

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
  public void get_CalledTwiceUnderRealSnapshot_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Get originalGet = prepareGet();
    Get getForStorage = toGetForStorageFrom(originalGet);
    Get get1 = prepareGet();
    Get get2 = prepareGet();
    Result result = prepareResult(TransactionState.COMMITTED);
    Optional<TransactionResult> expected = Optional.of(new TransactionResult(result));
    snapshot =
        new Snapshot(ANY_TX_ID, Isolation.SNAPSHOT, null, tableMetadataManager, parallelExecutor);
    handler = new CrudHandler(storage, snapshot, tableMetadataManager, false, parallelExecutor);
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> results1 = handler.get(get1);
    Optional<Result> results2 = handler.get(get2);

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

    when(tableMetadataManager.getTransactionTableMetadata(get)).thenReturn(null);

    // Act Assert
    assertThatThrownBy(() -> handler.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ResultGivenFromStorage_ShouldUpdateSnapshotAndReturn()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    TransactionResult expected = new TransactionResult(result);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    when(snapshot.getResult(any())).thenReturn(Optional.of(expected));

    // Act
    List<Result> results = handler.scan(scan);

    // Assert
    verify(snapshot).putIntoReadSet(key, Optional.of(expected));
    verify(snapshot).putIntoScanSet(scan, ImmutableMap.of(key, expected));
    verify(snapshot).verify(scan);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void
      scan_PreparedResultGivenFromStorage_ShouldNeverUpdateSnapshotThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.PREPARED);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.scan(scan))
        .isInstanceOf(UncommittedRecordException.class)
        .satisfies(
            e -> {
              UncommittedRecordException exception = (UncommittedRecordException) e;
              assertThat(exception.getSelection()).isEqualTo(scan);
              assertThat(exception.getResults().size()).isEqualTo(1);
              assertThat(exception.getResults().get(0)).isEqualTo(result);
            });

    verify(snapshot, never()).putIntoReadSet(any(), ArgumentMatchers.any());
    verify(snapshot, never()).putIntoScanSet(any(), ArgumentMatchers.any());
  }

  @Test
  public void scan_CalledTwice_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan originalScan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(originalScan);
    Scan scan1 = prepareScan();
    Scan scan2 = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    TransactionResult expected = new TransactionResult(result);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    Snapshot.Key key = new Snapshot.Key(scanForStorage, result);
    when(snapshot.getResults(scanForStorage))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(ImmutableMap.of(key, expected)));
    when(snapshot.getResult(key)).thenReturn(Optional.of(expected));

    // Act
    List<Result> results1 = handler.scan(scan1);
    List<Result> results2 = handler.scan(scan2);

    // Assert
    verify(snapshot).putIntoReadSet(key, Optional.of(expected));
    verify(snapshot).putIntoScanSet(scanForStorage, ImmutableMap.of(key, expected));
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
    assertThat(results1).isEqualTo(results2);
    verify(storage, never()).scan(originalScan);
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void scan_CalledTwiceUnderRealSnapshot_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan originalScan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(originalScan);
    Scan scan1 = prepareScan();
    Scan scan2 = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    TransactionResult expected = new TransactionResult(result);
    snapshot =
        new Snapshot(ANY_TX_ID, Isolation.SNAPSHOT, null, tableMetadataManager, parallelExecutor);
    handler = new CrudHandler(storage, snapshot, tableMetadataManager, false, parallelExecutor);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act
    List<Result> results1 = handler.scan(scan1);
    List<Result> results2 = handler.scan(scan2);

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0))
        .isEqualTo(new FilteredResult(expected, Collections.emptyList(), TABLE_METADATA, false));
    assertThat(results1).isEqualTo(results2);
    verify(storage, never()).scan(originalScan);
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void scan_GetCalledAfterScan_ShouldReturnFromStorage()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    Get getForStorage = toGetForStorageFrom(get);
    Optional<TransactionResult> transactionResult = Optional.of(new TransactionResult(result));
    when(storage.get(getForStorage)).thenReturn(Optional.of(result));
    when(snapshot.getResult(key, get)).thenReturn(transactionResult);
    when(snapshot.getResult(key)).thenReturn(transactionResult);

    // Act
    List<Result> results = handler.scan(scan);
    Optional<Result> result = handler.get(get);

    // Assert
    verify(storage).scan(scanForStorage);

    verify(storage).get(getForStorage);
    assertThat(results.size()).isEqualTo(1);
    assertThat(result).isPresent();
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @Test
  public void scan_GetCalledAfterScanUnderRealSnapshot_ShouldReturnFromStorage()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = toScanForStorageFrom(prepareScan());
    result = prepareResult(TransactionState.COMMITTED);
    snapshot =
        new Snapshot(ANY_TX_ID, Isolation.SNAPSHOT, null, tableMetadataManager, parallelExecutor);
    handler = new CrudHandler(storage, snapshot, tableMetadataManager, false, parallelExecutor);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);
    Get get = prepareGet();
    when(storage.get(get)).thenReturn(Optional.of(result));

    // Act
    List<Result> results = handler.scan(scan);
    Optional<Result> result = handler.get(get);

    // Assert
    verify(storage).scan(scan);
    verify(storage).get(get);
    assertThat(results.size()).isEqualTo(1);
    assertThat(result).isPresent();
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @Test
  public void scan_CalledAfterDeleteUnderRealSnapshot_ShouldReturnResultsWithoutDeletedRecord()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    Scan scanForStorage = toScanForStorageFrom(scan);
    result = prepareResult(TransactionState.COMMITTED);

    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_3))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID_2)))
            .put(
                Attribute.STATE,
                ScalarDbUtils.toColumn(Attribute.toStateValue(TransactionState.COMMITTED)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(2)))
            .put(Attribute.BEFORE_ID, ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
            .put(
                Attribute.BEFORE_STATE,
                ScalarDbUtils.toColumn(Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
            .put(
                Attribute.BEFORE_VERSION, ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(1)))
            .build();
    Result result2 = new ResultImpl(columns, TABLE_METADATA);

    ConcurrentMap<Snapshot.Key, Optional<TransactionResult>> readSet = new ConcurrentHashMap<>();
    Map<Snapshot.Key, Delete> deleteSet = new HashMap<>();
    snapshot =
        new Snapshot(
            ANY_TX_ID,
            Isolation.SNAPSHOT,
            null,
            tableMetadataManager,
            parallelExecutor,
            readSet,
            new ConcurrentHashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            deleteSet);
    handler = new CrudHandler(storage, snapshot, tableMetadataManager, false, parallelExecutor);
    when(scanner.iterator()).thenReturn(Arrays.asList(result, result2).iterator());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    Delete delete =
        new Delete(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_3))
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act
    handler.delete(delete);
    List<Result> results = handler.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false));

    // check the delete set
    assertThat(deleteSet.size()).isEqualTo(1);
    assertThat(deleteSet).containsKey(new Snapshot.Key(delete));

    // check if the scanned data is inserted correctly in the read set
    assertThat(readSet.size()).isEqualTo(2);
    Snapshot.Key key1 = new Snapshot.Key(scan, result);
    assertThat(readSet.get(key1).isPresent()).isTrue();
    assertThat(readSet.get(key1).get()).isEqualTo(new TransactionResult(result));
    Snapshot.Key key2 = new Snapshot.Key(scan, result2);
    assertThat(readSet.get(key2).isPresent()).isTrue();
    assertThat(readSet.get(key2).get()).isEqualTo(new TransactionResult(result2));
  }

  @Test
  public void
      scan_CrossPartitionScanAndResultFromStorageGiven_ShouldUpdateSnapshotAndValidateThenReturn()
          throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareCrossPartitionScan();
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(any(ScanAll.class))).thenReturn(scanner);
    TransactionResult transactionResult = new TransactionResult(result);
    when(snapshot.getResult(key)).thenReturn(Optional.of(transactionResult));

    // Act
    List<Result> results = handler.scan(scan);

    // Assert
    verify(snapshot).putIntoReadSet(key, Optional.of(transactionResult));
    verify(snapshot).putIntoScanSet(scan, ImmutableMap.of(key, transactionResult));
    verify(snapshot).verify(scan);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0))
        .isEqualTo(
            new FilteredResult(transactionResult, Collections.emptyList(), TABLE_METADATA, false));
  }

  @Test
  public void
      scan_CrossPartitionScanAndPreparedResultFromStorageGiven_ShouldNeverUpdateSnapshotNorValidateButThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareCrossPartitionScan();
    result = prepareResult(TransactionState.PREPARED);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(any(ScanAll.class))).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> handler.scan(scan))
        .isInstanceOf(UncommittedRecordException.class)
        .satisfies(
            e -> {
              UncommittedRecordException exception = (UncommittedRecordException) e;
              assertThat(exception.getSelection()).isEqualTo(scan);
              assertThat(exception.getResults().size()).isEqualTo(1);
              assertThat(exception.getResults().get(0)).isEqualTo(result);
            });

    verify(snapshot, never()).putIntoReadSet(any(Snapshot.Key.class), ArgumentMatchers.any());
    verify(snapshot, never()).verify(any());
  }

  @Test
  public void put_PutWithoutConditionGiven_ShouldCallAppropriateMethods() throws CrudException {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "foo")).build();

    CrudHandler spied = spy(handler);

    // Act
    spied.put(put);

    // Assert
    verify(spied, never()).readUnread(any(), any());
    verify(snapshot, never()).getResult(any());
    verify(mutationConditionsValidator, never()).checkIfConditionIsSatisfied(any(Put.class), any());
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
            .condition(ConditionBuilder.putIfExists())
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

    // Act
    spied.put(put);

    // Assert
    verify(spied, never()).readUnread(key, getForKey);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result);
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
            .condition(ConditionBuilder.putIfExists())
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
    doReturn(Optional.empty()).when(spied).getFromStorage(getForKey);

    // Act
    spied.put(put);

    // Assert
    verify(spied).read(key, getForKey);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result);
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
            .condition(ConditionBuilder.putIfExists())
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

    // Act
    spied.put(put);

    // Assert
    verify(spied, never()).readUnread(key, getForKey);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(put, result);
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
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act Assert
    assertThatThrownBy(() -> handler.put(put)).isInstanceOf(IllegalArgumentException.class);
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

    // Act
    spied.delete(delete);

    // Assert
    verify(spied, never()).readUnread(any(), any());
    verify(snapshot, never()).getResult(any());
    verify(mutationConditionsValidator, never())
        .checkIfConditionIsSatisfied(any(Delete.class), any());
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
            .condition(ConditionBuilder.deleteIfExists())
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

    // Act
    spied.delete(delete);

    // Assert
    verify(spied, never()).readUnread(key, getForKey);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(delete, result);
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
            .condition(ConditionBuilder.deleteIfExists())
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
    doReturn(Optional.empty()).when(spied).getFromStorage(getForKey);

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).read(key, getForKey);
    verify(snapshot).getResult(key);
    verify(mutationConditionsValidator).checkIfConditionIsSatisfied(delete, null);
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

    // Act
    handler.readUnread(key, getForKey);

    // Assert
    verify(storage, never()).get(any());
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

    // Act
    handler.readUnread(key, getForKey);

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
            .where(mock(ConditionalExpression.class))
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);
    when(storage.get(any())).thenReturn(Optional.empty());

    // Act
    handler.readUnread(key, getForKey);

    // Assert
    verify(storage).get(any());
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

    Result result = mock(Result.class);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.COMMITTED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);

    // Act
    handler.readUnread(key, getForKey);

    // Assert
    verify(storage).get(any());
    verify(snapshot).putIntoReadSet(key, Optional.of(new TransactionResult(result)));
  }

  @Test
  public void
      readUnread_GetNotContainedInGetSet_UncommittedRecordReturnedByStorage_ShouldThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Snapshot.Key key = mock(Snapshot.Key.class);
    when(key.getNamespace()).thenReturn(ANY_NAMESPACE_NAME);
    when(key.getTable()).thenReturn(ANY_TABLE_NAME);
    when(key.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    Result result = mock(Result.class);
    when(result.getInt(Attribute.STATE)).thenReturn(TransactionState.PREPARED.get());
    when(storage.get(any())).thenReturn(Optional.of(result));

    Get getForKey =
        Get.newBuilder()
            .namespace(key.getNamespace())
            .table(key.getTable())
            .partitionKey(key.getPartitionKey())
            .build();
    when(snapshot.containsKeyInGetSet(getForKey)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> handler.readUnread(key, getForKey))
        .isInstanceOf(UncommittedRecordException.class)
        .satisfies(
            e -> {
              UncommittedRecordException exception = (UncommittedRecordException) e;
              assertThat(exception.getSelection()).isEqualTo(getForKey);
              assertThat(exception.getResults().size()).isEqualTo(1);
              assertThat(exception.getResults().get(0)).isEqualTo(result);
            });
  }

  @Test
  public void readIfImplicitPreReadEnabled_ShouldCallAppropriateMethods() throws CrudException {
    // Arrange
    Put put1 = mock(Put.class);
    when(put1.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put1.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put1.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));
    when(put1.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .thenReturn(Optional.of("true"));

    Put put2 = mock(Put.class);
    when(put2.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put2.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put2.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_2));
    when(put2.getAttribute(ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED))
        .thenReturn(Optional.of("true"));

    Put put3 = mock(Put.class);
    when(put3.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(put3.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put3.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_3));

    when(snapshot.getPutsInWriteSet()).thenReturn(Arrays.asList(put1, put2, put3));

    Delete delete1 = mock(Delete.class);
    when(delete1.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(delete1.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete1.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_1));

    Delete delete2 = mock(Delete.class);
    when(delete2.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(delete2.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete2.getPartitionKey()).thenReturn(Key.ofText(ANY_NAME_1, ANY_TEXT_2));

    when(snapshot.getDeletesInDeleteSet()).thenReturn(Arrays.asList(delete1, delete2));

    when(snapshot.getId()).thenReturn(ANY_TX_ID);

    // Act
    handler.readIfImplicitPreReadEnabled();

    // Assert
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ParallelExecutor.ParallelExecutorTask>> tasksCaptor =
        ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> transactionIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(parallelExecutor)
        .executeImplicitPreRead(tasksCaptor.capture(), transactionIdCaptor.capture());

    List<ParallelExecutor.ParallelExecutorTask> tasks = tasksCaptor.getValue();
    assertThat(tasks.size()).isEqualTo(4);

    assertThat(transactionIdCaptor.getValue()).isEqualTo(ANY_TX_ID);
  }
}
