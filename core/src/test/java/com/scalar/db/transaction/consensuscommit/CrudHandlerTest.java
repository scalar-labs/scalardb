package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CrudHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TX_ID = "tx_id";

  @Mock private DistributedStorage storage;
  @Mock private Snapshot snapshot;
  @Mock private RecoveryHandler recovery;
  @Mock private TransactionalTableMetadataManager tableMetadataManager;
  @Mock private ParallelExecutor parallelExecutor;
  @Mock private Scanner scanner;
  @Mock private Result result;

  @InjectMocks private CrudHandler handler;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(tableMetadataManager.getTransactionalTableMetadata(any()))
        .thenReturn(
            new TransactionalTableMetadata(
                ConsensusCommitUtils.buildTransactionalTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn(ANY_NAME_1, DataType.TEXT)
                        .addColumn(ANY_NAME_2, DataType.TEXT)
                        .addPartitionKey(ANY_NAME_1)
                        .addClusteringKey(ANY_NAME_2)
                        .build())));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_KEYSPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private TransactionResult prepareResult(TransactionState state) {
    Result result = mock(Result.class);
    when(result.getPartitionKey()).thenReturn(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
    when(result.getClusteringKey()).thenReturn(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));
    ImmutableMap<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_2))
            .put(Attribute.STATE, Attribute.toStateValue(state))
            .put(Attribute.VERSION, Attribute.toVersionValue(2))
            .build();
    when(result.getValues()).thenReturn(values);
    return new TransactionResult(result);
  }

  @Test
  public void get_KeyExistsInSnapshot_ShouldReturnFromSnapshot() throws CrudException {
    // Arrange
    Get get = prepareGet();
    Optional<TransactionResult> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    when(snapshot.containsKeyInReadSet(new Snapshot.Key(get))).thenReturn(true);
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(expected);

    // Act
    Optional<Result> actual = handler.get(get);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void
      get_KeyNotExistsInSnapshotAndRecordInStorageCommitted_ShouldReturnFromStorageAndUpdateSnapshot()
          throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    Optional<Result> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    Snapshot.Key key = new Snapshot.Key(get);
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false);
    doNothing()
        .when(snapshot)
        .put(any(Snapshot.Key.class), ArgumentMatchers.<Optional<TransactionResult>>any());
    when(storage.get(get)).thenReturn(expected);
    when(snapshot.get(key)).thenReturn(expected.map(e -> (TransactionResult) e));

    // Act
    Optional<Result> result = handler.get(get);

    // Assert
    assertThat(result).isEqualTo(expected);
    verify(storage).get(get);
    verify(snapshot).put(key, Optional.of((TransactionResult) expected.get()));
  }

  @Test
  public void get_KeyNotExistsInSnapshotAndRecordInStorageNotCommitted_ShouldRecoverRecordProperly()
      throws ExecutionException, RecoveryException, CrudException {
    // Arrange
    Get get = prepareGet();
    Optional<Result> result = Optional.of(prepareResult(TransactionState.PREPARED));
    when(storage.get(get)).thenReturn(result);
    Optional<TransactionResult> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    when(recovery.recover(get, (TransactionResult) result.get())).thenReturn(expected);
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(expected);

    // Act
    Optional<Result> actual = handler.get(get);

    // Assert
    assertThat(actual).isEqualTo(expected);
    verify(recovery).recover(get, (TransactionResult) result.get());
  }

  @Test
  public void get_KeyNeitherExistsInSnapshotNorInStorage_ShouldReturnEmpty()
      throws CrudException, ExecutionException {
    // Arrange
    Get get = prepareGet();
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(Optional.empty());
    when(storage.get(get)).thenReturn(Optional.empty());

    // Act
    Optional<Result> result = handler.get(get);

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void get_KeyNotExistsInCrudSetAndExceptionThrownInStorage_ShouldThrowCrudException()
      throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(Optional.empty());
    ExecutionException toThrow = mock(ExecutionException.class);
    when(storage.get(get)).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(() -> handler.get(get)).isInstanceOf(CrudException.class).hasCause(toThrow);
  }

  @Test
  public void scan_ResultGivenFromStorage_ShouldUpdateSnapshotAndReturn()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(snapshot.get(key)).thenReturn(Optional.of((TransactionResult) result));
    doNothing()
        .when(snapshot)
        .put(any(Snapshot.Key.class), ArgumentMatchers.<Optional<TransactionResult>>any());
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    // Act
    List<Result> results = handler.scan(scan);

    // Assert
    TransactionResult expected = new TransactionResult(result);
    verify(snapshot).put(key, Optional.of(expected));
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(expected);
  }

  @Test
  public void scan_PreparedResultGivenFromStorage_ShouldRecoverRecordProperly()
      throws ExecutionException, RecoveryException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.PREPARED);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    Optional<TransactionResult> expected = Optional.of(prepareResult(TransactionState.COMMITTED));
    when(recovery.recover(scan, (TransactionResult) result)).thenReturn(expected);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(snapshot.get(key)).thenReturn(expected);

    // Act
    List<Result> actual = handler.scan(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0)).isEqualTo(expected.get());
    verify(recovery).recover(scan, (TransactionResult) result);
    verify(snapshot).put(key, expected);
    verify(snapshot).put(scan, Collections.singletonList(key));
  }

  @Test
  public void
      scan_PreparedResultGivenFromStorageAndRecordDeletedAfterRecovery_ShouldRecoverRecordProperlyAndResultsShouldBeEmpty()
          throws ExecutionException, RecoveryException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.PREPARED);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    Optional<TransactionResult> expected = Optional.empty();
    when(recovery.recover(scan, (TransactionResult) result)).thenReturn(expected);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(snapshot.get(key)).thenReturn(expected);

    // Act
    List<Result> actual = handler.scan(scan);

    // Assert
    assertThat(actual).isEmpty();
    verify(recovery).recover(scan, (TransactionResult) result);
    verify(snapshot, never())
        .put(any(Snapshot.Key.class), ArgumentMatchers.<Optional<TransactionResult>>any());
    verify(snapshot).put(scan, Collections.emptyList());
  }

  @Test
  public void scan_CalledTwice_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    doNothing()
        .when(snapshot)
        .put(any(Snapshot.Key.class), ArgumentMatchers.<Optional<TransactionResult>>any());
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(snapshot.get(scan))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(Collections.singletonList(key)));
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false).thenReturn(true);
    when(snapshot.get(key)).thenReturn(Optional.of((TransactionResult) result));

    // Act
    List<Result> results1 = handler.scan(scan);
    List<Result> results2 = handler.scan(scan);

    // Assert
    TransactionResult expected = new TransactionResult(result);
    verify(snapshot).put(key, Optional.of(expected));
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(expected);
    assertThat(results1).isEqualTo(results2);
  }

  @Test
  public void scan_CalledTwiceUnderRealSnapshot_SecondTimeShouldReturnTheSameFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    snapshot = new Snapshot(ANY_TX_ID, Isolation.SNAPSHOT, null, parallelExecutor);
    handler = new CrudHandler(storage, snapshot, recovery, tableMetadataManager);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    // Act
    List<Result> results1 = handler.scan(scan);
    List<Result> results2 = handler.scan(scan);

    // Assert
    TransactionResult expected = new TransactionResult(result);
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(expected);
    assertThat(results1).isEqualTo(results2);
  }

  @Test
  public void scan_GetCalledAfterScan_ShouldReturnFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    doNothing()
        .when(snapshot)
        .put(any(Snapshot.Key.class), ArgumentMatchers.<Optional<TransactionResult>>any());
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    when(snapshot.get(scan)).thenReturn(Optional.empty());
    when(snapshot.containsKeyInReadSet(key)).thenReturn(false).thenReturn(true);
    when(snapshot.get(key)).thenReturn(Optional.of((TransactionResult) result));

    // Act
    List<Result> results = handler.scan(scan);
    Optional<Result> result = handler.get(prepareGet());

    // Assert
    verify(storage, never()).get(any(Get.class));
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @Test
  public void scan_GetCalledAfterScanUnderRealSnapshot_ShouldReturnFromSnapshot()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);
    snapshot = new Snapshot(ANY_TX_ID, Isolation.SNAPSHOT, null, parallelExecutor);
    handler = new CrudHandler(storage, snapshot, recovery, tableMetadataManager);
    when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    // Act
    List<Result> results = handler.scan(scan);
    Optional<Result> result = handler.get(prepareGet());

    // Assert
    assertThat(results.get(0)).isEqualTo(result.get());
  }

  @Test
  public void scan_CalledAfterDeleteUnderRealSnapshot_ShouldReturnResultsWithoutDeletedRecord()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(TransactionState.COMMITTED);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    Result result2 = mock(Result.class);
    when(result2.getPartitionKey()).thenReturn(Optional.of(partitionKey));
    when(result2.getClusteringKey()).thenReturn(Optional.of(clusteringKey));
    ImmutableMap<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_2))
            .put(Attribute.STATE, Attribute.toStateValue(TransactionState.COMMITTED))
            .put(Attribute.VERSION, Attribute.toVersionValue(2))
            .put(Attribute.BEFORE_ID, Attribute.toBeforeIdValue(ANY_ID_1))
            .put(Attribute.BEFORE_STATE, Attribute.toBeforeStateValue(TransactionState.COMMITTED))
            .put(Attribute.BEFORE_VERSION, Attribute.toBeforeVersionValue(1))
            .build();
    when(result2.getValues()).thenReturn(values);

    Map<Snapshot.Key, Optional<TransactionResult>> readSet = new HashMap<>();
    Map<Snapshot.Key, Delete> deleteSet = new HashMap<>();
    snapshot =
        new Snapshot(
            ANY_TX_ID,
            Isolation.SNAPSHOT,
            null,
            parallelExecutor,
            readSet,
            new HashMap<>(),
            new HashMap<>(),
            deleteSet);
    handler = new CrudHandler(storage, snapshot, recovery, tableMetadataManager);
    when(scanner.iterator()).thenReturn(Arrays.asList(result, result2).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act
    handler.delete(delete);
    List<Result> results = handler.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(result);

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
}
