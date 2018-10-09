package com.scalar.database.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Get;
import com.scalar.database.api.Result;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Scanner;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.transaction.CrudException;
import com.scalar.database.exception.transaction.CrudRuntimeException;
import com.scalar.database.exception.transaction.UncommittedRecordException;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** */
public class CrudHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  @InjectMocks private CrudHandler handler;
  @Mock private DistributedStorage storage;
  @Mock private Snapshot snapshot;
  @Mock private Scanner scanner;
  @Mock private Result result;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_KEYSPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    return new Scan(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private void configureResult(Result mock, boolean hasClusteringKey, TransactionState state) {
    when(mock.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1))));
    if (hasClusteringKey) {
      when(mock.getClusteringKey())
          .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2))));
    } else {
      when(mock.getClusteringKey()).thenReturn(Optional.empty());
    }

    ImmutableMap<String, Value> values =
        ImmutableMap.<String, Value>builder()
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_2))
            .put(Attribute.STATE, Attribute.toStateValue(state))
            .put(Attribute.VERSION, Attribute.toVersionValue(2))
            .put(Attribute.BEFORE_ID, Attribute.toBeforeIdValue(ANY_ID_1))
            .put(Attribute.BEFORE_STATE, Attribute.toBeforeStateValue(TransactionState.COMMITTED))
            .put(Attribute.BEFORE_VERSION, Attribute.toBeforeVersionValue(1))
            .build();

    when(mock.getValues()).thenReturn(values);
  }

  private TransactionResult prepareResult(boolean hasClusteringKey, TransactionState state) {
    Result result = mock(Result.class);
    configureResult(result, hasClusteringKey, state);
    return new TransactionResult(result);
  }

  @Test
  public void get_KeyExistsInSnapshot_ShouldReturnFromSnapshot() throws CrudException {
    // Arrange
    Get get = prepareGet();
    Optional<TransactionResult> expected =
        Optional.of(prepareResult(true, TransactionState.COMMITTED));
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
    Optional<Result> expected = Optional.of(prepareResult(true, TransactionState.COMMITTED));
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(Optional.empty());
    doNothing().when(snapshot).put(any(Snapshot.Key.class), any(TransactionResult.class));
    when(storage.get(get)).thenReturn(expected);

    // Act
    Optional<Result> result = handler.get(get);

    // Assert
    assertThat(result).isEqualTo(expected);
    verify(storage).get(get);
    verify(snapshot).put(new Snapshot.Key(get), (TransactionResult) expected.get());
  }

  @Test
  public void
      get_KeyNotExistsInSnapshotAndRecordInStorageNotCommitted_ShouldThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Get get = prepareGet();
    Optional<Result> expected = Optional.of(prepareResult(true, TransactionState.PREPARED));
    when(snapshot.get(new Snapshot.Key(get))).thenReturn(Optional.empty());
    when(storage.get(get)).thenReturn(expected);

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.get(get);
            })
        .isInstanceOf(UncommittedRecordException.class);
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
    assertThatThrownBy(
            () -> {
              handler.get(get);
            })
        .isInstanceOf(CrudException.class)
        .hasCause(toThrow);
  }

  @Test
  public void scan_ResultGivenFromStorage_ShouldUpdateSnapshotAndReturn()
      throws ExecutionException, CrudException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(true, TransactionState.COMMITTED);
    when(snapshot.get(any(Snapshot.Key.class))).thenReturn(Optional.empty());
    doNothing().when(snapshot).put(any(Snapshot.Key.class), any(TransactionResult.class));
    when(scanner.iterator()).thenReturn(Arrays.asList(result).iterator());
    // doCallRealMethod().when(scanner).forEach(any(Consumer.class));
    when(storage.scan(scan)).thenReturn(scanner);

    // Act
    List<Result> results = handler.scan(scan);

    // Assert
    Snapshot.Key key =
        new Snapshot.Key(
            scan.forNamespace().get(),
            scan.forTable().get(),
            scan.getPartitionKey(),
            result.getClusteringKey().get());
    TransactionResult expected = new TransactionResult(result);
    verify(snapshot).put(key, expected);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(expected);
  }

  @Test
  public void
      scan_PreparedResultGivenFromStorage_ShouldNeverUpdateSnapshotThrowUncommittedRecordException()
          throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(false, TransactionState.PREPARED);
    when(scanner.iterator()).thenReturn(Arrays.asList(result).iterator());
    when(storage.scan(scan)).thenReturn(scanner);

    // Act
    assertThatThrownBy(
            () -> {
              handler.scan(scan);
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(snapshot, never()).put(any(Snapshot.Key.class), any(TransactionResult.class));
  }

  @Test
  public void scan_ClusteringKeyNotGivenInResult_ShouldThrowRuntimeException()
      throws ExecutionException {
    // Arrange
    Scan scan = prepareScan();
    result = prepareResult(false, TransactionState.COMMITTED);
    when(scanner.iterator()).thenReturn(Arrays.asList(result).iterator());
    // this is needed for forEach
    // doCallRealMethod().when(scanner).forEach(any(Consumer.class));
    when(storage.scan(scan)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.scan(scan);
            })
        .isInstanceOf(CrudRuntimeException.class);
  }
}
