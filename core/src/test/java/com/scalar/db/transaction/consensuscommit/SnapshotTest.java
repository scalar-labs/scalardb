package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionSetBuilder;
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
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.transaction.consensuscommit.Snapshot.ReadWriteSets;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SnapshotTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_NAMESPACE_NAME_2 = "namespace2";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_TABLE_NAME_2 = "table2";
  private static final String ANY_ID = "id";
  private static final int ANY_VERSION = 1;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_NAME_7 = "name7";
  private static final String ANY_NAME_8 = "name8";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TEXT_5 = "text5";
  private static final String ANY_TEXT_6 = "text6";
  private static final int ANY_INT_0 = 0;
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.TEXT)
              .addColumn(ANY_NAME_4, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  private Snapshot snapshot;
  private ConcurrentMap<Snapshot.Key, Optional<TransactionResult>> readSet;
  private ConcurrentMap<Get, Optional<TransactionResult>> getSet;
  private Map<Scan, Map<Snapshot.Key, TransactionResult>> scanSet;
  private Map<Snapshot.Key, Put> writeSet;
  private Map<Snapshot.Key, Delete> deleteSet;

  @Mock private ConsensusCommitConfig config;
  @Mock private PrepareMutationComposer prepareComposer;
  @Mock private CommitMutationComposer commitComposer;
  @Mock private RollbackMutationComposer rollbackComposer;
  @Mock private TransactionTableMetadataManager tableMetadataManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    when(tableMetadataManager.getTransactionTableMetadata(any(), any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private Snapshot prepareSnapshot(Isolation isolation) {
    return prepareSnapshot(isolation, SerializableStrategy.EXTRA_WRITE);
  }

  private Snapshot prepareSnapshot(Isolation isolation, SerializableStrategy strategy) {
    readSet = new ConcurrentHashMap<>();
    getSet = new ConcurrentHashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();

    return spy(
        new Snapshot(
            ANY_ID,
            isolation,
            strategy,
            tableMetadataManager,
            new ParallelExecutor(config),
            readSet,
            getSet,
            scanSet,
            writeSet,
            deleteSet));
  }

  private TransactionResult prepareResult(String txId) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, ScalarDbUtils.toColumn(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .put(ANY_NAME_2, ScalarDbUtils.toColumn(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
            .put(ANY_NAME_3, ScalarDbUtils.toColumn(new TextValue(ANY_NAME_3, ANY_TEXT_3)))
            .put(ANY_NAME_4, ScalarDbUtils.toColumn(new TextValue(ANY_NAME_4, ANY_TEXT_4)))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(txId)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION)))
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
            .put(Attribute.VERSION, IntColumn.ofNull(Attribute.VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareAnotherGet() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Scan(partitionKey)
        .withStart(clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareCrossPartitionScan() {
    return prepareCrossPartitionScan(ANY_NAMESPACE_NAME, ANY_TABLE_NAME);
  }

  private Scan prepareCrossPartitionScan(String namespace, String table) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .all()
        .where(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
        .build();
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .withValue(ANY_NAME_4, ANY_TEXT_4);
  }

  private Put prepareAnotherPut() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePutWithPartitionKeyOnly() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Put(partitionKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .withValue(ANY_NAME_4, ANY_TEXT_4);
  }

  private Put preparePutWithIntColumns() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofInt(ANY_NAME_1, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_2, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_3, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_4, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_5, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_6, ANY_INT_1))
        .value(IntColumn.of(ANY_NAME_7, ANY_INT_1))
        .value(IntColumn.ofNull(ANY_NAME_8))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePutForMergeTest() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .textValue(ANY_NAME_3, ANY_TEXT_5)
        .textValue(ANY_NAME_4, null)
        .build();
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Delete prepareAnotherDelete() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Delete(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private void configureBehavior() throws ExecutionException {
    doNothing().when(prepareComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(prepareComposer).add(any(Delete.class), any(TransactionResult.class));
    doNothing().when(commitComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(commitComposer).add(any(Delete.class), any(TransactionResult.class));
    doNothing().when(rollbackComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(rollbackComposer).add(any(Delete.class), any(TransactionResult.class));
  }

  @Test
  public void putIntoReadSet_ResultGiven_ShouldHoldWhatsGivenInReadSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);

    // Act
    snapshot.putIntoReadSet(key, Optional.of(result));

    // Assert
    assertThat(readSet.get(key)).isEqualTo(Optional.of(result));
  }

  @Test
  public void putIntoGetSet_ResultGiven_ShouldHoldWhatsGivenInReadSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Get get = prepareGet();
    TransactionResult result = prepareResult(ANY_ID);

    // Act
    snapshot.putIntoGetSet(get, Optional.of(result));

    // Assert
    assertThat(getSet.get(get)).isEqualTo(Optional.of(result));
  }

  @Test
  public void putIntoWriteSet_PutGiven_ShouldHoldWhatsGivenInWriteSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(put);

    // Act
    snapshot.putIntoWriteSet(key, put);

    // Assert
    assertThat(writeSet.get(key)).isEqualTo(put);
  }

  @Test
  public void putIntoWriteSet_PutGivenTwice_ShouldHoldMergedPut() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put1 = preparePut();
    Snapshot.Key key = new Snapshot.Key(put1);

    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Put put2 =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue(ANY_NAME_3, ANY_TEXT_5)
            .textValue(ANY_NAME_4, null)
            .enableImplicitPreRead()
            .build();

    // Act
    snapshot.putIntoWriteSet(key, put1);
    snapshot.putIntoWriteSet(key, put2);

    // Assert
    Put mergedPut = writeSet.get(key);
    assertThat(mergedPut.getColumns())
        .isEqualTo(
            ImmutableMap.of(
                ANY_NAME_3,
                TextColumn.of(ANY_NAME_3, ANY_TEXT_5),
                ANY_NAME_4,
                TextColumn.ofNull(ANY_NAME_4)));
    assertThat(ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(mergedPut)).isTrue();
  }

  @Test
  public void putIntoWriteSet_PutGivenAfterDelete_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(prepareDelete());
    snapshot.putIntoDeleteSet(deleteKey, delete);

    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(preparePut());

    // Act Assert
    assertThatThrownBy(() -> snapshot.putIntoWriteSet(putKey, put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      putIntoWriteSet_PutWithInsertModeEnabledGivenAfterPut_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Put putWithInsertModeEnabled = Put.newBuilder(put).enableInsertMode().build();
    Snapshot.Key key = new Snapshot.Key(put);

    // Act Assert
    snapshot.putIntoWriteSet(key, put);
    assertThatThrownBy(() -> snapshot.putIntoWriteSet(key, putWithInsertModeEnabled))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      putIntoWriteSet_PutWithImplicitPreReadEnabledGivenAfterWithInsertModeEnabled_ShouldHoldMergedPutWithoutImplicitPreRead() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put putWithInsertModeEnabled = Put.newBuilder(preparePut()).enableInsertMode().build();

    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Put putWithImplicitPreReadEnabled =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue(ANY_NAME_3, ANY_TEXT_5)
            .textValue(ANY_NAME_4, null)
            .enableImplicitPreRead()
            .build();

    Snapshot.Key key = new Snapshot.Key(putWithInsertModeEnabled);

    // Act
    snapshot.putIntoWriteSet(key, putWithInsertModeEnabled);
    snapshot.putIntoWriteSet(key, putWithImplicitPreReadEnabled);

    // Assert
    Put mergedPut = writeSet.get(key);
    assertThat(mergedPut.getColumns())
        .isEqualTo(
            ImmutableMap.of(
                ANY_NAME_3,
                TextColumn.of(ANY_NAME_3, ANY_TEXT_5),
                ANY_NAME_4,
                TextColumn.ofNull(ANY_NAME_4)));
    assertThat(ConsensusCommitOperationAttributes.isInsertModeEnabled(mergedPut)).isTrue();
    assertThat(ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(mergedPut)).isFalse();
  }

  @Test
  public void putIntoDeleteSet_DeleteGiven_ShouldHoldWhatsGivenInDeleteSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete);

    // Act
    snapshot.putIntoDeleteSet(key, delete);

    // Assert
    assertThat(deleteSet.get(key)).isEqualTo(delete);
  }

  @Test
  public void putIntoDeleteSet_DeleteGivenAfterPut_PutSupercedesDelete() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(preparePut());
    snapshot.putIntoWriteSet(putKey, put);

    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(prepareDelete());

    // Act
    snapshot.putIntoDeleteSet(deleteKey, delete);

    // Assert
    assertThat(writeSet.size()).isEqualTo(0);
    assertThat(deleteSet.size()).isEqualTo(1);
    assertThat(deleteSet.get(deleteKey)).isEqualTo(delete);
  }

  @Test
  public void
      putIntoDeleteSet_DeleteGivenAfterPutWithInsertModeEnabled_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete);

    Put putWithInsertModeEnabled = Put.newBuilder(preparePut()).enableInsertMode().build();
    snapshot.putIntoWriteSet(key, putWithInsertModeEnabled);

    // Act Assert
    assertThatThrownBy(() -> snapshot.putIntoDeleteSet(key, delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void putIntoScanSet_ScanGiven_ShouldHoldWhatsGivenInScanSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    Map<Snapshot.Key, TransactionResult> expected = Collections.singletonMap(key, result);

    // Act
    snapshot.putIntoScanSet(scan, expected);

    // Assert
    assertThat(scanSet.get(scan)).isEqualTo(expected);
  }

  @Test
  public void getResult_KeyNeitherContainedInWriteSetNorReadSet_ShouldReturnEmpty()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void getResult_KeyContainedInWriteSetButNotContainedInReadSet_ShouldReturnProperResult()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isPresent();

    assertThat(actual.get().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(actual.get().contains(ANY_NAME_2)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(actual.get().contains(ANY_NAME_3)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_3)).isEqualTo(ANY_TEXT_3);
    assertThat(actual.get().contains(ANY_NAME_4)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_4)).isEqualTo(ANY_TEXT_4);
  }

  @Test
  public void getResult_KeyContainedInWriteSetAndReadSetGiven_ShouldReturnMergedResult()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutForMergeTest();
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(key, Optional.of(result));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isPresent();
    assertMergedResultIsEqualTo(actual.get());
  }

  @Test
  public void getResult_KeyContainedInDeleteSetAndReadSetGiven_ShouldReturnEmpty()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(key, Optional.of(result));
    snapshot.putIntoDeleteSet(key, delete);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void
      getResult_KeyNeitherContainedInDeleteSetNorWriteSetButContainedInAndReadSetGiven_ShouldReturnOriginalResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(key, Optional.of(result));

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void getResult_KeyContainedInWriteSetAndGetNotContainedInGetSet_ShouldReturnEmpty()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void getResult_KeyContainedInWriteSetAndGetNotContainedInGetSet_ShouldReturnProperResult()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();

    assertThat(actual.get().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(actual.get().contains(ANY_NAME_2)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(actual.get().contains(ANY_NAME_3)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_3)).isEqualTo(ANY_TEXT_3);
    assertThat(actual.get().contains(ANY_NAME_4)).isTrue();
    assertThat(actual.get().getText(ANY_NAME_4)).isEqualTo(ANY_TEXT_4);
  }

  @Test
  public void
      getResult_KeyContainedInWriteSetAndGetContainedInGetSetGiven_ShouldReturnMergedResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutForMergeTest();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();
    assertMergedResultIsEqualTo(actual.get());
  }

  @Test
  public void getResult_KeyContainedInDeleteSetAndGetContainedInGetSetGiven_ShouldReturnEmpty()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoDeleteSet(key, delete);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void
      getResult_KeyNeitherContainedInDeleteSetNorWriteSetAndGetContainedInGetSetGiven_ShouldReturnOriginalResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void
      getResult_KeyContainedInWriteSetAndGetContainedInGetSetWithMatchedConjunctionGiven_ShouldReturnMergedResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutForMergeTest();
    ConditionalExpression condition = ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_5);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    Snapshot.Key key = new Snapshot.Key(get);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();
    assertMergedResultIsEqualTo(actual.get());
  }

  @Test
  public void
      getResult_KeyNeitherContainedInDeleteSetNorWriteSetAndGetContainedInGetSetWithUnmatchedConjunction_ShouldReturnOriginalResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    ConditionalExpression condition = ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_2);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    snapshot.putIntoGetSet(get, Optional.of(result));

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void
      getResult_KeyContainedInWriteSetAndGetContainedInGetSetWithUnmatchedConjunctionGiven_ShouldReturnEmpty()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutForMergeTest();
    ConditionalExpression condition = ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    Snapshot.Key key = new Snapshot.Key(get);
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isEmpty();
  }

  @Test
  public void getResults_ScanNotContainedInScanSetGiven_ShouldReturnEmpty() throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Scan scan = prepareScan();

    // Act
    Optional<Map<Snapshot.Key, TransactionResult>> results = snapshot.getResults(scan);

    // Assert
    assertThat(results.isPresent()).isFalse();
  }

  @Test
  public void getResults_ScanContainedInScanSetGiven_ShouldReturnProperResults()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Scan scan = prepareScan();

    TransactionResult result1 = mock(TransactionResult.class);
    TransactionResult result2 = mock(TransactionResult.class);
    TransactionResult result3 = mock(TransactionResult.class);
    Snapshot.Key key1 = mock(Snapshot.Key.class);
    Snapshot.Key key2 = mock(Snapshot.Key.class);
    Snapshot.Key key3 = mock(Snapshot.Key.class);
    scanSet.put(scan, ImmutableMap.of(key1, result1, key2, result2, key3, result3));

    // Act
    Optional<Map<Snapshot.Key, TransactionResult>> results = snapshot.getResults(scan);

    // Assert
    assertThat(results).isPresent();

    Iterator<Map.Entry<Snapshot.Key, TransactionResult>> entryIterator =
        results.get().entrySet().iterator();

    Map.Entry<Snapshot.Key, TransactionResult> entry = entryIterator.next();
    assertThat(entry.getKey()).isEqualTo(key1);
    assertThat(entry.getValue()).isEqualTo(result1);

    entry = entryIterator.next();
    assertThat(entry.getKey()).isEqualTo(key2);
    assertThat(entry.getValue()).isEqualTo(result2);

    entry = entryIterator.next();
    assertThat(entry.getKey()).isEqualTo(key3);
    assertThat(entry.getValue()).isEqualTo(result3);

    assertThat(entryIterator.hasNext()).isFalse();
  }

  private void assertMergedResultIsEqualTo(TransactionResult result) {
    assertThat(result.getValues())
        .isEqualTo(
            ImmutableMap.<String, Value<?>>builder()
                .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_5))
                .put(ANY_NAME_4, new TextValue(ANY_NAME_4, (String) null))
                .put(Attribute.ID, Attribute.toIdValue(ANY_ID))
                .put(Attribute.VERSION, Attribute.toVersionValue(ANY_VERSION))
                .build());
    assertThat(result.getValue(ANY_NAME_1).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_1).get()).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(result.getValue(ANY_NAME_2).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_2).get()).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(result.getValue(ANY_NAME_3).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_3).get()).isEqualTo(new TextValue(ANY_NAME_3, ANY_TEXT_5));
    assertThat(result.getValue(ANY_NAME_4).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_4).get())
        .isEqualTo(new TextValue(ANY_NAME_4, (String) null));
    assertThat(result.getValue(Attribute.ID).isPresent()).isTrue();
    assertThat(result.getValue(Attribute.ID).get()).isEqualTo(Attribute.toIdValue(ANY_ID));
    assertThat(result.getValue(Attribute.VERSION).isPresent()).isTrue();
    assertThat(result.getValue(Attribute.VERSION).get())
        .isEqualTo(Attribute.toVersionValue(ANY_VERSION));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_NAME_3,
                    ANY_NAME_4,
                    Attribute.ID,
                    Attribute.VERSION)));

    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(result.contains(ANY_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_NAME_3)).isFalse();
    assertThat(result.getText(ANY_NAME_3)).isEqualTo(ANY_TEXT_5);
    assertThat(result.getAsObject(ANY_NAME_3)).isEqualTo(ANY_TEXT_5);

    assertThat(result.contains(ANY_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_NAME_4)).isTrue();
    assertThat(result.getText(ANY_NAME_4)).isNull();
    assertThat(result.getAsObject(ANY_NAME_4)).isNull();

    assertThat(result.contains(Attribute.ID)).isTrue();
    assertThat(result.isNull(Attribute.ID)).isFalse();
    assertThat(result.getText(Attribute.ID)).isEqualTo(ANY_ID);
    assertThat(result.getAsObject(Attribute.ID)).isEqualTo(ANY_ID);

    assertThat(result.contains(Attribute.VERSION)).isTrue();
    assertThat(result.isNull(Attribute.VERSION)).isFalse();
    assertThat(result.getInt(Attribute.VERSION)).isEqualTo(ANY_VERSION);
    assertThat(result.getAsObject(Attribute.VERSION)).isEqualTo(ANY_VERSION);
  }

  @Test
  public void to_PrepareMutationComposerGivenAndSnapshotIsolationSet_ShouldCallComposerProperly()
      throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(prepareComposer);

    // Assert
    verify(prepareComposer).add(put, result);
    verify(prepareComposer).add(delete, result);
  }

  @Test
  public void
      to_PrepareMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    configureBehavior();

    // Act
    snapshot.to(prepareComposer);

    // Assert
    Put putFromGet = prepareAnotherPut();
    verify(prepareComposer).add(put, result);
    verify(prepareComposer).add(putFromGet, result);
    verify(snapshot).toSerializableWithExtraWrite(prepareComposer);
  }

  @Test
  public void to_CommitMutationComposerGiven_ShouldCallComposerProperly()
      throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);

    // Act
    snapshot.to(commitComposer);

    // Assert
    verify(commitComposer).add(put, result);
    verify(commitComposer).add(delete, result);
  }

  @Test
  public void
      to_CommitMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(commitComposer);

    // Assert
    // no effect on CommitMutationComposer
    verify(commitComposer).add(put, result);
    verify(commitComposer).add(delete, result);
    verify(snapshot).toSerializableWithExtraWrite(commitComposer);
  }

  @Test
  public void to_RollbackMutationComposerGiven_ShouldCallComposerProperly()
      throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(rollbackComposer);

    // Assert
    verify(rollbackComposer).add(put, result);
    verify(rollbackComposer).add(delete, result);
  }

  @Test
  public void
      to_RollbackMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws PreparationConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.putIntoReadSet(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(rollbackComposer);

    // Assert
    // no effect on RollbackMutationComposer
    verify(rollbackComposer).add(put, result);
    verify(rollbackComposer).add(delete, result);
    verify(snapshot).toSerializableWithExtraWrite(rollbackComposer);
  }

  @Test
  public void toSerializableWithExtraWrite_UnmutatedReadSetExists_ShouldConvertReadSetIntoWriteSet()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    TransactionResult txResult = new TransactionResult(result);
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.putIntoGetSet(get, Optional.of(txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraWrite(prepareComposer))
        .doesNotThrowAnyException();

    // Assert
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().get())
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    assertThat(writeSet).contains(entry(new Snapshot.Key(get), expected));
    assertThat(writeSet.size()).isEqualTo(2);
    verify(prepareComposer, never()).add(any(), any());
  }

  @Test
  public void
      toSerializableWithExtraWrite_UnmutatedReadSetForNonExistingExists_ShouldNotThrowAnyException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.empty());
    snapshot.putIntoGetSet(get, Optional.empty());
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.toSerializableWithExtraWrite(prepareComposer));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
    Get expected =
        new Get(get.getPartitionKey(), get.getClusteringKey().get())
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    verify(prepareComposer).add(expected, null);
  }

  @Test
  public void
      toSerializableWithExtraWrite_ScanSetAndWriteSetExist_ShouldThrowPreparationConflictException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult);
    Put put = preparePut();
    snapshot.putIntoReadSet(key, Optional.of(txResult));
    snapshot.putIntoScanSet(scan, Collections.singletonMap(key, txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.toSerializableWithExtraWrite(prepareComposer));

    // Assert
    assertThat(thrown).isInstanceOf(PreparationConflictException.class);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    TransactionResult txResult = new TransactionResult(result);
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.putIntoGetSet(get, Optional.of(txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.putIntoGetSet(get, Optional.of(txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(changedTxResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.empty());
    snapshot.putIntoGetSet(get, Optional.empty());
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = prepareResult(ANY_ID);
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult);
    snapshot.putIntoReadSet(key, Optional.of(txResult));
    snapshot.putIntoScanSet(scan, Collections.singletonMap(key, txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator()).thenReturn(Collections.singletonList((Result) txResult).iterator());
    Scan scanWithProjections =
        prepareScan()
            .withProjections(
                Arrays.asList(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2));
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult);
    snapshot.putIntoReadSet(key, Optional.of(txResult));
    snapshot.putIntoScanSet(scan, Collections.singletonMap(key, txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator())
        .thenReturn(Collections.singletonList((Result) changedTxResult).iterator());
    Scan scanWithProjections =
        prepareScan()
            .withProjections(
                Arrays.asList(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2));
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID + "x");
    snapshot.putIntoScanSet(scan, Collections.emptyMap());
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator()).thenReturn(Collections.singletonList((Result) txResult).iterator());
    Scan scanWithProjections =
        prepareScan()
            .withProjections(
                Arrays.asList(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2));
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void
      toSerializableWithExtraRead_MultipleScansInScanSetExist_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);

    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_2))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    Result result1 =
        new TransactionResult(
            new ResultImpl(
                ImmutableMap.of(
                    ANY_NAME_1,
                    TextColumn.of(ANY_NAME_1, ANY_TEXT_1),
                    ANY_NAME_2,
                    TextColumn.of(ANY_NAME_2, ANY_TEXT_2),
                    Attribute.ID,
                    ScalarDbUtils.toColumn(Attribute.toIdValue("id1")),
                    Attribute.VERSION,
                    ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION))),
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
                    ScalarDbUtils.toColumn(Attribute.toIdValue("id2")),
                    Attribute.VERSION,
                    ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION))),
                TABLE_METADATA));

    Snapshot.Key key1 = new Snapshot.Key(scan1, result1);
    Snapshot.Key key2 = new Snapshot.Key(scan2, result2);

    snapshot.putIntoScanSet(scan1, Collections.singletonMap(key1, new TransactionResult(result1)));
    snapshot.putIntoScanSet(scan2, Collections.singletonMap(key2, new TransactionResult(result2)));
    snapshot.putIntoReadSet(key1, Optional.of(new TransactionResult(result1)));
    snapshot.putIntoReadSet(key2, Optional.of(new TransactionResult(result2)));

    DistributedStorage storage = mock(DistributedStorage.class);

    Scanner scanner1 = mock(Scanner.class);
    when(scanner1.iterator()).thenReturn(Collections.singletonList(result1).iterator());
    Scan scan1WithProjections =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withProjections(
                Arrays.asList(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2));
    when(storage.scan(scan1WithProjections)).thenReturn(scanner1);

    Scanner scanner2 = mock(Scanner.class);
    when(scanner2.iterator()).thenReturn(Collections.singletonList(result2).iterator());
    Scan scan2WithProjections =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_2))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withProjections(
                Arrays.asList(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2));
    when(storage.scan(scan2WithProjections)).thenReturn(scanner2);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();
  }

  @Test
  public void
      toSerializableWithExtraRead_NullMetadataInReadSetNotChanged_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult txResult = new TransactionResult(result);
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.of(result));
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getWithProjections =
        Get.newBuilder(get).projections(Attribute.ID, Attribute.VERSION).build();
    when(storage.get(getWithProjections)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void
      toSerializableWithExtraRead_NullMetadataInReadSetChanged_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult changedResult = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(get), Optional.of(result));
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getWithProjections =
        Get.newBuilder(get).projections(Attribute.ID, Attribute.VERSION).build();
    when(storage.get(getWithProjections)).thenReturn(Optional.of(changedResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void
      toSerializableWithExtraRead_ScannedResultDeleted_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);
    snapshot.putIntoReadSet(key, Optional.of(result));
    snapshot.putIntoScanSet(scan, Collections.singletonMap(key, result));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanWithProjections =
        Scan.newBuilder(scan)
            .projections(Attribute.ID, Attribute.VERSION, ANY_NAME_1, ANY_NAME_2)
            .build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndDeleteKeyAlreadyPresentInDeleteSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(delete);
    snapshot.putIntoDeleteSet(deleteKey, delete);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndPutKeyAlreadyPresentInScanSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndPutWithSamePartitionKeyWithoutClusteringKeyInWriteSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutWithPartitionKeyOnly();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareScan();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithNoRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, infinite)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithNoRangeGivenButPutInWriteSetNotOverlappedWithScanWithConjunctions_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .consistency(Consistency.LINEARIZABLE)
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
            .build();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_ScanWithRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        prepareScan()
            // ["text1", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan2 =
        prepareScan()
            // ["text2", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan3 =
        prepareScan()
            // ["text1", "text2"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), true);
    Scan scan4 =
        prepareScan()
            // ("text2", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan5 =
        prepareScan()
            // ["text1", "text2")
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), false);

    // Act Assert
    Throwable thrown1 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan1, Collections.emptyMap()));
    Throwable thrown2 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan2, Collections.emptyMap()));
    Throwable thrown3 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan3, Collections.emptyMap()));
    Throwable thrown4 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan4, Collections.emptyMap()));
    Throwable thrown5 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan5, Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown4).doesNotThrowAnyException();
    assertThat(thrown5).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_ScanWithEndSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text3"]
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2"]
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan3 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2")
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown1 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan1, Collections.emptyMap()));
    Throwable thrown2 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan2, Collections.emptyMap()));
    Throwable thrown3 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan3, Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_ScanWithStartSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ["text1", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ["text2", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan3 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ("text2", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown1 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan1, Collections.emptyMap()));
    Throwable thrown2 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan2, Collections.emptyMap()));
    Throwable thrown3 =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan3, Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_ScanWithIndexGivenAndPutInWriteSetInSameTable_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexGivenAndPutInWriteSetInDifferentTable_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME_2)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_ScanWithIndexAndPutWithSameIndexKeyGiven_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put1 =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_4, ANY_TEXT_4)
            .build();
    Snapshot.Key putKey1 = new Snapshot.Key(put1);
    Snapshot.Key putKey2 = new Snapshot.Key(put2);
    snapshot.putIntoWriteSet(putKey1, put1);
    snapshot.putIntoWriteSet(putKey2, put2);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexAndPutWithSameIndexKeyGivenButNotOverlappedWithScanWithConjunctions_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put1 =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .textValue(ANY_NAME_4, ANY_TEXT_4)
            .build();
    Snapshot.Key putKey1 = new Snapshot.Key(put1);
    Snapshot.Key putKey2 = new Snapshot.Key(put2);
    snapshot.putIntoWriteSet(putKey1, put1);
    snapshot.putIntoWriteSet(putKey2, put2);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_ScanAllGivenAndPutInWriteSetInSameTable_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    ScanAll scanAll =
        new ScanAll()
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scanAll, result);

    // Act Assert
    Throwable thrown =
        catchThrowable(
            () -> snapshot.verifyNoOverlap(scanAll, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanAllGivenAndPutInWriteSetNotOverlappingWithScanAll_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    ScanAll scanAll =
        new ScanAll()
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME_2)
            .forTable(ANY_TABLE_NAME_2);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scanAll, result);

    // Act Assert
    Throwable thrown =
        catchThrowable(
            () -> snapshot.verifyNoOverlap(scanAll, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_CrossPartitionScanGivenAndPutInSameTable_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndPutInDifferentNamespace_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan(ANY_NAMESPACE_NAME_2, ANY_TABLE_NAME);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndPutInDifferentTable_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan(ANY_NAMESPACE_NAME, ANY_TABLE_NAME_2);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result);

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndAllConditionsMatch_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePutWithIntColumns();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan())
            .clearConditions()
            .where(
                ConditionSetBuilder.andConditionSet(
                        ImmutableSet.of(
                            ConditionBuilder.column(ANY_NAME_1).isEqualToInt(ANY_INT_1),
                            ConditionBuilder.column(ANY_NAME_2).isNotEqualToInt(ANY_INT_2),
                            ConditionBuilder.column(ANY_NAME_3).isGreaterThanInt(ANY_INT_0),
                            ConditionBuilder.column(ANY_NAME_4)
                                .isGreaterThanOrEqualToInt(ANY_INT_1),
                            ConditionBuilder.column(ANY_NAME_5).isLessThanInt(ANY_INT_2),
                            ConditionBuilder.column(ANY_NAME_6).isLessThanOrEqualToInt(ANY_INT_1),
                            ConditionBuilder.column(ANY_NAME_7).isNotNullInt(),
                            ConditionBuilder.column(ANY_NAME_8).isNullInt()))
                    .build())
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndAnyConjunctionMatch_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan())
            .clearConditions()
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_1))
            .or(ConditionBuilder.column(ANY_NAME_4).isEqualToText(ANY_TEXT_4))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndLikeConditionsMatch_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan())
            .clearConditions()
            .where(ConditionBuilder.column(ANY_NAME_3).isLikeText("text%"))
            .and(ConditionBuilder.column(ANY_NAME_4).isNotLikeText("text"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableButConditionNotMatch_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan())
            .clearConditions()
            .where(ConditionBuilder.column(ANY_NAME_4).isEqualToText(ANY_TEXT_1))
            .or(ConditionBuilder.column(ANY_NAME_5).isEqualToText(ANY_TEXT_1))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanWithoutConjunctionGivenAndNewPutInSameTable_ShouldThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Put put = preparePutWithIntColumns();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = Scan.newBuilder(prepareCrossPartitionScan()).clearConditions().build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getReadWriteSet_ReadSetAndWriteSetGiven_ShouldReturnProperValue() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);

    Get get1 = prepareGet();
    TransactionResult result1 = prepareResult("t1");
    Snapshot.Key readKey1 = new Snapshot.Key(get1);
    snapshot.putIntoReadSet(readKey1, Optional.of(result1));
    Get get2 = prepareAnotherGet();
    TransactionResult result2 = prepareResult("t2");
    Snapshot.Key readKey2 = new Snapshot.Key(get2);
    snapshot.putIntoReadSet(readKey2, Optional.of(result2));

    Put put1 = preparePut();
    Snapshot.Key putKey1 = new Snapshot.Key(put1);
    snapshot.putIntoWriteSet(putKey1, put1);
    Put put2 = prepareAnotherPut();
    Snapshot.Key putKey2 = new Snapshot.Key(put2);
    snapshot.putIntoWriteSet(putKey2, put2);

    // Act
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    {
      // The method returns an immutable value, so the following update shouldn't be included.
      Get delayedGet =
          new Get(new Key(ANY_NAME_1, ANY_TEXT_2), new Key(ANY_NAME_2, ANY_TEXT_1))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(ANY_NAMESPACE_NAME)
              .forTable(ANY_TABLE_NAME);
      TransactionResult delayedResult = prepareResult("t3");
      snapshot.putIntoReadSet(new Snapshot.Key(delayedGet), Optional.of(delayedResult));

      Put delayedPut =
          new Put(new Key(ANY_NAME_1, ANY_TEXT_2), new Key(ANY_NAME_2, ANY_TEXT_1))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(ANY_NAMESPACE_NAME)
              .forTable(ANY_TABLE_NAME)
              .withValue(ANY_NAME_3, ANY_TEXT_3);
      snapshot.putIntoWriteSet(new Snapshot.Key(delayedPut), delayedPut);
    }

    // Assert
    assertThat(readWriteSets.readSetMap).size().isEqualTo(2);
    for (Map.Entry<Snapshot.Key, Optional<TransactionResult>> entry :
        readWriteSets.readSetMap.entrySet()) {
      if (entry.getKey().equals(readKey1)) {
        assertThat(entry.getValue()).isPresent().get().isEqualTo(result1);
      } else if (entry.getKey().equals(readKey2)) {
        assertThat(entry.getValue()).isPresent().get().isEqualTo(result2);
      } else {
        throw new AssertionError("Unexpected key: " + entry.getKey());
      }
    }

    assertThat(readWriteSets.writeSet).size().isEqualTo(2);
    for (Map.Entry<Snapshot.Key, Put> entry : readWriteSets.writeSet) {
      if (entry.getKey().equals(putKey1)) {
        assertThat(entry.getValue()).isEqualTo(put1);
      } else if (entry.getKey().equals(putKey2)) {
        assertThat(entry.getValue()).isEqualTo(put2);
      } else {
        throw new AssertionError("Unexpected key: " + entry.getKey());
      }
    }
  }

  @Test
  void getReadWriteSet_ReadSetAndDeleteSetGiven_ShouldReturnProperValue() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);

    Get get1 = prepareGet();
    TransactionResult result1 = prepareResult("t1");
    Snapshot.Key readKey1 = new Snapshot.Key(get1);
    snapshot.putIntoReadSet(readKey1, Optional.of(result1));
    Get get2 = prepareAnotherGet();
    TransactionResult result2 = prepareResult("t2");
    Snapshot.Key readKey2 = new Snapshot.Key(get2);
    snapshot.putIntoReadSet(readKey2, Optional.of(result2));

    Delete delete1 = prepareDelete();
    Snapshot.Key deleteKey1 = new Snapshot.Key(delete1);
    snapshot.putIntoDeleteSet(deleteKey1, delete1);
    Delete delete2 = prepareAnotherDelete();
    Snapshot.Key deleteKey2 = new Snapshot.Key(delete2);
    snapshot.putIntoDeleteSet(deleteKey2, delete2);

    // Act
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    {
      // The method returns an immutable value, so the following update shouldn't be included.
      Get delayedGet =
          new Get(new Key(ANY_NAME_1, ANY_TEXT_2), new Key(ANY_NAME_2, ANY_TEXT_1))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(ANY_NAMESPACE_NAME)
              .forTable(ANY_TABLE_NAME);
      TransactionResult delayedResult = prepareResult("t3");
      snapshot.putIntoReadSet(new Snapshot.Key(delayedGet), Optional.of(delayedResult));

      Delete delayedDelete =
          new Delete(new Key(ANY_NAME_1, ANY_TEXT_2), new Key(ANY_NAME_2, ANY_TEXT_1))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(ANY_NAMESPACE_NAME)
              .forTable(ANY_TABLE_NAME);
      snapshot.putIntoDeleteSet(new Snapshot.Key(delayedDelete), delayedDelete);
    }

    // Assert
    assertThat(readWriteSets.readSetMap).size().isEqualTo(2);
    for (Map.Entry<Snapshot.Key, Optional<TransactionResult>> entry :
        readWriteSets.readSetMap.entrySet()) {
      if (entry.getKey().equals(readKey1)) {
        assertThat(entry.getValue()).isPresent().get().isEqualTo(result1);
      } else if (entry.getKey().equals(readKey2)) {
        assertThat(entry.getValue()).isPresent().get().isEqualTo(result2);
      } else {
        throw new AssertionError("Unexpected key: " + entry.getKey());
      }
    }

    assertThat(readWriteSets.deleteSet).size().isEqualTo(2);
    for (Map.Entry<Snapshot.Key, Delete> entry : readWriteSets.deleteSet) {
      if (entry.getKey().equals(deleteKey1)) {
        assertThat(entry.getValue()).isEqualTo(delete1);
      } else if (entry.getKey().equals(deleteKey2)) {
        assertThat(entry.getValue()).isEqualTo(delete2);
      } else {
        throw new AssertionError("Unexpected key: " + entry.getKey());
      }
    }
  }
}
