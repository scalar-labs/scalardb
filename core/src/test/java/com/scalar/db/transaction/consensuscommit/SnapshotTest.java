package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
              .addSecondaryIndex(ANY_NAME_4)
              .build());

  // Table metadata where the partition key is also a secondary index
  private static final TableMetadata TABLE_METADATA_WITH_PK_INDEX =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.TEXT)
              .addColumn(ANY_NAME_4, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .addSecondaryIndex(ANY_NAME_1)
              .addSecondaryIndex(ANY_NAME_4)
              .build());

  private Snapshot snapshot;
  private ConcurrentMap<Snapshot.Key, Optional<TransactionResult>> readSet;
  private ConcurrentMap<Get, Optional<TransactionResult>> getSet;
  private Map<Scan, LinkedHashMap<Snapshot.Key, TransactionResult>> scanSet;
  private Map<Snapshot.Key, Put> writeSet;
  private Map<Snapshot.Key, Delete> deleteSet;
  private List<Snapshot.ScannerInfo> scannerSet;

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

  private Snapshot prepareSnapshot() {
    return prepareSnapshot(binaryCollation());
  }

  private Snapshot prepareSnapshot(CollationComparator collationComparator) {
    readSet = new ConcurrentHashMap<>();
    getSet = new ConcurrentHashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();
    scannerSet = new ArrayList<>();

    return spy(
        new Snapshot(
            ANY_ID,
            tableMetadataManager,
            new ParallelExecutor(config),
            collationComparator,
            readSet,
            getSet,
            scanSet,
            writeSet,
            deleteSet,
            scannerSet));
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

  private Get prepareGetWithIndex() {
    Key indexKey = Key.ofText(ANY_NAME_4, ANY_TEXT_1);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .indexKey(indexKey)
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

  private Put preparePutWithPartitionKeyOnly() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .textValue(ANY_NAME_3, ANY_TEXT_3)
        .textValue(ANY_NAME_4, ANY_TEXT_4)
        .build();
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
    return prepareDelete(ANY_TEXT_1, ANY_TEXT_2);
  }

  private Delete prepareDelete(String partitionKeyColumnValue, String clusteringKeyColumnValue) {
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, partitionKeyColumnValue))
        .clusteringKey(Key.ofText(ANY_NAME_2, clusteringKeyColumnValue))
        .build();
  }

  private Delete prepareAnotherDelete() {
    Key partitionKey = Key.ofText(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = Key.ofText(ANY_NAME_6, ANY_TEXT_6);
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
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
    snapshot = prepareSnapshot();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());
    TransactionResult result = prepareResult(ANY_ID);

    // Act
    snapshot.putIntoReadSet(key, Optional.of(result));

    // Assert
    assertThat(readSet.get(key)).isEqualTo(Optional.of(result));
  }

  @Test
  public void putIntoGetSet_ResultGiven_ShouldHoldWhatsGivenInReadSet() {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareGet();
    TransactionResult result = prepareResult(ANY_ID);

    // Act
    snapshot.putIntoGetSet(get, Optional.of(result));

    // Assert
    assertThat(getSet.get(get)).isEqualTo(Optional.of(result));
  }

  @Test
  public void putIntoWriteSet_PutGiven_ShouldHoldWhatsGivenInWriteSet() throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(put, binaryCollation());

    // Act
    snapshot.putIntoWriteSet(key, put);

    // Assert
    assertThat(writeSet.get(key)).isEqualTo(put);
  }

  @Test
  public void putIntoWriteSet_PutGivenTwice_ShouldHoldMergedPut() throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put1 = preparePut();
    Snapshot.Key key = new Snapshot.Key(put1, binaryCollation());

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
  public void putIntoWriteSet_PutGivenAfterDelete_ShouldMoveFromDeleteSetToWriteSetWithNullColumns()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(delete, binaryCollation());
    snapshot.putIntoDeleteSet(deleteKey, delete);

    // Put with only ANY_NAME_3 specified (ANY_NAME_4 is not specified)
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());

    // Act
    snapshot.putIntoWriteSet(putKey, put);

    // Assert
    assertThat(deleteSet).isEmpty();
    assertThat(writeSet).containsKey(putKey);
    Put actualPut = writeSet.get(putKey);
    // The Put should contain the specified column
    assertThat(actualPut.getColumns().get(ANY_NAME_3))
        .isEqualTo(TextColumn.of(ANY_NAME_3, ANY_TEXT_3));
    // The unspecified non-key column should be set to null
    assertThat(actualPut.getColumns().get(ANY_NAME_4)).isEqualTo(TextColumn.ofNull(ANY_NAME_4));
    // Insert mode should be disabled since the record previously existed
    assertThat(ConsensusCommitOperationAttributes.isInsertModeEnabled(actualPut)).isFalse();
    // Implicit pre-read should be enabled for proper preparation
    assertThat(ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(actualPut)).isTrue();
  }

  @Test
  public void
      putIntoWriteSet_PutWithInsertModeEnabledGivenAfterDelete_ShouldDisableInsertModeAndEnableImplicitPreRead()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(delete, binaryCollation());
    snapshot.putIntoDeleteSet(deleteKey, delete);

    // Put with insert mode enabled
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .enableInsertMode()
            .build();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());

    // Act
    snapshot.putIntoWriteSet(putKey, put);

    // Assert
    assertThat(deleteSet).isEmpty();
    assertThat(writeSet).containsKey(putKey);
    Put actualPut = writeSet.get(putKey);
    // Insert mode should be disabled even if the original Put had insert mode enabled
    assertThat(ConsensusCommitOperationAttributes.isInsertModeEnabled(actualPut)).isFalse();
    // Implicit pre-read should be enabled for proper preparation
    assertThat(ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(actualPut)).isTrue();
  }

  @Test
  public void
      putIntoWriteSet_PutWithInsertModeEnabledGivenAfterPut_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Put putWithInsertModeEnabled = Put.newBuilder(put).enableInsertMode().build();
    Snapshot.Key key = new Snapshot.Key(put, binaryCollation());

    // Act Assert
    snapshot.putIntoWriteSet(key, put);
    assertThatThrownBy(() -> snapshot.putIntoWriteSet(key, putWithInsertModeEnabled))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      putIntoWriteSet_PutWithImplicitPreReadEnabledGivenAfterWithInsertModeEnabled_ShouldHoldMergedPutWithoutImplicitPreRead()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
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

    Snapshot.Key key = new Snapshot.Key(putWithInsertModeEnabled, binaryCollation());

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
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete, binaryCollation());

    // Act
    snapshot.putIntoDeleteSet(key, delete);

    // Assert
    assertThat(deleteSet.get(key)).isEqualTo(delete);
  }

  @Test
  public void putIntoDeleteSet_DeleteGivenAfterPut_PutSupercedesDelete() throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(preparePut(), binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);

    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(prepareDelete(), binaryCollation());

    // Act
    snapshot.putIntoDeleteSet(deleteKey, delete);

    // Assert
    assertThat(writeSet.size()).isEqualTo(0);
    assertThat(deleteSet.size()).isEqualTo(1);
    assertThat(deleteSet.get(deleteKey)).isEqualTo(delete);
  }

  @Test
  public void
      putIntoDeleteSet_DeleteGivenAfterPutWithInsertModeEnabled_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete, binaryCollation());

    Put putWithInsertModeEnabled = Put.newBuilder(preparePut()).enableInsertMode().build();
    snapshot.putIntoWriteSet(key, putWithInsertModeEnabled);

    // Act Assert
    assertThatThrownBy(() -> snapshot.putIntoDeleteSet(key, delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void putIntoScanSet_ScanGiven_ShouldHoldWhatsGivenInScanSet() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());
    LinkedHashMap<Snapshot.Key, TransactionResult> expected =
        Maps.newLinkedHashMap(Collections.singletonMap(key, result));

    // Act
    snapshot.putIntoScanSet(scan, expected);

    // Assert
    assertThat(scanSet.get(scan)).isEqualTo(expected);
  }

  @Test
  public void getResult_KeyNeitherContainedInWriteSetNorReadSet_ShouldReturnEmpty()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void getResult_KeyContainedInWriteSetButNotContainedInReadSet_ShouldReturnProperResult()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());
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
    snapshot = prepareSnapshot();
    Put put = preparePutForMergeTest();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());
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
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete, binaryCollation());
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
    snapshot = prepareSnapshot();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());
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
    snapshot = prepareSnapshot();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void getResult_KeyContainedInWriteSetAndGetNotContainedInGetSet_ShouldReturnProperResult()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
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
    snapshot = prepareSnapshot();
    Put put = preparePutForMergeTest();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
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
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
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
    snapshot = prepareSnapshot();
    Get get = prepareGet();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
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
    snapshot = prepareSnapshot();
    Put put = preparePutForMergeTest();
    ConditionalExpression condition = ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_5);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
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
    snapshot = prepareSnapshot();
    Snapshot.Key key = new Snapshot.Key(prepareGet(), binaryCollation());
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
    snapshot = prepareSnapshot();
    Put put = preparePutForMergeTest();
    ConditionalExpression condition = ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3);
    Get get = Get.newBuilder(prepareGet()).where(condition).build();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isEmpty();
  }

  @Test
  public void getResults_ScanNotContainedInScanSetGiven_ShouldReturnEmpty() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();

    // Act
    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> results = snapshot.getResults(scan);

    // Assert
    assertThat(results.isPresent()).isFalse();
  }

  @Test
  public void getResults_ScanContainedInScanSetGiven_ShouldReturnProperResults() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();

    TransactionResult result1 = mock(TransactionResult.class);
    TransactionResult result2 = mock(TransactionResult.class);
    TransactionResult result3 = mock(TransactionResult.class);
    Snapshot.Key key1 = mock(Snapshot.Key.class);
    Snapshot.Key key2 = mock(Snapshot.Key.class);
    Snapshot.Key key3 = mock(Snapshot.Key.class);
    scanSet.put(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3)));

    // Act
    Optional<LinkedHashMap<Snapshot.Key, TransactionResult>> results = snapshot.getResults(scan);

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
    assertThat(result.getColumns())
        .isEqualTo(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_5))
                .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, null))
                .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID))
                .build());
    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.contains(ANY_NAME_3)).isTrue();
    assertThat(result.getText(ANY_NAME_3)).isEqualTo(ANY_TEXT_5);
    assertThat(result.contains(ANY_NAME_4)).isTrue();
    assertThat(result.getText(ANY_NAME_4)).isNull();
    assertThat(result.contains(Attribute.ID)).isTrue();
    assertThat(result.getText(Attribute.ID)).isEqualTo(ANY_ID);

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3, ANY_NAME_4, Attribute.ID)));

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
  }

  @Test
  public void to_PrepareMutationComposerGivenAndSnapshotIsolationSet_ShouldCallComposerProperly()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoReadSet(
        new Snapshot.Key(prepareAnotherGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete, binaryCollation()), delete);
    configureBehavior();

    // Act
    snapshot.to(prepareComposer);

    // Assert
    verify(prepareComposer).add(put, result);
    verify(prepareComposer).add(delete, result);
  }

  @Test
  public void to_CommitMutationComposerGiven_ShouldCallComposerProperly()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoReadSet(
        new Snapshot.Key(prepareAnotherGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete, binaryCollation()), delete);

    // Act
    snapshot.to(commitComposer);

    // Assert
    verify(commitComposer).add(put, result);
    verify(commitComposer).add(delete, result);
  }

  @Test
  public void to_RollbackMutationComposerGiven_ShouldCallComposerProperly()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoReadSet(new Snapshot.Key(prepareGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoReadSet(
        new Snapshot.Key(prepareAnotherGet(), binaryCollation()), Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete, binaryCollation()), delete);
    configureBehavior();

    // Act
    snapshot.to(rollbackComposer);

    // Assert
    verify(rollbackComposer).add(put, result);
    verify(rollbackComposer).add(delete, result);
  }

  @Test
  public void toSerializable_ReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    TransactionResult txResult = new TransactionResult(result);
    snapshot.putIntoGetSet(get, Optional.of(txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ReadSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(txResult));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(changedTxResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ReadSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareAnotherGet();
    Put put = preparePut();
    snapshot.putIntoGetSet(get, Optional.empty());
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = prepareResult(ANY_ID);
    Get getForStorage =
        Get.newBuilder(prepareAnotherGet()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_GetSetWithGetWithIndex_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult txResult = prepareResult(ANY_ID + "x");
    snapshot.putIntoGetSet(getWithIndex, Optional.of(txResult));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(getWithIndex);
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetSetWithGetWithIndex_RecordInserted_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "xx", ANY_TEXT_1, ANY_TEXT_3);
    snapshot.putIntoGetSet(getWithIndex, Optional.of(result1));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetSetWithGetWithIndex_RecordInsertedByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID, ANY_TEXT_1, ANY_TEXT_3);
    snapshot.putIntoGetSet(getWithIndex, Optional.of(result1));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(getWithIndex);
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(ANY_ID + "x");
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.singletonMap(key, txResult)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetUpdated_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.singletonMap(key, txResult)));
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(changedTxResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetUpdatedByMyself_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.singletonMap(key, txResult)));
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(changedTxResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetExtended_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID + "x");
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.emptyMap()));
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsExtended_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(ANY_ID + "xx", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key2, result2)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetExtendedByMyself_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.emptyMap()));
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsExtendedByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(ANY_ID, ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key2, result2)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScanSetDeleted_ShouldThrowValidationConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.singletonMap(key, txResult)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanSetWithMultipleRecordsDeleted_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(ANY_ID + "xx", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2)));

    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());
    Scan scanForStorage =
        Scan.newBuilder(prepareScan()).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_MultipleScansInScanSetExist_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();

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

    Snapshot.Key key1 = new Snapshot.Key(scan1, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan2, result2, TABLE_METADATA, binaryCollation());

    snapshot.putIntoScanSet(
        scan1,
        Maps.newLinkedHashMap(Collections.singletonMap(key1, new TransactionResult(result1))));
    snapshot.putIntoScanSet(
        scan2,
        Maps.newLinkedHashMap(Collections.singletonMap(key2, new TransactionResult(result2))));

    DistributedStorage storage = mock(DistributedStorage.class);

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
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();
  }

  @Test
  public void toSerializable_NullMetadataInReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult txResult = new TransactionResult(result);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getForStorage = Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_NullMetadataInReadSetChanged_ShouldThrowValidationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();
    TransactionResult changedResult = prepareResult(ANY_ID);
    snapshot.putIntoGetSet(get, Optional.of(result));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getForStorage = Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build();
    when(storage.get(getForStorage)).thenReturn(Optional.of(changedResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).get(getForStorage);
  }

  @Test
  public void toSerializable_ScanWithLimitInScanSet_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(ANY_ID + "x");
    TransactionResult result2 = prepareResult(ANY_ID + "x");
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(Collections.singletonMap(key1, result1)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingFirstRecordIntoScanRange_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_4);
    TransactionResult insertedResult = prepareResult(ANY_ID + "xx", ANY_TEXT_1, ANY_TEXT_2);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1)));
    DistributedStorage storage = mock(DistributedStorage.class);
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
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingFirstRecordIntoScanRangeByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithLimit(1);
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_4);
    TransactionResult insertedResult = prepareResult(ANY_ID, ANY_TEXT_1, ANY_TEXT_2);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1)));
    DistributedStorage storage = mock(DistributedStorage.class);
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
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingLastRecordIntoScanRange_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithLimit(3);
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult insertedResult = prepareResult(ANY_ID + "xx", ANY_TEXT_1, ANY_TEXT_4);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2)));
    DistributedStorage storage = mock(DistributedStorage.class);
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
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithLimitInScanSet_WhenInsertingLastRecordIntoScanRangeByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithLimit(3);
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    TransactionResult insertedResult = prepareResult(ANY_ID, ANY_TEXT_1, ANY_TEXT_4);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2)));
    DistributedStorage storage = mock(DistributedStorage.class);
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
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenUpdatingRecords_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(ANY_ID + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3)));

    // Simulate that the first and third records were updated by another transaction
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();

    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenUpdatingRecordsByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(ANY_ID + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3)));

    // Simulate that the first and third records were updated by myself
    snapshot.putIntoWriteSet(key1, preparePut(ANY_TEXT_1, ANY_TEXT_1));
    snapshot.putIntoWriteSet(key3, preparePut(ANY_TEXT_3, ANY_TEXT_1));
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();

    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(scan);
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenDeletingRecordsByMyself_ShouldProcessWithoutExceptions()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_1);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_2, ANY_TEXT_1);
    TransactionResult result3 = prepareResult(ANY_ID + "x", ANY_TEXT_3, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    Snapshot.Key key2 = new Snapshot.Key(scan, result2, TABLE_METADATA, binaryCollation());
    Snapshot.Key key3 = new Snapshot.Key(scan, result3, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1, key2, result2, key3, result3)));

    // Simulate that the first and third records were deleted by myself
    snapshot.putIntoDeleteSet(key1, prepareDelete(ANY_TEXT_1, ANY_TEXT_1));
    snapshot.putIntoDeleteSet(key3, prepareDelete(ANY_TEXT_3, ANY_TEXT_1));
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result2)).thenReturn(Optional.empty());

    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(scan);
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void toSerializable_ScannerSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScan();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_2);
    TransactionResult result2 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_3);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScannerSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1)));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage = Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetSetWithGetWithIndex_WhenBeforeIndexHasUncommittedRecordFromOtherTransaction_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Get getWithIndex = prepareGetWithIndex();
    TransactionResult txResult = prepareResult(ANY_ID + "x");
    snapshot.putIntoGetSet(getWithIndex, Optional.of(txResult));
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(prepareScanWithIndex()).consistency(Consistency.LINEARIZABLE).build();

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(txResult)).thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan returning a PREPARED record from another transaction
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(getWithIndex);
    ImmutableMap<String, Column<?>> preparedColumns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_3))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_1))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID + "other"))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, TransactionState.PREPARED.get()))
            .build();
    TransactionResult preparedResult =
        new TransactionResult(new ResultImpl(preparedColumns, TABLE_METADATA));
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator())
        .thenReturn(Collections.singletonList((Result) preparedResult).iterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScanSet_WhenBeforeIndexHasUncommittedRecordFromOtherTransaction_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1)));

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result1)).thenReturn(Optional.empty());

    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage =
        Scan.newBuilder(scan).limit(0).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan returning a PREPARED record from another transaction
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(scan);
    ImmutableMap<String, Column<?>> preparedColumns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_3))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_1))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID + "other"))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, TransactionState.PREPARED.get()))
            .build();
    TransactionResult preparedResult =
        new TransactionResult(new ResultImpl(preparedColumns, TABLE_METADATA));
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator())
        .thenReturn(Collections.singletonList((Result) preparedResult).iterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);
  }

  @Test
  public void
      toSerializable_ScanWithIndexInScannerSet_WhenBeforeIndexHasUncommittedRecordFromOtherTransaction_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();
    TransactionResult result1 = prepareResult(ANY_ID + "x", ANY_TEXT_1, ANY_TEXT_1);
    Snapshot.Key key1 = new Snapshot.Key(scan, result1, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScannerSet(scan, Maps.newLinkedHashMap(ImmutableMap.of(key1, result1)));

    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(result1)).thenReturn(Optional.empty());

    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage = Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build();
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Mock the before-image index scan returning a PREPARED record from another transaction
    Scan beforeIndexScan = ConsensusCommitUtils.createBeforeIndexScan(scan);
    ImmutableMap<String, Column<?>> preparedColumns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_3))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_1))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, ANY_TEXT_3))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID + "other"))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, TransactionState.PREPARED.get()))
            .build();
    TransactionResult preparedResult =
        new TransactionResult(new ResultImpl(preparedColumns, TABLE_METADATA));
    Scanner beforeIndexScanner = mock(Scanner.class);
    when(beforeIndexScanner.iterator())
        .thenReturn(Collections.singletonList((Result) preparedResult).iterator());
    when(storage.scan(beforeIndexScan)).thenReturn(beforeIndexScanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndDeleteKeyAlreadyPresentInDeleteSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot();
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(delete, binaryCollation());
    snapshot.putIntoDeleteSet(deleteKey, delete);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndPutKeyAlreadyPresentInScanSet_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanGivenAndPutWithSamePartitionKeyWithoutClusteringKeyInWriteSet_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePutWithPartitionKeyOnly();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareScan();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithNoRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, infinite)
            .build();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithNoRangeGivenButPutInWriteSetNotOverlappedWithScanWithConjunctions_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_4))
            .build();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_ScanWithRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        Scan.newBuilder(prepareScan())
            // ["text1", "text3"]
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1), true)
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_3), true)
            .build();
    Scan scan2 =
        Scan.newBuilder(prepareScan())
            // ["text2", "text3"]
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2), true)
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_3), true)
            .build();
    Scan scan3 =
        Scan.newBuilder(prepareScan())
            // ["text1", "text2"]
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1), true)
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_2), true)
            .build();
    Scan scan4 =
        Scan.newBuilder(prepareScan())
            // ("text2", "text3"]
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2), false)
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_3), true)
            .build();
    Scan scan5 =
        Scan.newBuilder(prepareScan())
            // ["text1", "text2")
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1), true)
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_2), false)
            .build();

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
      verifyNoOverlap_ScanWithEndSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text3"]
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_3), true)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2"]
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_2), true)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2")
            .end(Key.ofText(ANY_NAME_2, ANY_TEXT_2), false)
            .build();

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
      verifyNoOverlap_ScanWithStartSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan1 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["text1", infinite)
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_1), true)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["text2", infinite)
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2), true)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ("text2", infinite)
            .start(Key.ofText(ANY_NAME_2, ANY_TEXT_2), false)
            .build();

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
  public void verifyNoOverlap_ScanWithIndexGivenAndPutInWriteSetInSameTable_ShouldThrowException()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexGivenAndPutInWriteSetInDifferentTable_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME_2)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act Assert
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_ScanWithIndexAndPutWithSameIndexKeyGiven_ShouldThrowException()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
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
    Snapshot.Key putKey1 = new Snapshot.Key(put1, binaryCollation());
    Snapshot.Key putKey2 = new Snapshot.Key(put2, binaryCollation());
    snapshot.putIntoWriteSet(putKey1, put1);
    snapshot.putIntoWriteSet(putKey2, put2);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, ANY_TEXT_4))
            .build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexAndPutWithSameIndexKeyGivenButNotOverlappedWithScanWithConjunctions_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
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
    Snapshot.Key putKey1 = new Snapshot.Key(put1, binaryCollation());
    Snapshot.Key putKey2 = new Snapshot.Key(put2, binaryCollation());
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
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_ScanAllGivenAndPutInWriteSetInSameTable_ShouldThrowException()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scanAll =
        ScanAll.newBuilder().namespace(ANY_NAMESPACE_NAME).table(ANY_TABLE_NAME).all().build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scanAll, result, TABLE_METADATA, binaryCollation());

    // Act Assert
    Throwable thrown =
        catchThrowable(
            () -> snapshot.verifyNoOverlap(scanAll, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanAllGivenAndPutInWriteSetNotOverlappingWithScanAll_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scanAll =
        ScanAll.newBuilder().namespace(ANY_NAMESPACE_NAME_2).table(ANY_TABLE_NAME_2).all().build();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scanAll, result, TABLE_METADATA, binaryCollation());

    // Act Assert
    Throwable thrown =
        catchThrowable(
            () -> snapshot.verifyNoOverlap(scanAll, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void verifyNoOverlap_CrossPartitionScanGivenAndPutInSameTable_ShouldThrowException()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan();
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndPutInDifferentNamespace_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan(ANY_NAMESPACE_NAME_2, ANY_TABLE_NAME);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndPutInDifferentTable_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = prepareCrossPartitionScan(ANY_NAMESPACE_NAME, ANY_TABLE_NAME_2);
    TransactionResult result = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, result, TABLE_METADATA, binaryCollation());

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.singletonMap(key, result)));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndAllConditionsMatch_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePutWithIntColumns();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
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
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndAnyConjunctionMatch_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
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
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableAndLikeConditionsMatch_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
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
      verifyNoOverlap_CrossPartitionScanGivenAndNewPutInSameTableButConditionNotMatch_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
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
      verifyNoOverlap_CrossPartitionScanWithoutConjunctionGivenAndNewPutInSameTable_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot();
    Put put = preparePutWithIntColumns();
    Snapshot.Key putKey = new Snapshot.Key(put, binaryCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Scan scan = Scan.newBuilder(prepareCrossPartitionScan()).clearConditions().build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void requiresBeforeIndexValidation_GetWithSecondaryIndex_ShouldReturnTrue() {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareGetWithIndex();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(get, TABLE_METADATA);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void requiresBeforeIndexValidation_ScanWithSecondaryIndex_ShouldReturnTrue() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scan = prepareScanWithIndex();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(scan, TABLE_METADATA);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void requiresBeforeIndexValidation_GetWithPartitionKeyIndex_ShouldReturnFalse() {
    // Arrange
    snapshot = prepareSnapshot();
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(get, TABLE_METADATA_WITH_PK_INDEX);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void requiresBeforeIndexValidation_GetWithPartitionKey_ShouldReturnFalse() {
    // Arrange
    snapshot = prepareSnapshot();
    Get get = prepareGet();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(get, TABLE_METADATA);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void requiresBeforeIndexValidation_ScanAllWithSecondaryIndexCondition_ShouldReturnTrue() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scanAll =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(ConditionBuilder.column(ANY_NAME_4).isEqualToText(ANY_TEXT_4))
            .build();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(scanAll, TABLE_METADATA);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      requiresBeforeIndexValidation_ScanAllWithPartitionKeyIndexCondition_ShouldReturnFalse() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scanAll =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
            .build();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(scanAll, TABLE_METADATA_WITH_PK_INDEX);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      requiresBeforeIndexValidation_ScanAllWithNonIndexedColumnCondition_ShouldReturnFalse() {
    // Arrange
    snapshot = prepareSnapshot();
    Scan scanAll =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
            .build();

    // Act
    boolean result = snapshot.requiresBeforeIndexValidation(scanAll, TABLE_METADATA);

    // Assert
    assertThat(result).isFalse();
  }

  private static DatabaseConfig collationConfig(String... keyValues) {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return new DatabaseConfig(props);
  }

  private static CollationComparator caseInsensitiveIcuCollation() {
    return CollationComparator.from(
        collationConfig(
            DatabaseConfig.COLLATION, "ICU", DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]"));
  }

  private static CollationComparator binaryCollation() {
    return CollationComparator.from(collationConfig(DatabaseConfig.COLLATION, "BINARY"));
  }

  @Test
  public void
      verifyNoOverlap_ScanWithRangeAndCaseInsensitiveCollationGivenAndCaseDifferingWrittenKeyInRange_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePut(ANY_TEXT_1, "Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["apple", "banana"]
            .start(Key.ofText(ANY_NAME_2, "apple"), true)
            .end(Key.ofText(ANY_NAME_2, "banana"), true)
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithRangeAndBinaryCollationGivenAndCaseDifferingWrittenKey_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePut(ANY_TEXT_1, "Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["apple", "banana"]
            .start(Key.ofText(ANY_NAME_2, "apple"), true)
            .end(Key.ofText(ANY_NAME_2, "banana"), true)
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_StartInclusiveBoundaryKeyCollatesEqualButNotByteIdentical_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePut(ANY_TEXT_1, "Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["apple", infinite)
            .start(Key.ofText(ANY_NAME_2, "apple"), true)
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      putIntoWriteSetAndReadSet_CollateEqualButByteDifferentKeys_ShouldRemainDistinctUnderBinary()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put putUpper = preparePut(ANY_TEXT_1, "Apple");
    Put putLower = preparePut(ANY_TEXT_1, "apple");
    Snapshot.Key keyUpper = new Snapshot.Key(putUpper, binaryCollation());
    Snapshot.Key keyLower = new Snapshot.Key(putLower, binaryCollation());

    // Act
    snapshot.putIntoWriteSet(keyUpper, putUpper);
    snapshot.putIntoWriteSet(keyLower, putLower);
    snapshot.putIntoReadSet(keyUpper, Optional.empty());
    snapshot.putIntoReadSet(keyLower, Optional.empty());

    // Assert
    assertThat(keyUpper).isNotEqualTo(keyLower);
    assertThat(writeSet).hasSize(2);
    assertThat(readSet).hasSize(2);
    assertThat(snapshot.containsKeyInWriteSet(keyUpper)).isTrue();
    assertThat(snapshot.containsKeyInWriteSet(keyLower)).isTrue();
    assertThat(snapshot.containsKeyInReadSet(keyUpper)).isTrue();
    assertThat(snapshot.containsKeyInReadSet(keyLower)).isTrue();

    Map<Snapshot.Key, TransactionResult> results = new HashMap<>();
    results.put(keyUpper, prepareResult(ANY_ID, ANY_TEXT_1, "Apple"));
    assertThat(results.containsKey(keyUpper)).isTrue();
    assertThat(results.containsKey(keyLower)).isFalse();
  }

  @Test
  public void
      verifyNoOverlap_ScanWithRangeAndBinaryCollationGivenAndCaseDifferingWrittenKey_ShouldReproduceByteExactBehavior()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePut(ANY_TEXT_1, "Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            // ["apple", "banana"]
            .start(Key.ofText(ANY_NAME_2, "apple"), true)
            .end(Key.ofText(ANY_NAME_2, "banana"), true)
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  // The conjunction tests below turn on one fact: "B" sorts before "a" in byte order (0x42 <
  // 0x61) but after it at PRIMARY strength, so a value "B" matches `col > 'a'` only under the
  // collation.

  private Put preparePutWithName3(String name3Value) {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .textValue(ANY_NAME_3, name3Value)
        .textValue(ANY_NAME_4, ANY_TEXT_4)
        .build();
  }

  private TransactionResult prepareResultWithName3(String txId, String name3Value) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, name3Value))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, txId))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void
      verifyNoOverlap_PlainScanRangeConjunctionAndCaseInsensitiveCollation_WrittenValueMatchesOnlyUnderCollation_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("B");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_PlainScanRangeConjunctionAndBinaryCollation_ShouldReproduceByteExactBehavior()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePutWithName3("B");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_ScanAllRangeConjunctionAndCaseInsensitiveCollation_WrittenValueMatchesOnlyUnderCollation_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("B");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scanAll =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .all()
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();

    // Act
    Throwable thrown =
        catchThrowable(() -> snapshot.verifyNoOverlap(scanAll, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_PlainScanEqualityConjunctionUnderCaseInsensitiveCollation_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("B");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("b"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_CrossPartitionScanEqualityConjunctionUnderCaseInsensitiveCollation_ShouldThrowException()
          throws CrudException {
    // Arrange: preparePut sets ANY_NAME_3 = ANY_TEXT_3 ("text3").
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePut(ANY_TEXT_1, ANY_TEXT_2);
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder(prepareCrossPartitionScan())
            .clearConditions()
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("TEXT3"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      getResult_MergedResultRangeConjunctionUnderCaseInsensitiveCollation_ShouldMatchUnderCollation()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("B");
    Get get =
        Get.newBuilder(prepareGet())
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    Snapshot.Key key = new Snapshot.Key(get, caseInsensitiveIcuCollation());
    snapshot.putIntoGetSet(get, Optional.of(prepareResult(ANY_ID)));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();
  }

  @Test
  public void
      getResult_MergedResultRangeConjunctionUnderBinaryCollation_ShouldReproduceByteExactBehavior()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePutWithName3("B");
    Get get =
        Get.newBuilder(prepareGet())
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    Snapshot.Key key = new Snapshot.Key(get, binaryCollation());
    snapshot.putIntoGetSet(get, Optional.of(prepareResult(ANY_ID)));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isEmpty();
  }

  @Test
  public void
      getResult_MergedResultEqualityConjunctionUnderCaseInsensitiveCollation_ShouldMatchUnderCollation()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("B");
    Get get =
        Get.newBuilder(prepareGet())
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("b"))
            .build();
    Snapshot.Key key = new Snapshot.Key(get, caseInsensitiveIcuCollation());
    snapshot.putIntoGetSet(get, Optional.of(prepareResult(ANY_ID)));
    snapshot.putIntoWriteSet(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();
  }

  @Test
  public void
      toSerializable_GetRangeConjunctionUnderCaseInsensitiveCollation_LatestMatchesOnlyUnderCollation_ShouldThrowValidationConflictException()
          throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Get get =
        Get.newBuilder(prepareGet())
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    snapshot.putIntoGetSet(get, Optional.empty());
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getForStorage = ConsensusCommitUtils.prepareGetForStorage(get, TABLE_METADATA);
    when(storage.get(getForStorage))
        .thenReturn(Optional.of(prepareResultWithName3(ANY_ID + "x", "B")));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);
  }

  @Test
  public void
      toSerializable_GetRangeConjunctionUnderBinaryCollation_ShouldReproduceByteExactBehavior()
          throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Get get =
        Get.newBuilder(prepareGet())
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    snapshot.putIntoGetSet(get, Optional.empty());
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getForStorage = ConsensusCommitUtils.prepareGetForStorage(get, TABLE_METADATA);
    when(storage.get(getForStorage))
        .thenReturn(Optional.of(prepareResultWithName3(ANY_ID + "x", "B")));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();
  }

  @Test
  public void
      toSerializable_ScanRangeConjunctionUnderCaseInsensitiveCollation_LatestMatchesOnlyUnderCollation_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    snapshot.putIntoScanSet(scan, new LinkedHashMap<>());
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage = ConsensusCommitUtils.prepareScanForStorage(scan, TABLE_METADATA);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(prepareResultWithName3(ANY_ID + "x", "B")))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);
  }

  @Test
  public void
      toSerializable_ScanRangeConjunctionUnderBinaryCollation_ShouldReproduceByteExactBehavior()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isGreaterThanText("a"))
            .build();
    snapshot.putIntoScanSet(scan, new LinkedHashMap<>());
    DistributedStorage storage = mock(DistributedStorage.class);
    Scan scanForStorage = ConsensusCommitUtils.prepareScanForStorage(scan, TABLE_METADATA);
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(prepareResultWithName3(ANY_ID + "x", "B")))
        .thenReturn(Optional.empty());
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();
  }

  @Test
  public void
      putIntoWriteSetAndReadSet_CollateEqualByteDifferentKeysUnderCaseInsensitiveIcu_ShouldBeOneLogicalKey()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put putUpper = preparePutWithClusteringKeyAndName3("Apple", "v1");
    Put putLower = preparePutWithClusteringKeyAndName3("apple", "v2");
    Snapshot.Key keyUpper = new Snapshot.Key(putUpper, caseInsensitiveIcuCollation());
    Snapshot.Key keyLower = new Snapshot.Key(putLower, caseInsensitiveIcuCollation());

    // Act
    snapshot.putIntoWriteSet(keyUpper, putUpper);
    snapshot.putIntoWriteSet(keyLower, putLower);
    snapshot.putIntoReadSet(keyUpper, Optional.empty());
    snapshot.putIntoReadSet(keyLower, Optional.empty());

    // Assert
    assertThat(keyUpper).isEqualTo(keyLower);
    assertThat(writeSet).hasSize(1);
    assertThat(readSet).hasSize(1);
    assertThat(snapshot.containsKeyInWriteSet(keyUpper)).isTrue();
    assertThat(snapshot.containsKeyInWriteSet(keyLower)).isTrue();
    assertThat(snapshot.containsKeyInReadSet(keyUpper)).isTrue();
    assertThat(snapshot.containsKeyInReadSet(keyLower)).isTrue();

    Put mergedPut = writeSet.get(keyUpper);
    assertThat(mergedPut).isNotNull();
    assertThat(writeSet.get(keyLower)).isSameAs(mergedPut);
    assertThat(mergedPut.getColumns().get(ANY_NAME_3)).isEqualTo(TextColumn.of(ANY_NAME_3, "v2"));

    Map<Snapshot.Key, TransactionResult> results = new HashMap<>();
    results.put(keyUpper, prepareResult(ANY_ID, ANY_TEXT_1, "Apple"));
    assertThat(results.containsKey(keyUpper)).isTrue();
    assertThat(results.containsKey(keyLower)).isTrue();
  }

  @Test
  public void
      verifyNoOverlap_EqualityConjunctionMatchingOnlyUnderCaseInsensitiveIcu_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithName3("Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_EqualityConjunctionScanAfterWriteUnderBinaryCollation_ShouldReproduceByteExactBehavior()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePutWithName3("Apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  private Put preparePutWithClusteringKeyAndName3(
      String clusteringKeyColumnValue, String name3Value) {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, clusteringKeyColumnValue))
        .textValue(ANY_NAME_3, name3Value)
        .build();
  }

  private TransactionResult prepareResultWithClusteringKeyAndName3(
      String txId, String clusteringKeyColumnValue, String name3Value) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, clusteringKeyColumnValue))
            .put(ANY_NAME_3, TextColumn.of(ANY_NAME_3, name3Value))
            .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_4))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, txId))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexAndCollateEqualIndexValueUnderCaseInsensitiveIcu_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_4, "Apple")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, "apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_ScanWithIndexAndCollateEqualIndexValueUnderBinaryCollation_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_4, "Apple")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .indexKey(Key.ofText(ANY_NAME_4, "apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_PlainScanOfCollateEqualPartitionKeyUnderCaseInsensitiveIcu_ShouldThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePut("apple", ANY_TEXT_2);
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, "Apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      verifyNoOverlap_PlainScanOfCollateEqualPartitionKeyUnderBinaryCollation_ShouldNotThrowException()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePut("apple", ANY_TEXT_2);
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, "Apple"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      verifyNoOverlap_PlainScanOfDistinctUnpairedSurrogatePartitionKeyUnderBinary_ShouldNotThrowException()
          throws CrudException {
    // Arrange: the unpaired surrogates U+D800 and U+DC00 are distinct strings that
    // String#getBytes(UTF_8) would encode identically, so BINARY equality must not go through bytes
    snapshot = prepareSnapshot(binaryCollation());
    Put put = preparePut("\uD800", ANY_TEXT_2);
    snapshot.putIntoWriteSet(new Snapshot.Key(put, binaryCollation()), put);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, "\uDC00"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, Collections.emptyMap()));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      toSerializable_OwnWriteRescannedUnderStoredSpellingUnderCaseInsensitiveIcu_ShouldBeClassifiedAsOwnUpdate()
          throws ExecutionException {
    // Arrange: the writeSet is deliberately left EMPTY. A byte-equal writeSet entry would rescue
    // the original entry via validateScanResults' leftover loop, making this test pass vacuously
    // at any collation.
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();
    TransactionResult originalResult = prepareResult(ANY_ID + "x", ANY_TEXT_1, "apple");
    Snapshot.Key originalKey =
        new Snapshot.Key(scan, originalResult, TABLE_METADATA, caseInsensitiveIcuCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(Collections.singletonMap(originalKey, originalResult)));

    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult latestOwnResult = prepareResult(ANY_ID, ANY_TEXT_1, "Apple");
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(latestOwnResult)).thenReturn(Optional.empty());
    Scan scanForStorage = ConsensusCommitUtils.prepareScanForStorage(scan, TABLE_METADATA);
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_OwnWriteRescannedUnderStoredSpellingUnderBinary_ShouldThrowValidationConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(binaryCollation());
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();
    TransactionResult originalResult = prepareResult(ANY_ID + "x", ANY_TEXT_1, "apple");
    Snapshot.Key originalKey =
        new Snapshot.Key(scan, originalResult, TABLE_METADATA, binaryCollation());
    snapshot.putIntoScanSet(
        scan, Maps.newLinkedHashMap(Collections.singletonMap(originalKey, originalResult)));

    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult latestOwnResult = prepareResult(ANY_ID, ANY_TEXT_1, "Apple");
    Scanner scanner = mock(Scanner.class);
    when(scanner.one()).thenReturn(Optional.of(latestOwnResult)).thenReturn(Optional.empty());
    Scan scanForStorage = ConsensusCommitUtils.prepareScanForStorage(scan, TABLE_METADATA);
    when(storage.scan(scanForStorage)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializable(storage))
        .isInstanceOf(ValidationConflictException.class);

    // Assert
    verify(storage).scan(scanForStorage);
  }

  @Test
  public void
      toSerializable_GetWithCollateEqualBufferedWriteUnderCaseInsensitiveIcu_ShouldSkipGetValidation()
          throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, "Apple"))
            .build();
    snapshot.putIntoGetSet(get, Optional.of(prepareResult(ANY_ID + "x", ANY_TEXT_1, "Apple")));
    Put put = preparePut(ANY_TEXT_1, "apple");
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);
    DistributedStorage storage = mock(DistributedStorage.class);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializable(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage, never()).get(any());
  }

  @Test
  public void
      putIntoDeleteSet_DeleteWithCollateEqualKeyGivenAfterPutUnderCaseInsensitiveIcu_ShouldSupersedeWrite()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePut(ANY_TEXT_1, "apple");
    Snapshot.Key putKey = new Snapshot.Key(put, caseInsensitiveIcuCollation());
    snapshot.putIntoWriteSet(putKey, put);
    Delete delete = prepareDelete(ANY_TEXT_1, "Apple");
    Snapshot.Key deleteKey = new Snapshot.Key(delete, caseInsensitiveIcuCollation());

    // Act
    snapshot.putIntoDeleteSet(deleteKey, delete);

    // Assert
    assertThat(writeSet).isEmpty();
    assertThat(deleteSet).hasSize(1);
    assertThat(deleteSet.get(deleteKey)).isEqualTo(delete);
    assertThat(snapshot.containsKeyInDeleteSet(putKey)).isTrue();
    assertThat(snapshot.containsKeyInWriteSet(putKey)).isFalse();
  }

  @Test
  public void
      putIntoWriteSet_PutWithCollateEqualKeyGivenAfterDeleteUnderCaseInsensitiveIcu_ShouldMoveToWriteSetWithNullColumns()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Delete delete = prepareDelete(ANY_TEXT_1, "apple");
    Snapshot.Key deleteKey = new Snapshot.Key(delete, caseInsensitiveIcuCollation());
    snapshot.putIntoDeleteSet(deleteKey, delete);
    Put put = preparePutWithClusteringKeyAndName3("Apple", ANY_TEXT_3);
    Snapshot.Key putKey = new Snapshot.Key(put, caseInsensitiveIcuCollation());

    // Act
    snapshot.putIntoWriteSet(putKey, put);

    // Assert
    assertThat(deleteSet).isEmpty();
    assertThat(writeSet).hasSize(1);
    assertThat(writeSet).containsKey(deleteKey);
    Put actualPut = writeSet.get(putKey);
    assertThat(actualPut.getColumns().get(ANY_NAME_3))
        .isEqualTo(TextColumn.of(ANY_NAME_3, ANY_TEXT_3));
    assertThat(actualPut.getColumns().get(ANY_NAME_4)).isEqualTo(TextColumn.ofNull(ANY_NAME_4));
    assertThat(ConsensusCommitOperationAttributes.isInsertModeEnabled(actualPut)).isFalse();
    assertThat(ConsensusCommitOperationAttributes.isImplicitPreReadEnabled(actualPut)).isTrue();
  }

  @Test
  public void
      getResult_GetUnderStoredSpellingWithConjunctionMatchingMergedOwnWriteUnderCaseInsensitiveIcu_ShouldReturnMergedResult()
          throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put put = preparePutWithClusteringKeyAndName3("apple", "B");
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, "Apple"))
            .where(ConditionBuilder.column(ANY_NAME_3).isEqualToText("b"))
            .build();
    Snapshot.Key key = new Snapshot.Key(get, caseInsensitiveIcuCollation());
    snapshot.putIntoGetSet(
        get, Optional.of(prepareResultWithClusteringKeyAndName3(ANY_ID + "x", "Apple", "zzz")));
    snapshot.putIntoWriteSet(new Snapshot.Key(put, caseInsensitiveIcuCollation()), put);

    // Act
    Optional<TransactionResult> actual = snapshot.getResult(key, get);

    // Assert
    assertThat(actual).isPresent();
    assertThat(actual.get().getText(ANY_NAME_3)).isEqualTo("B");
  }

  @Test
  public void readYourOwnWrite_CollateEqualKeyFromStorage_ShouldSeeOwnBufferedWrite()
      throws CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Snapshot.Key storageKey =
        new Snapshot.Key(
            preparePutWithClusteringKeyAndName3("Apple", "ignored"), caseInsensitiveIcuCollation());
    snapshot.putIntoReadSet(
        storageKey,
        Optional.of(prepareResultWithClusteringKeyAndName3(ANY_ID + "x", "Apple", "stale")));
    Put ownWrite = preparePutWithClusteringKeyAndName3("apple", "updated");
    snapshot.putIntoWriteSet(new Snapshot.Key(ownWrite, caseInsensitiveIcuCollation()), ownWrite);

    // Act
    Optional<TransactionResult> result = snapshot.getResult(storageKey);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getText(ANY_NAME_3))
        .as("read-your-own-writes must reflect the buffered write, as the CI backend would")
        .isEqualTo("updated");
  }

  @Test
  public void prepare_CollateEqualReadAndWriteKeys_ComposerShouldReceiveBeforeImage()
      throws ExecutionException, CrudException {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    TransactionResult beforeImage =
        prepareResultWithClusteringKeyAndName3(ANY_ID + "x", "Apple", "old");
    snapshot.putIntoReadSet(
        new Snapshot.Key(
            preparePutWithClusteringKeyAndName3("Apple", "ignored"), caseInsensitiveIcuCollation()),
        Optional.of(beforeImage));
    Put ownWrite = preparePutWithClusteringKeyAndName3("apple", "new");
    snapshot.putIntoWriteSet(new Snapshot.Key(ownWrite, caseInsensitiveIcuCollation()), ownWrite);

    // Act
    snapshot.to(prepareComposer);

    // Assert: the composer receives the before image, not null, which would flip it into the
    // putIfNotExists insert branch.
    verify(prepareComposer).add(ownWrite, beforeImage);
  }

  @Test
  public void writeSet_TwoCollateEqualPuts_ShouldMergeIntoOneLogicalEntry() throws CrudException {
    // Arrange + Act
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Put first = preparePutWithClusteringKeyAndName3("banana", "v1");
    Put second = preparePutWithClusteringKeyAndName3("BANANA", "v2");
    snapshot.putIntoWriteSet(new Snapshot.Key(first, caseInsensitiveIcuCollation()), first);
    snapshot.putIntoWriteSet(new Snapshot.Key(second, caseInsensitiveIcuCollation()), second);

    // Assert
    assertThat(writeSet)
        .as("collate-equal puts must merge into one logical write, as the CI backend holds one row")
        .hasSize(1);

    Snapshot.Key firstKey = new Snapshot.Key(first, caseInsensitiveIcuCollation());
    Snapshot.Key secondKey = new Snapshot.Key(second, caseInsensitiveIcuCollation());
    Put mergedPut = writeSet.get(firstKey);
    assertThat(mergedPut).isNotNull();
    assertThat(writeSet.get(secondKey)).isSameAs(mergedPut);
    assertThat(mergedPut.getColumns().get(ANY_NAME_3)).isEqualTo(TextColumn.of(ANY_NAME_3, "v2"));
  }

  @Test
  public void verifyNoOverlap_ScanSeesRowDeletedUnderCollateEqualKey_ShouldThrow() {
    // Arrange
    snapshot = prepareSnapshot(caseInsensitiveIcuCollation());
    Delete ownDelete = prepareDelete(ANY_TEXT_1, "apple");
    snapshot.putIntoDeleteSet(
        new Snapshot.Key(ownDelete, caseInsensitiveIcuCollation()), ownDelete);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults = new LinkedHashMap<>();
    scanResults.put(
        new Snapshot.Key(
            preparePutWithClusteringKeyAndName3("Apple", "ignored"), caseInsensitiveIcuCollation()),
        prepareResultWithClusteringKeyAndName3(ANY_ID + "x", "Apple", "v"));
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, scanResults));

    // Assert
    assertThat(thrown)
        .as("scan-after-delete on a collate-equal key must be detected as an overlap")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
