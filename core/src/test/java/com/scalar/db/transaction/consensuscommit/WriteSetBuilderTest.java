package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WriteSetBuilderTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String TX_ID = "tx-id";

  private TransactionTableMetadataManager tableMetadataManager;
  private ParallelExecutor parallelExecutor;
  private WriteSetBuilder writeSetBuilder;

  @BeforeEach
  void setUp() throws Exception {
    tableMetadataManager = mock(TransactionTableMetadataManager.class);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.TEXT)
            .addColumn("ck", DataType.INT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .build();
    TransactionTableMetadata transactionTableMetadata =
        new TransactionTableMetadata(
            ConsensusCommitUtils.buildTransactionTableMetadata(tableMetadata, false));
    when(tableMetadataManager.getTransactionTableMetadata(anyString(), anyString()))
        .thenReturn(transactionTableMetadata);
    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(transactionTableMetadata);

    parallelExecutor = new ParallelExecutor(mock(ConsensusCommitConfig.class));
    writeSetBuilder = new WriteSetBuilder(tableMetadataManager);
  }

  private Snapshot newSnapshot() {
    return new Snapshot(TX_ID, tableMetadataManager, parallelExecutor);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_NonGroupCommitWithPutAndDelete_ShouldBuildEntryGroupWithoutChildId(
      boolean includeColumns) throws Exception {
    // Arrange
    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p1"))
            .clusteringKey(Key.ofInt("ck", 10))
            .textValue("v", "val1")
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p2"))
            .clusteringKey(Key.ofInt("ck", 20))
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, includeColumns);

    // Assert
    assertThat(group.hasChildId()).isFalse();
    assertThat(group.getEntriesList()).hasSize(2);

    Entry put1 = group.getEntries(0);
    assertThat(put1.getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
    assertThat(put1.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(put1.getTableName()).isEqualTo(TABLE);
    assertThat(put1.getPartitionKey().getColumnsList()).hasSize(1);
    assertThat(put1.getPartitionKey().getColumns(0).getName()).isEqualTo("pk");
    assertThat(put1.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("p1");
    assertThat(put1.hasClusteringKey()).isTrue();
    assertThat(put1.getClusteringKey().getColumns(0).getIntValue().getValue()).isEqualTo(10);
    if (includeColumns) {
      assertThat(put1.getColumnsList()).hasSize(1);
      assertThat(put1.getColumns(0).getName()).isEqualTo("v");
      assertThat(put1.getColumns(0).getTextValue().getValue()).isEqualTo("val1");
    } else {
      assertThat(put1.getColumnsList()).isEmpty();
    }

    // Delete entries never carry non-key columns regardless of includeColumns.
    Entry delete1 = group.getEntries(1);
    assertThat(delete1.getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_DELETE);
    assertThat(delete1.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(delete1.getTableName()).isEqualTo(TABLE);
    assertThat(delete1.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("p2");
    assertThat(delete1.getClusteringKey().getColumns(0).getIntValue().getValue()).isEqualTo(20);
    assertThat(delete1.getColumnsList()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_GroupCommitChild_ShouldSetChildId(boolean includeColumns) throws Exception {
    // Arrange
    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p1"))
            .clusteringKey(Key.ofInt("ck", 1))
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, "child-1", includeColumns);

    // Assert
    assertThat(group.hasChildId()).isTrue();
    assertThat(group.getChildId()).isEqualTo("child-1");
    assertThat(group.getEntriesList()).hasSize(1);
    assertThat(group.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_NoWritesOrDeletes_ShouldBuildEmptyEntryGroup(boolean includeColumns) {
    // Arrange
    Snapshot snapshot = newSnapshot();

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, includeColumns);

    // Assert
    assertThat(group.hasChildId()).isFalse();
    assertThat(group.getEntriesList()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_CompositeKey_ShouldEncodeAllKeyColumns(boolean includeColumns)
      throws Exception {
    // Arrange
    TableMetadata compositeKeyMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk1", DataType.TEXT)
            .addColumn("pk2", DataType.INT)
            .addColumn("ck1", DataType.BIGINT)
            .addColumn("ck2", DataType.TEXT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1")
            .addClusteringKey("ck2")
            .build();
    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(
            new TransactionTableMetadata(
                ConsensusCommitUtils.buildTransactionTableMetadata(compositeKeyMetadata, false)));

    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.newBuilder().addText("pk1", "p").addInt("pk2", 7).build())
            .clusteringKey(Key.newBuilder().addBigInt("ck1", 100L).addText("ck2", "c").build())
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, includeColumns);

    // Assert
    Entry entry = group.getEntries(0);
    assertThat(entry.getPartitionKey().getColumnsList()).hasSize(2);
    assertThat(entry.getPartitionKey().getColumns(0).getName()).isEqualTo("pk1");
    assertThat(entry.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("p");
    assertThat(entry.getPartitionKey().getColumns(1).getName()).isEqualTo("pk2");
    assertThat(entry.getPartitionKey().getColumns(1).getIntValue().getValue()).isEqualTo(7);
    assertThat(entry.getClusteringKey().getColumnsList()).hasSize(2);
    assertThat(entry.getClusteringKey().getColumns(0).getBigintValue().getValue()).isEqualTo(100L);
    assertThat(entry.getClusteringKey().getColumns(1).getTextValue().getValue()).isEqualTo("c");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_BooleanKey_ShouldEncodeBooleanValue(boolean includeColumns)
      throws Exception {
    EntryGroup group = encodeKey(DataType.BOOLEAN, Key.ofBoolean("pk", true), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBooleanValue().getValue())
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_BigIntKey_ShouldEncodeBigIntValue(boolean includeColumns) throws Exception {
    EntryGroup group =
        encodeKey(DataType.BIGINT, Key.ofBigInt("pk", 12345678901234L), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBigintValue().getValue())
        .isEqualTo(12345678901234L);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_FloatKey_ShouldEncodeFloatValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.FLOAT, Key.ofFloat("pk", 1.25f), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getFloatValue().getValue())
        .isEqualTo(1.25f);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_DoubleKey_ShouldEncodeDoubleValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.DOUBLE, Key.ofDouble("pk", 12.345), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDoubleValue().getValue())
        .isEqualTo(12.345);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_BlobKey_ShouldEncodeBlobValue(boolean includeColumns) throws Exception {
    byte[] blobValue = new byte[] {1, 2, 3, 4};
    EntryGroup group = encodeKey(DataType.BLOB, Key.ofBlob("pk", blobValue), includeColumns);
    assertThat(
            group
                .getEntries(0)
                .getPartitionKey()
                .getColumns(0)
                .getBlobValue()
                .getValue()
                .toByteArray())
        .containsExactly(blobValue);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_DateKey_ShouldEncodeDateValueAsEpochDay(boolean includeColumns)
      throws Exception {
    LocalDate date = LocalDate.of(2026, 5, 10);
    EntryGroup group = encodeKey(DataType.DATE, Key.ofDate("pk", date), includeColumns);
    int expected = TimeRelatedColumnEncodingUtils.encode(date);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDateValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_TimeKey_ShouldEncodeTimeValueAsNanoOfDay(boolean includeColumns)
      throws Exception {
    LocalTime time = LocalTime.of(12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIME, Key.ofTime("pk", time), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(time);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimeValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_TimestampKey_ShouldEncodeTimestampValue(boolean includeColumns)
      throws Exception {
    LocalDateTime ts = LocalDateTime.of(2026, 5, 10, 12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIMESTAMP, Key.ofTimestamp("pk", ts), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(ts);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimestampValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_TimestampTZKey_ShouldEncodeTimestampTZValue(boolean includeColumns)
      throws Exception {
    Instant instant = Instant.ofEpochSecond(1747000000L);
    EntryGroup group =
        encodeKey(DataType.TIMESTAMPTZ, Key.ofTimestampTZ("pk", instant), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(instant);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimestamptzValue().getValue())
        .isEqualTo(expected);
  }

  /**
   * Builds an EntryGroup containing a single Put whose single-column partition key has the given
   * type and value. Helper used by the per-type encoding tests above.
   *
   * @param pkType the data type of the partition key column
   * @param partitionKey the partition key value
   * @param includeColumns whether to include non-key column values in the resulting entries
   * @return the built {@link EntryGroup}
   * @throws Exception if table metadata setup fails
   */
  private EntryGroup encodeKey(DataType pkType, Key partitionKey, boolean includeColumns)
      throws Exception {
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", pkType)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("pk")
            .build();
    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(
            new TransactionTableMetadata(
                ConsensusCommitUtils.buildTransactionTableMetadata(metadata, false)));

    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(partitionKey)
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    return writeSetBuilder.buildEntryGroup(snapshot, null, includeColumns);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildEntryGroup_PartitionKeyOnly_ShouldOmitClusteringKey(boolean includeColumns)
      throws Exception {
    // Arrange
    Snapshot snapshot = newSnapshot();
    TableMetadata pkOnlyMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.TEXT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("pk")
            .build();
    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(
            new TransactionTableMetadata(
                ConsensusCommitUtils.buildTransactionTableMetadata(pkOnlyMetadata, false)));

    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p1"))
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, includeColumns);

    // Assert
    assertThat(group.getEntriesList()).hasSize(1);
    Entry entry = group.getEntries(0);
    assertThat(entry.hasClusteringKey()).isFalse();
    assertThat(entry.getPartitionKey().getColumns(0).getName()).isEqualTo("pk");
  }

  @Test
  void buildEntryGroup_IncludeColumnsTrue_WithNullValuedColumn_ShouldEmitEmptyInnerValue()
      throws Exception {
    // Arrange — a Put with a null-valued non-key column. The ColumnToProto visitor should encode
    // it as a Column whose inner TextValue carries no `value` field (proto3 default).
    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p1"))
            .clusteringKey(Key.ofInt("ck", 1))
            .textValue("v", null)
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, true);

    // Assert
    Entry putEntry = group.getEntries(0);
    assertThat(putEntry.getColumnsList()).hasSize(1);
    assertThat(putEntry.getColumns(0).getName()).isEqualTo("v");
    // hasValue() on the inner TextValue is false because the proto field is unset (null marker).
    assertThat(putEntry.getColumns(0).getTextValue().hasValue()).isFalse();
  }

  @Test
  void buildEntryGroup_IncludeColumnsTrue_ShouldFilterTransactionMetaColumns() throws Exception {
    // Arrange — a Put that carries both a user column ("v") and ConsensusCommit-injected
    // transaction-meta columns (tx_state and before_v). The latter must not appear in the emitted
    // entry when includeColumns is true.
    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("pk", "p1"))
            .clusteringKey(Key.ofInt("ck", 1))
            .textValue("v", "user-value")
            .intValue(Attribute.STATE, TransactionState.PREPARED.get())
            .textValue(Attribute.BEFORE_PREFIX + "v", "old-value")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);

    // Act
    EntryGroup group = writeSetBuilder.buildEntryGroup(snapshot, null, true);

    // Assert — only the user column survives the filter.
    Entry putEntry = group.getEntries(0);
    assertThat(putEntry.getColumnsList()).hasSize(1);
    assertThat(putEntry.getColumns(0).getName()).isEqualTo("v");
    assertThat(putEntry.getColumns(0).getTextValue().getValue()).isEqualTo("user-value");
  }
}
