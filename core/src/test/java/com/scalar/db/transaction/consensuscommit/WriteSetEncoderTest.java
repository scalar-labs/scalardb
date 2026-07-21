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
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WriteSetEncoderTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String TX_ID = "tx-id";

  private TransactionTableMetadataManager tableMetadataManager;
  private ParallelExecutor parallelExecutor;
  private WriteSetEncoder writeSetEncoder;

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
    writeSetEncoder = new WriteSetEncoder(tableMetadataManager);
  }

  private Snapshot newSnapshot() {
    return new Snapshot(TX_ID, tableMetadataManager, parallelExecutor);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_NonGroupCommitWithPutAndDelete_ShouldEncodeEntryGroupWithoutChildId(
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, includeColumns);

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
  void encodeEntryGroup_GroupCommitChild_ShouldSetChildId(boolean includeColumns) throws Exception {
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, "child-1", includeColumns);

    // Assert
    assertThat(group.hasChildId()).isTrue();
    assertThat(group.getChildId()).isEqualTo("child-1");
    assertThat(group.getEntriesList()).hasSize(1);
    assertThat(group.getEntries(0).getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_NoWritesOrDeletes_ShouldEncodeEmptyEntryGroup(boolean includeColumns) {
    // Arrange
    Snapshot snapshot = newSnapshot();

    // Act
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, includeColumns);

    // Assert
    assertThat(group.hasChildId()).isFalse();
    assertThat(group.getEntriesList()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_CompositeKey_ShouldEncodeAllKeyColumns(boolean includeColumns)
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, includeColumns);

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
  void encodeEntryGroup_BooleanKey_ShouldEncodeBooleanValue(boolean includeColumns)
      throws Exception {
    EntryGroup group = encodeKey(DataType.BOOLEAN, Key.ofBoolean("pk", true), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBooleanValue().getValue())
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_IntKey_ShouldEncodeIntValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.INT, Key.ofInt("pk", 42), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getIntValue().getValue())
        .isEqualTo(42);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_TextKey_ShouldEncodeTextValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.TEXT, Key.ofText("pk", "hello"), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTextValue().getValue())
        .isEqualTo("hello");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_BigIntKey_ShouldEncodeBigIntValue(boolean includeColumns) throws Exception {
    EntryGroup group =
        encodeKey(DataType.BIGINT, Key.ofBigInt("pk", 12345678901234L), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBigintValue().getValue())
        .isEqualTo(12345678901234L);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_FloatKey_ShouldEncodeFloatValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.FLOAT, Key.ofFloat("pk", 1.25f), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getFloatValue().getValue())
        .isEqualTo(1.25f);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_DoubleKey_ShouldEncodeDoubleValue(boolean includeColumns) throws Exception {
    EntryGroup group = encodeKey(DataType.DOUBLE, Key.ofDouble("pk", 12.345), includeColumns);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDoubleValue().getValue())
        .isEqualTo(12.345);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_BlobKey_ShouldEncodeBlobValue(boolean includeColumns) throws Exception {
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
  void encodeEntryGroup_DateKey_ShouldEncodeDateValueAsEpochDay(boolean includeColumns)
      throws Exception {
    LocalDate date = LocalDate.of(2026, 5, 10);
    EntryGroup group = encodeKey(DataType.DATE, Key.ofDate("pk", date), includeColumns);
    int expected = TimeRelatedColumnEncodingUtils.encode(date);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDateValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_TimeKey_ShouldEncodeTimeValueAsNanoOfDay(boolean includeColumns)
      throws Exception {
    LocalTime time = LocalTime.of(12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIME, Key.ofTime("pk", time), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(time);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimeValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_TimestampKey_ShouldEncodeTimestampValue(boolean includeColumns)
      throws Exception {
    LocalDateTime ts = LocalDateTime.of(2026, 5, 10, 12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIMESTAMP, Key.ofTimestamp("pk", ts), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(ts);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimestampValue().getValue())
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_TimestampTZKey_ShouldEncodeTimestampTZValue(boolean includeColumns)
      throws Exception {
    Instant instant = Instant.ofEpochSecond(1747000000L);
    EntryGroup group =
        encodeKey(DataType.TIMESTAMPTZ, Key.ofTimestampTZ("pk", instant), includeColumns);
    long expected = TimeRelatedColumnEncodingUtils.encode(instant);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimestamptzValue().getValue())
        .isEqualTo(expected);
  }

  /**
   * Encodes an EntryGroup containing a single Put whose single-column partition key has the given
   * type and value. Helper used by the per-type encoding tests above.
   *
   * @param pkType the data type of the partition key column
   * @param partitionKey the partition key value
   * @param includeColumns whether to include non-key column values in the resulting entries
   * @return the encoded {@link EntryGroup}
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
    return writeSetEncoder.encodeEntryGroup(snapshot, null, includeColumns);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodeEntryGroup_PartitionKeyOnly_ShouldOmitClusteringKey(boolean includeColumns)
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, includeColumns);

    // Assert
    assertThat(group.getEntriesList()).hasSize(1);
    Entry entry = group.getEntries(0);
    assertThat(entry.hasClusteringKey()).isFalse();
    assertThat(entry.getPartitionKey().getColumns(0).getName()).isEqualTo("pk");
  }

  @Test
  void encodeEntryGroup_IncludeColumnsTrue_WithNullValuedColumn_ShouldEmitEmptyInnerValue()
      throws Exception {
    // Arrange — a Put with a null-valued non-key column. The encoding visitor should encode
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, true);

    // Assert
    Entry putEntry = group.getEntries(0);
    assertThat(putEntry.getColumnsList()).hasSize(1);
    assertThat(putEntry.getColumns(0).getName()).isEqualTo("v");
    // hasValue() on the inner TextValue is false because the proto field is unset (null marker).
    assertThat(putEntry.getColumns(0).getTextValue().hasValue()).isFalse();
  }

  @Test
  void encodeEntryGroup_IncludeColumnsTrue_ShouldFilterTransactionMetaColumns() throws Exception {
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, true);

    // Assert — only the user column survives the filter.
    Entry putEntry = group.getEntries(0);
    assertThat(putEntry.getColumnsList()).hasSize(1);
    assertThat(putEntry.getColumns(0).getName()).isEqualTo("v");
    assertThat(putEntry.getColumns(0).getTextValue().getValue()).isEqualTo("user-value");
  }

  @Test
  void
      encodeFromWriteSetEntries_GivenWriteSetsFromMultipleParticipants_ShouldGroupByParticipantAndStampParticipantId() {
    // Arrange — p1 owns two entries, p2 owns one; map iteration order is p1 then p2.
    TwoPhaseCommitParticipant.WriteSetEntry p1Write =
        writeSetEntry(
            TwoPhaseCommitParticipant.WriteSetEntry.Type.WRITE,
            Key.ofText("pk", "a"),
            Optional.of(Key.ofInt("ck", 1)));
    TwoPhaseCommitParticipant.WriteSetEntry p2Write =
        writeSetEntry(
            TwoPhaseCommitParticipant.WriteSetEntry.Type.WRITE,
            Key.ofText("pk", "b"),
            Optional.empty());
    TwoPhaseCommitParticipant.WriteSetEntry p1Delete =
        writeSetEntry(
            TwoPhaseCommitParticipant.WriteSetEntry.Type.DELETE,
            Key.ofText("pk", "c"),
            Optional.of(Key.ofInt("ck", 2)));

    Map<String, List<TwoPhaseCommitParticipant.WriteSetEntry>> writeSetsByParticipant =
        new LinkedHashMap<>();
    writeSetsByParticipant.put("p1", Arrays.asList(p1Write, p1Delete));
    writeSetsByParticipant.put("p2", Collections.singletonList(p2Write));

    // Act
    WriteSet writeSet =
        WriteSetEncoder.encodeFromWriteSetEntries(
            writeSetsByParticipant, /* includeColumns= */ false);

    // Assert — two groups in map iteration order (p1, then p2); p1 holds both its entries.
    assertThat(writeSet.getSchemaVersion()).isEqualTo(1);
    assertThat(writeSet.getEntryGroupsList()).hasSize(2);

    EntryGroup p1Group = writeSet.getEntryGroups(0);
    assertThat(p1Group.hasChildId()).isFalse();
    assertThat(p1Group.getEntriesList()).hasSize(2);
    Entry p1FirstEntry = p1Group.getEntries(0);
    assertThat(p1FirstEntry.getParticipantId()).isEqualTo("p1");
    assertThat(p1FirstEntry.getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_WRITE);
    assertThat(p1FirstEntry.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(p1FirstEntry.getTableName()).isEqualTo(TABLE);
    assertThat(p1FirstEntry.hasClusteringKey()).isTrue();
    // includeColumns=false: non-key columns are not persisted.
    assertThat(p1FirstEntry.getColumnsList()).isEmpty();
    Entry p1SecondEntry = p1Group.getEntries(1);
    assertThat(p1SecondEntry.getParticipantId()).isEqualTo("p1");
    assertThat(p1SecondEntry.getEntryType()).isEqualTo(Entry.EntryType.ENTRY_TYPE_DELETE);

    EntryGroup p2Group = writeSet.getEntryGroups(1);
    assertThat(p2Group.getEntriesList()).hasSize(1);
    Entry p2Entry = p2Group.getEntries(0);
    assertThat(p2Entry.getParticipantId()).isEqualTo("p2");
    assertThat(p2Entry.hasClusteringKey()).isFalse();
  }

  @Test
  void encodeFromWriteSetEntries_WhenIncludeColumns_ShouldEncodeNonKeyColumnsOfWriteEntries() {
    // Arrange — a WRITE entry carrying one non-key column (the participant already filtered out any
    // ConsensusCommit-internal columns).
    TwoPhaseCommitParticipant.WriteSetEntry write =
        writeSetEntry(
            TwoPhaseCommitParticipant.WriteSetEntry.Type.WRITE,
            Key.ofText("pk", "a"),
            Optional.empty());
    when(write.getColumns()).thenReturn(Collections.singletonList(TextColumn.of("v", "val")));
    Map<String, List<TwoPhaseCommitParticipant.WriteSetEntry>> writeSetsByParticipant =
        new LinkedHashMap<>();
    writeSetsByParticipant.put("p1", Collections.singletonList(write));

    // Act
    WriteSet writeSet =
        WriteSetEncoder.encodeFromWriteSetEntries(
            writeSetsByParticipant, /* includeColumns= */ true);

    // Assert — the non-key column is encoded on the entry.
    Entry entry = writeSet.getEntryGroups(0).getEntries(0);
    assertThat(entry.getColumnsList()).hasSize(1);
    assertThat(entry.getColumns(0).getName()).isEqualTo("v");
    assertThat(entry.getColumns(0).getTextValue().getValue()).isEqualTo("val");
  }

  @Test
  void encodeFromWriteSetEntries_WhenAParticipantHasNoEntries_ShouldSkipThatParticipant() {
    // Arrange — p1 owns one entry, p2 owns none (e.g. a read-only participant). The empty
    // participant must not contribute an EntryGroup.
    TwoPhaseCommitParticipant.WriteSetEntry p1Write =
        writeSetEntry(
            TwoPhaseCommitParticipant.WriteSetEntry.Type.WRITE,
            Key.ofText("pk", "a"),
            Optional.empty());
    Map<String, List<TwoPhaseCommitParticipant.WriteSetEntry>> writeSetsByParticipant =
        new LinkedHashMap<>();
    writeSetsByParticipant.put("p1", Collections.singletonList(p1Write));
    writeSetsByParticipant.put("p2", Collections.emptyList());

    // Act
    WriteSet writeSet =
        WriteSetEncoder.encodeFromWriteSetEntries(
            writeSetsByParticipant, /* includeColumns= */ false);

    // Assert — only p1's group is emitted; the empty p2 is skipped.
    assertThat(writeSet.getEntryGroupsList()).hasSize(1);
    assertThat(writeSet.getEntryGroups(0).getEntries(0).getParticipantId()).isEqualTo("p1");
  }

  private static TwoPhaseCommitParticipant.WriteSetEntry writeSetEntry(
      TwoPhaseCommitParticipant.WriteSetEntry.Type type,
      Key partitionKey,
      Optional<Key> clusteringKey) {
    TwoPhaseCommitParticipant.WriteSetEntry entry =
        mock(TwoPhaseCommitParticipant.WriteSetEntry.class);
    when(entry.getType()).thenReturn(type);
    when(entry.getNamespaceName()).thenReturn(NAMESPACE);
    when(entry.getTableName()).thenReturn(TABLE);
    when(entry.getPartitionKey()).thenReturn(partitionKey);
    when(entry.getClusteringKey()).thenReturn(clusteringKey);
    return entry;
  }
}
