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
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
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

class WriteSetEncoderTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String TX_ID = "tx-id";

  private TransactionTableMetadataManager tableMetadataManager;
  private ParallelExecutor parallelExecutor;

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
  }

  private Snapshot newSnapshot() {
    return new Snapshot(TX_ID, tableMetadataManager, parallelExecutor);
  }

  @Test
  void encodeEntryGroup_NonGroupCommitWithPutAndDelete_ShouldEncodeEntryGroupWithoutChildId()
      throws Exception {
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
    EntryGroup group = WriteSetEncoder.encodeEntryGroup(snapshot, null);

    // Assert
    assertThat(group.hasChildId()).isFalse();
    assertThat(group.getEntriesList()).hasSize(2);

    Entry put1 = group.getEntries(0);
    assertThat(put1.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(put1.getTableName()).isEqualTo(TABLE);
    assertThat(put1.getPartitionKey().getColumnsList()).hasSize(1);
    assertThat(put1.getPartitionKey().getColumns(0).getName()).isEqualTo("pk");
    assertThat(put1.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("p1");
    assertThat(put1.hasClusteringKey()).isTrue();
    assertThat(put1.getClusteringKey().getColumns(0).getIntValue().getValue()).isEqualTo(10);

    Entry delete1 = group.getEntries(1);
    assertThat(delete1.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(delete1.getTableName()).isEqualTo(TABLE);
    assertThat(delete1.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("p2");
    assertThat(delete1.getClusteringKey().getColumns(0).getIntValue().getValue()).isEqualTo(20);
  }

  @Test
  void encodeEntryGroup_GroupCommitChild_ShouldSetChildId() throws Exception {
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
    EntryGroup group = WriteSetEncoder.encodeEntryGroup(snapshot, "child-1");

    // Assert
    assertThat(group.hasChildId()).isTrue();
    assertThat(group.getChildId()).isEqualTo("child-1");
    assertThat(group.getEntriesList()).hasSize(1);
  }

  @Test
  void encodeEntryGroup_NoWritesOrDeletes_ShouldEncodeEmptyEntryGroup() {
    // Arrange
    Snapshot snapshot = newSnapshot();

    // Act
    EntryGroup group = WriteSetEncoder.encodeEntryGroup(snapshot, null);

    // Assert
    assertThat(group.hasChildId()).isFalse();
    assertThat(group.getEntriesList()).isEmpty();
  }

  @Test
  void encodeSingleGroupWriteSet_ReadOnly_ShouldSetEmptyEntryGroupsPayload() {
    // Arrange
    TransactionContext context =
        new TransactionContext(TX_ID, newSnapshot(), Isolation.SNAPSHOT, false, false, false);

    // Act
    WriteSet writeSet = WriteSetEncoder.encodeSingleGroupWriteSet(context);

    // Assert — the payload oneof is always set, never left absent. The unsupported-payload guard
    // in ConsensusCommitManager reads an unset payload as "written by a newer schema version", so
    // a read-only commit must still set the case rather than omit the payload.
    assertThat(writeSet.getPayloadCase()).isEqualTo(WriteSet.PayloadCase.ENTRY_GROUPS);
    assertThat(writeSet.getEntryGroups().getEntryGroupsList()).isEmpty();
    assertThat(writeSet.getSchemaVersion()).isEqualTo(1);
  }

  @Test
  void encodeEntryGroup_CompositeKey_ShouldEncodeAllKeyColumns() throws Exception {
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
    EntryGroup group = WriteSetEncoder.encodeEntryGroup(snapshot, null);

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

  @Test
  void encodeEntryGroup_BooleanKey_ShouldEncodeBooleanValue() throws Exception {
    EntryGroup group = encodeKey(DataType.BOOLEAN, Key.ofBoolean("pk", true));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBooleanValue().getValue())
        .isTrue();
  }

  @Test
  void encodeEntryGroup_IntKey_ShouldEncodeIntValue() throws Exception {
    EntryGroup group = encodeKey(DataType.INT, Key.ofInt("pk", 42));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getIntValue().getValue())
        .isEqualTo(42);
  }

  @Test
  void encodeEntryGroup_TextKey_ShouldEncodeTextValue() throws Exception {
    EntryGroup group = encodeKey(DataType.TEXT, Key.ofText("pk", "hello"));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTextValue().getValue())
        .isEqualTo("hello");
  }

  @Test
  void encodeEntryGroup_BigIntKey_ShouldEncodeBigIntValue() throws Exception {
    EntryGroup group = encodeKey(DataType.BIGINT, Key.ofBigInt("pk", 12345678901234L));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getBigintValue().getValue())
        .isEqualTo(12345678901234L);
  }

  @Test
  void encodeEntryGroup_FloatKey_ShouldEncodeFloatValue() throws Exception {
    EntryGroup group = encodeKey(DataType.FLOAT, Key.ofFloat("pk", 1.25f));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getFloatValue().getValue())
        .isEqualTo(1.25f);
  }

  @Test
  void encodeEntryGroup_DoubleKey_ShouldEncodeDoubleValue() throws Exception {
    EntryGroup group = encodeKey(DataType.DOUBLE, Key.ofDouble("pk", 12.345));
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDoubleValue().getValue())
        .isEqualTo(12.345);
  }

  @Test
  void encodeEntryGroup_BlobKey_ShouldEncodeBlobValue() throws Exception {
    byte[] blobValue = new byte[] {1, 2, 3, 4};
    EntryGroup group = encodeKey(DataType.BLOB, Key.ofBlob("pk", blobValue));
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

  @Test
  void encodeEntryGroup_DateKey_ShouldEncodeDateValueAsEpochDay() throws Exception {
    LocalDate date = LocalDate.of(2026, 5, 10);
    EntryGroup group = encodeKey(DataType.DATE, Key.ofDate("pk", date));
    int expected = TimeRelatedColumnEncodingUtils.encode(date);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getDateValue().getValue())
        .isEqualTo(expected);
  }

  @Test
  void encodeEntryGroup_TimeKey_ShouldEncodeTimeValueAsNanoOfDay() throws Exception {
    LocalTime time = LocalTime.of(12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIME, Key.ofTime("pk", time));
    long expected = TimeRelatedColumnEncodingUtils.encode(time);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimeValue().getValue())
        .isEqualTo(expected);
  }

  @Test
  void encodeEntryGroup_TimestampKey_ShouldEncodeTimestampValue() throws Exception {
    LocalDateTime ts = LocalDateTime.of(2026, 5, 10, 12, 34, 56);
    EntryGroup group = encodeKey(DataType.TIMESTAMP, Key.ofTimestamp("pk", ts));
    long expected = TimeRelatedColumnEncodingUtils.encode(ts);
    assertThat(group.getEntries(0).getPartitionKey().getColumns(0).getTimestampValue().getValue())
        .isEqualTo(expected);
  }

  @Test
  void encodeEntryGroup_TimestampTZKey_ShouldEncodeTimestampTZValue() throws Exception {
    Instant instant = Instant.ofEpochSecond(1747000000L);
    EntryGroup group = encodeKey(DataType.TIMESTAMPTZ, Key.ofTimestampTZ("pk", instant));
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
   * @return the encoded {@link EntryGroup}
   * @throws Exception if table metadata setup fails
   */
  private EntryGroup encodeKey(DataType pkType, Key partitionKey) throws Exception {
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
    return WriteSetEncoder.encodeEntryGroup(snapshot, null);
  }

  @Test
  void encodeEntryGroup_PartitionKeyOnly_ShouldOmitClusteringKey() throws Exception {
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
    EntryGroup group = WriteSetEncoder.encodeEntryGroup(snapshot, null);

    // Assert
    assertThat(group.getEntriesList()).hasSize(1);
    Entry entry = group.getEntries(0);
    assertThat(entry.hasClusteringKey()).isFalse();
    assertThat(entry.getPartitionKey().getColumns(0).getName()).isEqualTo("pk");
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
    WriteSet writeSet = WriteSetEncoder.encodeFromWriteSetEntries(writeSetsByParticipant);

    // Assert — two groups in map iteration order (p1, then p2); p1 holds both its entries, in the
    // order they were listed. Each entry's partition key is asserted too: the participant id alone
    // would not distinguish p1's two entries from each other, so the assertions would hold even if
    // the encoder emitted one of them twice or swapped them.
    assertThat(writeSet.getSchemaVersion()).isEqualTo(1);
    assertThat(writeSet.getEntryGroups().getEntryGroupsList()).hasSize(2);

    EntryGroup p1Group = writeSet.getEntryGroups().getEntryGroups(0);
    assertThat(p1Group.hasChildId()).isFalse();
    assertThat(p1Group.getEntriesList()).hasSize(2);
    Entry p1FirstEntry = p1Group.getEntries(0);
    assertThat(p1FirstEntry.getParticipantId()).isEqualTo("p1");
    assertThat(p1FirstEntry.getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(p1FirstEntry.getTableName()).isEqualTo(TABLE);
    assertThat(p1FirstEntry.getPartitionKey().getColumns(0).getTextValue().getValue())
        .isEqualTo("a");
    assertThat(p1FirstEntry.hasClusteringKey()).isTrue();
    Entry p1SecondEntry = p1Group.getEntries(1);
    assertThat(p1SecondEntry.getParticipantId()).isEqualTo("p1");
    assertThat(p1SecondEntry.getPartitionKey().getColumns(0).getTextValue().getValue())
        .isEqualTo("c");

    EntryGroup p2Group = writeSet.getEntryGroups().getEntryGroups(1);
    assertThat(p2Group.getEntriesList()).hasSize(1);
    Entry p2Entry = p2Group.getEntries(0);
    assertThat(p2Entry.getParticipantId()).isEqualTo("p2");
    assertThat(p2Entry.getPartitionKey().getColumns(0).getTextValue().getValue()).isEqualTo("b");
    assertThat(p2Entry.hasClusteringKey()).isFalse();
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
    WriteSet writeSet = WriteSetEncoder.encodeFromWriteSetEntries(writeSetsByParticipant);

    // Assert — only p1's group is emitted; the empty p2 is skipped.
    assertThat(writeSet.getEntryGroups().getEntryGroupsList()).hasSize(1);
    assertThat(writeSet.getEntryGroups().getEntryGroups(0).getEntries(0).getParticipantId())
        .isEqualTo("p1");
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
