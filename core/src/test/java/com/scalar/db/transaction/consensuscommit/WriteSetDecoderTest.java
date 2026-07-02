package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WriteSetDecoderTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String TX_ID = "tx-id";

  private TransactionTableMetadataManager tableMetadataManager;
  private ParallelExecutor parallelExecutor;
  private WriteSetEncoder writeSetEncoder;

  @BeforeEach
  void setUp() throws Exception {
    tableMetadataManager = mock(TransactionTableMetadataManager.class);
    TableMetadata defaultMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.TEXT)
            .addColumn("ck", DataType.INT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .build();
    TransactionTableMetadata transactionTableMetadata =
        new TransactionTableMetadata(
            ConsensusCommitUtils.buildTransactionTableMetadata(defaultMetadata, false));
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

  private Entry encodeSinglePut(DataType pkType, Key partitionKey) throws Exception {
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
    EntryGroup group = writeSetEncoder.encodeEntryGroup(snapshot, null, null);
    return group.getEntries(0);
  }

  private void assertPartitionKeyRoundTrip(DataType pkType, Key originalKey) throws Exception {
    Entry entry = encodeSinglePut(pkType, originalKey);
    Get get = WriteSetDecoder.toGet(entry);
    // Cast to Object to avoid the ambiguous `assertThat` overloads between Comparable and Iterable
    // — Key implements both, so the compiler cannot pick a winner.
    assertThat((Object) get.getPartitionKey()).isEqualTo(originalKey);
  }

  @Test
  void toGet_BooleanPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.BOOLEAN, Key.ofBoolean("pk", true));
  }

  @Test
  void toGet_IntPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.INT, Key.ofInt("pk", 42));
  }

  @Test
  void toGet_BigIntPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.BIGINT, Key.ofBigInt("pk", 12345678901234L));
  }

  @Test
  void toGet_FloatPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.FLOAT, Key.ofFloat("pk", 1.25f));
  }

  @Test
  void toGet_DoublePartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.DOUBLE, Key.ofDouble("pk", 12.345));
  }

  @Test
  void toGet_TextPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.TEXT, Key.ofText("pk", "p1"));
  }

  @Test
  void toGet_BlobPartitionKey_ShouldRoundTripIntoByteBufferBackedKey() throws Exception {
    byte[] blobBytes = new byte[] {1, 2, 3, 4};
    assertPartitionKeyRoundTrip(DataType.BLOB, Key.ofBlob("pk", blobBytes));
  }

  @Test
  void toGet_DatePartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.DATE, Key.ofDate("pk", LocalDate.of(2026, 5, 10)));
  }

  @Test
  void toGet_TimePartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(DataType.TIME, Key.ofTime("pk", LocalTime.of(12, 34, 56)));
  }

  @Test
  void toGet_TimestampPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(
        DataType.TIMESTAMP, Key.ofTimestamp("pk", LocalDateTime.of(2026, 5, 10, 12, 34, 56)));
  }

  @Test
  void toGet_TimestampTZPartitionKey_ShouldRoundTrip() throws Exception {
    assertPartitionKeyRoundTrip(
        DataType.TIMESTAMPTZ, Key.ofTimestampTZ("pk", Instant.ofEpochSecond(1747000000L)));
  }

  @Test
  void toGet_CompositePartitionAndClusteringKey_ShouldRoundTrip() throws Exception {
    // Arrange
    TableMetadata metadata =
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
                ConsensusCommitUtils.buildTransactionTableMetadata(metadata, false)));

    Key partitionKey = Key.newBuilder().addText("pk1", "p").addInt("pk2", 7).build();
    Key clusteringKey = Key.newBuilder().addBigInt("ck1", 100L).addText("ck2", "c").build();
    Snapshot snapshot = newSnapshot();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    Entry entry = writeSetEncoder.encodeEntryGroup(snapshot, null, null).getEntries(0);

    // Act
    Get get = WriteSetDecoder.toGet(entry);

    // Assert
    assertThat((Object) get.getPartitionKey()).isEqualTo(partitionKey);
    assertThat(get.getClusteringKey()).hasValue(clusteringKey);
  }

  @Test
  void toGet_EntryWithPartitionAndClusteringKey_ShouldBuildGetWithBothKeysAndLinearizable()
      throws Exception {
    // Arrange
    Snapshot snapshot = newSnapshot();
    Key partitionKey = Key.ofText("pk", "p1");
    Key clusteringKey = Key.ofInt("ck", 10);
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    Entry entry = writeSetEncoder.encodeEntryGroup(snapshot, null, null).getEntries(0);

    // Act
    Get get = WriteSetDecoder.toGet(entry);

    // Assert
    assertThat(get.forNamespace()).hasValue(NAMESPACE);
    assertThat(get.forTable()).hasValue(TABLE);
    assertThat((Object) get.getPartitionKey()).isEqualTo(partitionKey);
    assertThat(get.getClusteringKey()).hasValue(clusteringKey);
    assertThat(get.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
  }

  @Test
  void toGet_EntryWithoutClusteringKey_ShouldBuildGetWithoutClusteringKey() throws Exception {
    // Arrange
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

    Snapshot snapshot = newSnapshot();
    Key partitionKey = Key.ofText("pk", "p1");
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(partitionKey)
            .textValue("v", "val")
            .build();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    Entry entry = writeSetEncoder.encodeEntryGroup(snapshot, null, null).getEntries(0);

    // Act
    Get get = WriteSetDecoder.toGet(entry);

    // Assert
    assertThat((Object) get.getPartitionKey()).isEqualTo(partitionKey);
    assertThat(get.getClusteringKey()).isEmpty();
    assertThat(get.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
  }

  @Test
  void toGet_NullMarkerPrimaryKeyColumn_ShouldThrowAssertionError() {
    // Arrange — a proto Entry whose partition key Column has TextValue with no `value` set (null
    // marker). This shape should never occur for a primary-key column in production because
    // ScalarDB primary keys are non-nullable.
    Entry entry =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName(NAMESPACE)
            .setTableName(TABLE)
            .setPartitionKey(
                com.scalar.db.transaction.consensuscommit.proto.v1.Key.newBuilder()
                    .addColumns(
                        Column.newBuilder()
                            .setName("pk")
                            .setTextValue(Column.TextValue.getDefaultInstance())))
            .build();

    // Act + Assert
    assertThatThrownBy(() -> WriteSetDecoder.toGet(entry)).isInstanceOf(AssertionError.class);
  }

  @Test
  void toGet_ColumnWithoutValueOneofSet_ShouldThrowAssertionError() {
    // Arrange — a proto Entry whose partition key Column has no value oneof set (VALUE_NOT_SET).
    Entry entry =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName(NAMESPACE)
            .setTableName(TABLE)
            .setPartitionKey(
                com.scalar.db.transaction.consensuscommit.proto.v1.Key.newBuilder()
                    .addColumns(Column.newBuilder().setName("pk")))
            .build();

    // Act + Assert
    assertThatThrownBy(() -> WriteSetDecoder.toGet(entry)).isInstanceOf(AssertionError.class);
  }
}
