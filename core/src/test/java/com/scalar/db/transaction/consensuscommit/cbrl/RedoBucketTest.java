package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Round-trip and lifecycle tests for the temp-file-backed {@link RedoBucket} spill. */
class RedoBucketTest {

  private static Entry write(int keyIndex, String prevTxId, int value) {
    Entry.Builder builder =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName(RedoLogGenerator.NAMESPACE)
            .setTableName(RedoLogGenerator.TABLE)
            .setPartitionKey(
                Key.newBuilder()
                    .addColumns(RedoLogGenerator.intColumn(RedoLogGenerator.PK, keyIndex)))
            .addColumns(RedoLogGenerator.intColumn(RedoLogGenerator.COL_V, value));
    if (prevTxId != null) {
      builder.setPrevTxId(prevTxId);
    }
    return builder.build();
  }

  private static Entry delete(int keyIndex) {
    return Entry.newBuilder()
        .setEntryType(Entry.EntryType.ENTRY_TYPE_DELETE)
        .setNamespaceName(RedoLogGenerator.NAMESPACE)
        .setTableName(RedoLogGenerator.TABLE)
        .setPartitionKey(
            Key.newBuilder().addColumns(RedoLogGenerator.intColumn(RedoLogGenerator.PK, keyIndex)))
        .build();
  }

  @Test
  void addSealRead_roundTripsEveryOp() {
    // Arrange
    RedoBucket bucket = new RedoBucket();
    RedoOperation insert = new RedoOperation("t0", write(1, null, 10), 100L);
    RedoOperation update = new RedoOperation("t1", write(1, "t0", 20), 200L);
    RedoOperation del = new RedoOperation("t2", delete(1), 300L);
    bucket.add(insert);
    bucket.add(update);
    bucket.add(del);
    bucket.seal();

    // Act
    List<RedoOperation> read = bucket.read();

    // Assert
    assertThat(read).hasSize(3);
    assertRoundTrips(read.get(0), insert);
    assertRoundTrips(read.get(1), update);
    assertRoundTrips(read.get(2), del);
    bucket.delete();
  }

  @Test
  void read_isRepeatable() {
    // Arrange
    RedoBucket bucket = new RedoBucket();
    RedoOperation op = new RedoOperation("t0", write(1, null, 10), 100L);
    bucket.add(op);
    bucket.seal();

    // Act
    List<RedoOperation> first = bucket.read();
    List<RedoOperation> second = bucket.read();

    // Assert
    assertThat(first).hasSize(1);
    assertThat(second).hasSize(1);
    assertRoundTrips(second.get(0), op);
    bucket.delete();
  }

  @Test
  void emptyBucket_sealsAndReadsEmpty() {
    // Arrange
    RedoBucket bucket = new RedoBucket();
    bucket.seal();

    // Act
    List<RedoOperation> read = bucket.read();

    // Assert
    assertThat(read).isEmpty();
    bucket.delete();
  }

  @Test
  void read_beforeSeal_throws() {
    // Arrange
    RedoBucket bucket = new RedoBucket();
    bucket.add(new RedoOperation("t0", write(1, null, 10), 100L));

    // Act & Assert
    assertThatThrownBy(bucket::read).isInstanceOf(IllegalStateException.class);
    bucket.delete();
  }

  @Test
  void add_afterSeal_throws() {
    // Arrange
    RedoBucket bucket = new RedoBucket();
    bucket.seal();

    // Act & Assert
    assertThatThrownBy(() -> bucket.add(new RedoOperation("t0", write(1, null, 10), 100L)))
        .isInstanceOf(IllegalStateException.class);
    bucket.delete();
  }

  private static void assertRoundTrips(RedoOperation actual, RedoOperation expected) {
    assertThat(actual.txId()).isEqualTo(expected.txId());
    assertThat(actual.committedAt()).isEqualTo(expected.committedAt());
    assertThat(actual.prevTxId()).isEqualTo(expected.prevTxId());
    assertThat(actual.key()).isEqualTo(expected.key());
    assertThat(actual.isDelete()).isEqualTo(expected.isDelete());
    assertThat(actual.entry()).isEqualTo(expected.entry());
  }
}
