package com.scalar.db.transaction.consensuscommit.cbrl;

import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.COL_V;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.COL_W;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.PK;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.intColumn;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Boundary/edge unit tests for the §5 replay primitive. */
class ReplayCoreTest {

  private static final int KEY = 0;
  private static final RestoredRecordReader ABSENT = k -> RecordState.absent();

  @Test
  void emptyWindow_absentStaysAbsent() {
    // Arrange
    RecordApplier applier = new RecordApplier(ABSENT);
    // Act
    RecordState result = applier.computeWriteOps(key(), ImmutableList.of());
    // Assert
    assertThat(result.present()).isFalse();
  }

  @Test
  void emptyWindow_presentStaysUnchanged() {
    // Arrange
    RecordState present = present("tX", ImmutableMap.of(COL_V, 1));
    // Act
    RecordState result = new RecordApplier(k -> present).computeWriteOps(key(), ImmutableList.of());
    // Assert
    assertThat(result).isEqualTo(present);
  }

  @Test
  void singleInsertRoot_createsRecord() {
    // Arrange
    List<RedoOperation> ops =
        ImmutableList.of(op("t0", write(null, ImmutableMap.of(COL_V, 5, COL_W, 9))));
    // Act
    RecordState result = new RecordApplier(ABSENT).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(5);
    assertThat(intOf(result, COL_W)).isEqualTo(9);
  }

  @Test
  void insertThenDelete_netZero_absent() {
    // Arrange
    List<RedoOperation> ops =
        ImmutableList.of(op("t0", write(null, ImmutableMap.of(COL_V, 1))), op("t1", delete("t0")));
    // Act
    RecordState result = new RecordApplier(ABSENT).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isFalse();
  }

  @Test
  void partialColumnMerge_acrossTwoUpdates_carriesUnsetColumn() {
    // Arrange
    List<RedoOperation> ops =
        ImmutableList.of(
            op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2))),
            op("t1", write("t0", ImmutableMap.of(COL_V, 3))), // partial: only v
            op("t2", write("t1", ImmutableMap.of(COL_V, 4)))); // partial: only v
    // Act
    RecordState result = new RecordApplier(ABSENT).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(4); // last update wins
    assertThat(intOf(result, COL_W)).isEqualTo(2); // carried from insert
  }

  @Test
  void deleteThenReinsert_reinsertIsRoot_replacesColumns() {
    // Arrange
    List<RedoOperation> ops =
        ImmutableList.of(
            op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2))),
            op("t1", delete("t0")),
            op("t2", write(null, ImmutableMap.of(COL_V, 7)))); // re-insert, root, only v
    // Act
    RecordState result = new RecordApplier(ABSENT).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(7);
    assertThat(result.columns()).doesNotContainKey(COL_W); // insert replaces, doesn't merge
  }

  @Test
  void updateChainsOffExistingRecord() {
    // Arrange
    RecordState current = present("tX", ImmutableMap.of(COL_V, 1, COL_W, 2));
    List<RedoOperation> ops = ImmutableList.of(op("t1", write("tX", ImmutableMap.of(COL_V, 9))));
    // Act
    RecordState result = new RecordApplier(k -> current).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(9);
    assertThat(intOf(result, COL_W)).isEqualTo(2);
  }

  @Test
  void windowScopedRedo_danglingRootBelowBase_tolerated() {
    // Arrange
    // Windowed repair: the copy's base is at an in-window version "v1", and the op that produced
    // it links to a pre-window (unlogged) root the backup never captured. That op is unreachable
    // from the base and must be tolerated — left unapplied — not rejected as a broken chain.
    RecordState base = present("v1", ImmutableMap.of(COL_V, 5, COL_W, 9));
    List<RedoOperation> ops =
        ImmutableList.of(
            op("v1", write("preWindowRoot", ImmutableMap.of(COL_V, 5))), // dangling: prev unlogged
            op("v2", write("v1", ImmutableMap.of(COL_V, 6)))); // chains forward off the base
    // Act
    RecordState result = new RecordApplier(k -> base).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(6); // forward update applied
    assertThat(intOf(result, COL_W)).isEqualTo(9); // carried from the copy's base
  }

  @Test
  void midChainAnchor_deleteThenReinsert_skipsStaleInsertRoot() {
    // Arrange
    // Repair anchored mid-chain on the copy's base "I0". After the delete, replay must resume from
    // the re-insert that follows it — never the original insert root "I0", which the base already
    // reflects (it is the base's own version / a chain-ancestor of it). The stale root is skipped
    // by
    // walking prev_tx_id back from the cursor. Without it the record reverts.
    RecordState base = present("I0", ImmutableMap.of(COL_V, 1));
    List<RedoOperation> ops =
        ImmutableList.of(
            op("I0", write(null, ImmutableMap.of(COL_V, 1))), // original (stale) insert root
            op("d1", delete("I0")),
            op("I2", write(null, ImmutableMap.of(COL_V, 7)))); // re-insert after the delete
    // Act
    RecordState result = new RecordApplier(k -> base).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(7); // the re-insert, not the stale root's 1
  }

  @Test
  void reorderedInput_sameResult() {
    // Arrange
    RedoOperation a = op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2)));
    RedoOperation b = op("t1", write("t0", ImmutableMap.of(COL_V, 3)));
    RedoOperation c = op("t2", write("t1", ImmutableMap.of(COL_V, 4)));
    // Act
    RecordState forward =
        new RecordApplier(ABSENT).computeWriteOps(key(), ImmutableList.of(a, b, c));
    RecordState reversed =
        new RecordApplier(ABSENT).computeWriteOps(key(), ImmutableList.of(c, b, a));
    // Assert
    assertThat(reversed).isEqualTo(forward);
  }

  @Test
  void blindDeletesWithNoPrevTxId_areTolerated() {
    // Arrange
    // Two DELETEs whose prior version had no captured tx_id (a deemed-as-committed / imported
    // record) carry no prev_tx_id. They are unreachable from the chain walk, so they are tolerated
    // (skipped, logged), leaving the record absent.
    List<RedoOperation> ops = ImmutableList.of(op("d1", delete(null)), op("d2", delete(null)));
    // Act
    RecordState result = new RecordApplier(ABSENT).computeWriteOps(key(), ops);
    // Assert
    assertThat(result.present()).isFalse();
  }

  private static Key keyProto() {
    return Key.newBuilder().addColumns(intColumn(PK, KEY)).build();
  }

  private static RecordKey key() {
    return new RecordKey(RedoLogGenerator.NAMESPACE, RedoLogGenerator.TABLE, keyProto(), null);
  }

  private static RecordState present(String txId, Map<String, Integer> cols) {
    Map<String, Column> columns = new LinkedHashMap<>();
    cols.forEach((name, value) -> columns.put(name, intColumn(name, value)));
    return RecordState.of(txId, false, columns, Collections.emptySet());
  }

  private static Entry write(String prevTxId, Map<String, Integer> cols) {
    Entry.Builder builder =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName(RedoLogGenerator.NAMESPACE)
            .setTableName(RedoLogGenerator.TABLE)
            .setPartitionKey(keyProto());
    cols.forEach((name, value) -> builder.addColumns(intColumn(name, value)));
    if (prevTxId != null) {
      builder.setPrevTxId(prevTxId);
    }
    return builder.build();
  }

  private static Entry delete(String prevTxId) {
    Entry.Builder builder =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_DELETE)
            .setNamespaceName(RedoLogGenerator.NAMESPACE)
            .setTableName(RedoLogGenerator.TABLE)
            .setPartitionKey(keyProto());
    if (prevTxId != null) {
      builder.setPrevTxId(prevTxId);
    }
    return builder.build();
  }

  private static RedoOperation op(String txId, Entry entry) {
    return new RedoOperation(txId, entry);
  }

  private static int intOf(RecordState state, String column) {
    return state.columns().get(column).getIntValue().getValue();
  }
}
