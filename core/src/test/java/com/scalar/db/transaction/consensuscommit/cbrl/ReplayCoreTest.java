package com.scalar.db.transaction.consensuscommit.cbrl;

import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.COL_V;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.COL_W;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.PK;
import static com.scalar.db.transaction.consensuscommit.cbrl.RedoLogGenerator.intColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    RecordState result = new RecordApplier(ABSENT).replayKey(key(), ImmutableList.of());
    assertThat(result.present()).isFalse();
  }

  @Test
  void emptyWindow_presentStaysUnchanged() {
    RecordState present = present("tX", ImmutableMap.of(COL_V, 1));
    RecordState result = new RecordApplier(k -> present).replayKey(key(), ImmutableList.of());
    assertThat(result).isEqualTo(present);
  }

  @Test
  void singleInsertRoot_createsRecord() {
    List<RedoOperation> ops =
        ImmutableList.of(op("t0", write(null, ImmutableMap.of(COL_V, 5, COL_W, 9))));
    RecordState result = new RecordApplier(ABSENT).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(5);
    assertThat(intOf(result, COL_W)).isEqualTo(9);
  }

  @Test
  void insertThenDelete_netZero_absent() {
    List<RedoOperation> ops =
        ImmutableList.of(op("t0", write(null, ImmutableMap.of(COL_V, 1))), op("t1", delete("t0")));
    RecordState result = new RecordApplier(ABSENT).replayKey(key(), ops);
    assertThat(result.present()).isFalse();
  }

  @Test
  void partialColumnMerge_acrossTwoUpdates_carriesUnsetColumn() {
    List<RedoOperation> ops =
        ImmutableList.of(
            op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2))),
            op("t1", write("t0", ImmutableMap.of(COL_V, 3))), // partial: only v
            op("t2", write("t1", ImmutableMap.of(COL_V, 4)))); // partial: only v
    RecordState result = new RecordApplier(ABSENT).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(4); // last update wins
    assertThat(intOf(result, COL_W)).isEqualTo(2); // carried from insert
  }

  @Test
  void deleteThenReinsert_reinsertIsRoot_replacesColumns() {
    List<RedoOperation> ops =
        ImmutableList.of(
            op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2))),
            op("t1", delete("t0")),
            op("t2", write(null, ImmutableMap.of(COL_V, 7)))); // re-insert, root, only v
    RecordState result = new RecordApplier(ABSENT).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(7);
    assertThat(result.columns()).doesNotContainKey(COL_W); // insert replaces, doesn't merge
  }

  @Test
  void updateChainsOffExistingRecord() {
    RecordState current = present("tX", ImmutableMap.of(COL_V, 1, COL_W, 2));
    List<RedoOperation> ops = ImmutableList.of(op("t1", write("tX", ImmutableMap.of(COL_V, 9))));
    RecordState result = new RecordApplier(k -> current).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(9);
    assertThat(intOf(result, COL_W)).isEqualTo(2);
  }

  @Test
  void windowScopedRedo_danglingRootBelowBase_tolerated() {
    // Windowed repair: the copy's base is at an in-window version "v1", and the op that produced
    // it links to a pre-window (unlogged) root the backup never captured. That op is unreachable
    // from the base and must be tolerated — left unapplied — not rejected as a broken chain.
    RecordState base = present("v1", ImmutableMap.of(COL_V, 5, COL_W, 9));
    List<RedoOperation> ops =
        ImmutableList.of(
            op("v1", write("preWindowRoot", ImmutableMap.of(COL_V, 5))), // dangling: prev unlogged
            op("v2", write("v1", ImmutableMap.of(COL_V, 6)))); // chains forward off the base
    RecordState result = new RecordApplier(k -> base).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(6); // forward update applied
    assertThat(intOf(result, COL_W)).isEqualTo(9); // carried from the copy's base
  }

  @Test
  void midChainAnchor_deleteThenReinsert_skipsStaleInsertRoot() {
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
    RecordState result = new RecordApplier(k -> base).replayKey(key(), ops);
    assertThat(result.present()).isTrue();
    assertThat(intOf(result, COL_V)).isEqualTo(7); // the re-insert, not the stale root's 1
  }

  @Test
  void reorderedInput_sameResult() {
    RedoOperation a = op("t0", write(null, ImmutableMap.of(COL_V, 1, COL_W, 2)));
    RedoOperation b = op("t1", write("t0", ImmutableMap.of(COL_V, 3)));
    RedoOperation c = op("t2", write("t1", ImmutableMap.of(COL_V, 4)));
    RecordState forward = new RecordApplier(ABSENT).replayKey(key(), ImmutableList.of(a, b, c));
    RecordState reversed = new RecordApplier(ABSENT).replayKey(key(), ImmutableList.of(c, b, a));
    assertThat(reversed).isEqualTo(forward);
  }

  @Test
  void fork_twoOpsShareAPrevTxId_rejected() {
    // A fork — two ops superseding the same prior version — cannot arise from serializable commit
    // (the prepare CAS linearizes each record's history). This is a defensive guard against a
    // corrupt backup or an encoder bug: replay must reject it loudly, not silently pick a branch.
    List<RedoOperation> ops =
        ImmutableList.of(
            op("t0", write(null, ImmutableMap.of(COL_V, 1))),
            op("t1", write("t0", ImmutableMap.of(COL_V, 2))),
            op("t2", write("t0", ImmutableMap.of(COL_V, 3)))); // also chains off t0 -> fork
    assertThatThrownBy(() -> new RecordApplier(ABSENT).replayKey(key(), ops))
        .isInstanceOf(CbrlReplayException.class)
        .hasMessageContaining("share prev_tx_id");
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
    return Entry.newBuilder()
        .setEntryType(Entry.EntryType.ENTRY_TYPE_DELETE)
        .setNamespaceName(RedoLogGenerator.NAMESPACE)
        .setTableName(RedoLogGenerator.TABLE)
        .setPartitionKey(keyProto())
        .setPrevTxId(prevTxId)
        .build();
  }

  private static RedoOperation op(String txId, Entry entry) {
    return new RedoOperation(txId, entry);
  }

  private static int intOf(RecordState state, String column) {
    return state.columns().get(column).getIntValue().getValue();
  }
}
