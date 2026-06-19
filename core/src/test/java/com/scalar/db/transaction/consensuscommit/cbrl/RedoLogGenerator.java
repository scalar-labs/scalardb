package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Deterministic, seeded generator of legal per-key redo histories: INSERT -> UPDATE* -> DELETE ->
 * (re-INSERT as a new root) -> ... Each op gets a unique tx id, and ops are emitted in commit order
 * (list order). UPDATEs set only a subset of columns so the merge path is exercised; INSERTs set
 * all columns. The expected final state is computed separately by {@link ReferenceApplier}, never
 * here.
 */
final class RedoLogGenerator {
  static final String NAMESPACE = "ns";
  static final String TABLE = "t";
  static final String PK = "pk";
  static final String COL_V = "v"; // set by every write
  static final String COL_W = "w"; // set only by inserts -> survives across updates (merge test)

  private final Random random;
  private int txCounter;
  private int valueCounter;

  RedoLogGenerator(long seed) {
    this.random = new Random(seed);
  }

  /** Generates a flat list of ops across {@code numKeys} keys, in commit order. */
  List<RedoOp> generate(int numKeys) {
    List<RedoOp> ops = new ArrayList<>();
    for (int k = 0; k < numKeys; k++) {
      generateKeyHistory(k, ops);
    }
    return ops;
  }

  private void generateKeyHistory(int keyIndex, List<RedoOp> ops) {
    boolean present = false;
    String lastTxId = null;
    int events = 1 + random.nextInt(7);
    for (int e = 0; e < events; e++) {
      String txId = "tx-" + txCounter++;
      if (!present) {
        ops.add(insert(keyIndex, txId));
        present = true;
      } else if (random.nextInt(100) < 30) {
        ops.add(delete(keyIndex, txId, lastTxId));
        present = false;
      } else {
        ops.add(update(keyIndex, txId, lastTxId));
      }
      lastTxId = txId;
    }
  }

  private RedoOp insert(int keyIndex, String txId) {
    Entry entry =
        baseEntry(keyIndex, Entry.EntryType.ENTRY_TYPE_WRITE)
            .addColumns(intColumn(COL_V, valueCounter++))
            .addColumns(intColumn(COL_W, valueCounter++))
            .build();
    return new RedoOp(txId, entry);
  }

  private RedoOp update(int keyIndex, String txId, String prevTxId) {
    Entry entry =
        baseEntry(keyIndex, Entry.EntryType.ENTRY_TYPE_WRITE)
            .setPrevTxId(prevTxId)
            .addColumns(intColumn(COL_V, valueCounter++)) // partial: only COL_V
            .build();
    return new RedoOp(txId, entry);
  }

  private RedoOp delete(int keyIndex, String txId, String prevTxId) {
    Entry entry =
        baseEntry(keyIndex, Entry.EntryType.ENTRY_TYPE_DELETE).setPrevTxId(prevTxId).build();
    return new RedoOp(txId, entry);
  }

  private static Entry.Builder baseEntry(int keyIndex, Entry.EntryType type) {
    return Entry.newBuilder()
        .setEntryType(type)
        .setNamespaceName(NAMESPACE)
        .setTableName(TABLE)
        .setPartitionKey(Key.newBuilder().addColumns(intColumn(PK, keyIndex)));
  }

  static Column intColumn(String name, int value) {
    return Column.newBuilder()
        .setName(name)
        .setIntValue(IntValue.newBuilder().setValue(value))
        .build();
  }

  static RecordKey recordKey(int keyIndex) {
    return new RecordKey(
        NAMESPACE, TABLE, Key.newBuilder().addColumns(intColumn(PK, keyIndex)).build(), null);
  }
}
