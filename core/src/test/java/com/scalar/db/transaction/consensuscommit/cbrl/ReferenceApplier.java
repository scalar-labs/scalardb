package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The oracle: computes the expected final state per key by applying ops in commit order ({@code
 * createdAtMillis}), sequentially, with no chain machinery, bucketing, or insert queue.
 * Deliberately independent of the replay primitive (plain maps, not {@link RecordState.Builder}) so
 * a bug shared with the applier can't hide. Used to check that the cursor-driven replay reaches the
 * same state regardless of input order.
 */
final class ReferenceApplier {

  Map<RecordKey, RecordState> finalStates(List<RedoOp> ops) {
    Map<RecordKey, List<RedoOp>> byKey = new LinkedHashMap<>();
    for (RedoOp op : ops) {
      byKey.computeIfAbsent(op.key(), k -> new java.util.ArrayList<>()).add(op);
    }
    Map<RecordKey, RecordState> result = new LinkedHashMap<>();
    for (Map.Entry<RecordKey, List<RedoOp>> entry : byKey.entrySet()) {
      result.put(entry.getKey(), apply(entry.getValue()));
    }
    return result;
  }

  private RecordState apply(List<RedoOp> keyOps) {
    keyOps.sort(Comparator.comparingLong(RedoOp::createdAtMillis));
    Map<String, Column> columns = new TreeMap<>();
    boolean present = false;
    String lastTxId = null;
    for (RedoOp op : keyOps) {
      if (op.isInsert()) {
        columns.clear();
        putAll(columns, op.entry().getColumnsList());
        present = true;
      } else if (op.isUpdate()) {
        putAll(columns, op.entry().getColumnsList());
        present = true;
      } else {
        columns.clear();
        present = false;
      }
      lastTxId = op.txId();
    }
    return present
        ? RecordState.of(lastTxId, false, new LinkedHashMap<>(columns), Collections.emptySet())
        : RecordState.absent();
  }

  private static void putAll(Map<String, Column> columns, List<Column> updates) {
    for (Column column : updates) {
      columns.put(column.getName(), column);
    }
  }
}
