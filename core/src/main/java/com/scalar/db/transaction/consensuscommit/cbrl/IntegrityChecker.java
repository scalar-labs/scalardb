package com.scalar.db.transaction.consensuscommit.cbrl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fail-loud chain-connectivity check for one key's redo ops. Every non-INSERT op (it carries a
 * {@code prevTxId}) must link to either the current record's tx id or another op's tx id on the
 * same key; otherwise the chain is broken (e.g. a window read missed a mid-chain op) and replaying
 * it would silently produce wrong state. INSERT roots ({@code prevTxId == null}) are always valid —
 * the replay loop only consumes them when the record is absent or deleted.
 */
final class IntegrityChecker {

  void check(RecordKey key, List<RedoOp> ops, RecordState current) {
    Set<String> knownTxIds = new HashSet<>();
    for (RedoOp op : ops) {
      knownTxIds.add(op.txId());
    }
    if (current.currentTxId() != null) {
      knownTxIds.add(current.currentTxId());
    }
    for (RedoOp op : ops) {
      if (op.isInsert()) {
        continue;
      }
      String prevTxId = op.prevTxId();
      if (!knownTxIds.contains(prevTxId)) {
        throw new CbrlReplayException(
            "Broken redo chain for "
                + key
                + ": op tx "
                + op.txId()
                + " links to prev_tx_id "
                + prevTxId
                + " which is neither the current record's version nor any other op on this key");
      }
    }
  }
}
