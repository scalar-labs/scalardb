package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.util.List;
import javax.annotation.Nullable;

/**
 * One captured coordinator-state row — the unit of the coordinator backup that {@link CbrlRestore}
 * consumes. {@code childIds} is non-empty only for a normal group-commit parent row; {@code
 * writeSet} is null for a pre-feature / lazy-recovery-abort row.
 */
final class CoordinatorBackupRow {
  final String txId;
  final int state;
  final List<String> childIds;
  @Nullable final WriteSet writeSet;

  CoordinatorBackupRow(String txId, int state, List<String> childIds, @Nullable WriteSet writeSet) {
    this.txId = txId;
    this.state = state;
    this.childIds = childIds;
    this.writeSet = writeSet;
  }
}
