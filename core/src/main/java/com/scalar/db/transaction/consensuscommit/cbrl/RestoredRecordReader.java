package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Reads a key's current {@link RecordState} in the database being restored (the loaded primary
 * backup image) — the replay cursor's origin and merge target. "Restored" is load-bearing: this
 * reads the user-table record in the DB being restored, not the coordinator rows the redo stream
 * comes from, and not ConsensusCommit's own {@code Snapshot}.
 *
 * <p>In production this read happens after PREPARED records are resolved via the cut and {@code
 * before_*} images (a C4 seam). The PoC supplies states directly.
 */
public interface RestoredRecordReader {
  RecordState get(RecordKey key);
}
