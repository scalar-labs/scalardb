package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Reads a key's current {@link RecordState} in the database being restored (the loaded primary
 * backup image) — the replay cursor's origin and merge target. "Restored" is load-bearing: this
 * reads the user-table record in the DB being restored, not the coordinator rows the redo stream
 * comes from, and not ConsensusCommit's own {@code Snapshot}.
 *
 * <p>In production this read happens after C4: PREPARED-record recovery — records the copy caught
 * in the PREPARED state are resolved via the consistency point and {@code before_*} images before
 * replay anchors on them. The PoC supplies states directly.
 */
interface RestoredRecordReader {
  RecordState get(RecordKey key);
}
