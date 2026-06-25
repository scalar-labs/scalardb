package com.scalar.db.transaction.consensuscommit.cbrl;

/**
 * Persists one key's reconstructed final {@link RecordState}, called by the owning pass-2 worker
 * right after that key's write ops are computed — so write-back runs per key on the worker, not in
 * a separate sequential pass. The production sink writes the record back with the Storage API (a
 * COMMITTED put if present, a delete if absent); tests collect the states to compare.
 */
@FunctionalInterface
interface RecordSink {
  void writeBack(RecordKey key, RecordState state) throws Exception;
}
