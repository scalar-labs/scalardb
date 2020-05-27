package com.scalar.db.transaction.consensuscommit;

/**
 * A Serializable strategy used in Consensus Commit algorithm. Both strategies basically make
 * transactions avoid an anti-dependency that is the root cause of the anomalies in Snapshot
 * Isolation.
 *
 * <p>Extra-write strategy converts all read set into write set when preparing records to remove
 * anti-dependencies.
 *
 * <p>Extra-read strategy checks all read set after preparing records and before committing a state
 * to actually find out if there is an anti-dependency.
 */
public enum SerializableStrategy implements com.scalar.db.api.SerializableStrategy {
  EXTRA_WRITE,
  EXTRA_READ,
}
