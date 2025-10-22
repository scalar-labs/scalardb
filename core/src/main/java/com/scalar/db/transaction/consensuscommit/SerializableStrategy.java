package com.scalar.db.transaction.consensuscommit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A Serializable strategy used in Consensus Commit algorithm. Both strategies basically make
 * transactions avoid an anti-dependency that is the root cause of the anomalies in Snapshot
 * Isolation.
 *
 * @deprecated As of release 3.16.0. Will be removed in release 5.0.0
 */
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
@Deprecated
public enum SerializableStrategy implements com.scalar.db.api.SerializableStrategy {
  /**
   * Extra-write strategy converts all read set into write set when preparing records to remove
   * anti-dependencies. As a limitation, it does not support including Scan and Put in a transaction
   * because it has to convert records that were read but didn't exist at the time of scan, which is
   * not possible only with the current implementation.
   */
  EXTRA_WRITE,
  /**
   * Extra-read strategy checks all read set after preparing records and before committing a state
   * to actually find out if there is an anti-dependency.
   */
  EXTRA_READ,
}
