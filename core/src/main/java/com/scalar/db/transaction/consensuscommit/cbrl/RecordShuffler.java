package com.scalar.db.transaction.consensuscommit.cbrl;

import java.util.ArrayList;
import java.util.List;

/**
 * Pass 1: distributes redo ops into {@code N} buckets by {@code floorMod(hash(recordKey), N)}. All
 * ops sharing a {@link RecordKey} land in exactly one bucket, so pass 2 can own whole buckets with
 * no intra-key concurrency (no CAS, no locks). Append order within a bucket is irrelevant — the
 * replay primitive is cursor-driven and order-independent within a key.
 */
final class RecordShuffler {

  static int bucketOf(RecordKey key, int bucketCount) {
    return Math.floorMod(key.hashCode(), bucketCount);
  }

  List<List<RedoOp>> shuffle(Iterable<RedoOp> ops, int bucketCount) {
    if (bucketCount < 1) {
      throw new IllegalArgumentException("bucketCount must be >= 1, was " + bucketCount);
    }
    List<List<RedoOp>> buckets = new ArrayList<>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      buckets.add(new ArrayList<>());
    }
    for (RedoOp op : ops) {
      buckets.get(bucketOf(op.key(), bucketCount)).add(op);
    }
    return buckets;
  }
}
