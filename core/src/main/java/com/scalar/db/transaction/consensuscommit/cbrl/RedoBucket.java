package com.scalar.db.transaction.consensuscommit.cbrl;

import java.util.ArrayList;
import java.util.List;

/**
 * A single pass-1 bucket: the redo ops for a disjoint subset of {@link RecordKey}s. All ops sharing
 * a key land in exactly one bucket (see {@link RecordShuffler}), so a pass-2 worker owns whole
 * buckets and there is no intra-key concurrency — no CAS, no locks (the design's core
 * simplification over SSR). Append order within a bucket is irrelevant: the replay primitive is
 * cursor-driven and order-independent within a key.
 */
final class RedoBucket {
  private final List<RedoOperation> operations = new ArrayList<>();

  void add(RedoOperation operation) {
    operations.add(operation);
  }

  List<RedoOperation> operations() {
    return operations;
  }
}
