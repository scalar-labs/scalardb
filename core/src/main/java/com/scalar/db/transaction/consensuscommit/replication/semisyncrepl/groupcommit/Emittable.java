package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import java.util.List;

@FunctionalInterface
public interface Emittable<K, V> {
  // FIXME This should receive K key so that CommitHandler can abort the transactions when no
  // snapshots
  void execute(K parentKey, List<V> values);
}
