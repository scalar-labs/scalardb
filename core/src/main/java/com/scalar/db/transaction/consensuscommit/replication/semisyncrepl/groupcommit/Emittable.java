package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import java.util.List;

@FunctionalInterface
public interface Emittable<V> {
  // FIXME This should receive K key so that CommitHandler can abort the transactions when no
  // snapshots
  void execute(List<V> values);
}
