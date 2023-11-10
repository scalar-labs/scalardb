package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import java.util.List;

@FunctionalInterface
public interface Emittable<V> {
  void execute(List<V> values);
}
