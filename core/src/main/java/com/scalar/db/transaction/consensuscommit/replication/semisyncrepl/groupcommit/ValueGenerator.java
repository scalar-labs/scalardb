package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

@FunctionalInterface
public interface ValueGenerator<K, V> {
  V execute(K key);
}
