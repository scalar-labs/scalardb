package com.scalar.db.util.groupcommit;

@FunctionalInterface
interface GarbageGroupCollector<K, V> {
  void collect(Group<K, V> group);
}
