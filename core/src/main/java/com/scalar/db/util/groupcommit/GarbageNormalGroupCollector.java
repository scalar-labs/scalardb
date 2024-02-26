package com.scalar.db.util.groupcommit;

@FunctionalInterface
interface GarbageNormalGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  void collect(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group);
}
