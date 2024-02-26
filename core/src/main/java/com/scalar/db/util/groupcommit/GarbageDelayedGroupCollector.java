package com.scalar.db.util.groupcommit;

@FunctionalInterface
interface GarbageDelayedGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  void collect(DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group);
}
