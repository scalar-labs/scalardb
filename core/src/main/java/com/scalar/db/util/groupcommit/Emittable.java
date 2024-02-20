package com.scalar.db.util.groupcommit;

import java.util.List;

@FunctionalInterface
public interface Emittable<K, V> {
  void execute(K parentKey, List<V> values);
}
