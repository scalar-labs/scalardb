package com.scalar.db.util.groupcommit;

import java.util.List;

@FunctionalInterface
public interface Emittable<EMIT_KEY, V> {
  void execute(EMIT_KEY key, List<V> values) throws Exception;
}
