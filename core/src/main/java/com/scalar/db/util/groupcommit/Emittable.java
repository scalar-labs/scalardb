package com.scalar.db.util.groupcommit;

import java.util.List;

/**
 * An emittable interface which is capable of emitting multiple values at once.
 *
 * @param <EMIT_KEY> A key type that Emitter can interpret.
 * @param <V> A value type to be set to a slot.
 */
@FunctionalInterface
public interface Emittable<EMIT_KEY, V> {
  void execute(EMIT_KEY key, List<V> values) throws Exception;
}
