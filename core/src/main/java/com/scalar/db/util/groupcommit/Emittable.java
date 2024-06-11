package com.scalar.db.util.groupcommit;

import java.util.List;

/**
 * An emittable interface to emit multiple values at once.
 *
 * @param <FULL_KEY> A full-key type that Emitter can interpret.
 * @param <PARENT_KEY> A parent-key type that Emitter can interpret.
 * @param <V> A value type to be set to a slot.
 */
public interface Emittable<PARENT_KEY, FULL_KEY, V> {
  void emitNormalGroup(PARENT_KEY parentKey, List<V> values) throws Exception;

  void emitDelayedGroup(FULL_KEY fullKey, V value) throws Exception;
}
