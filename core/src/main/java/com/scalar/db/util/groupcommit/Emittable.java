package com.scalar.db.util.groupcommit;

import java.util.List;
import javax.annotation.Nullable;

/**
 * An emittable interface to emit multiple values at once.
 *
 * @param <PARENT_KEY> A parent-key type that Emitter can interpret.
 * @param <FULL_KEY> A full-key type that Emitter can interpret.
 * @param <V> A value type to be set to a slot.
 * @param <R> A result type returned by an emit. The same result is handed back to every slot that
 *     belongs to the emitted group, so a client can obtain it from {@link
 *     GroupCommitter#ready(Object, Object)}. The result may be null.
 */
public interface Emittable<PARENT_KEY, FULL_KEY, V, R> {
  @Nullable
  R emitNormalGroup(PARENT_KEY parentKey, List<V> values) throws Exception;

  @Nullable
  R emitDelayedGroup(FULL_KEY fullKey, V value) throws Exception;
}
