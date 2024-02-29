package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;

/**
 * A key manipulator which contains logics how to treat keys.
 *
 * @param <PARENT_KEY> A key type to NormalGroup which contains multiple slots and is
 *     group-committed.
 * @param <CHILD_KEY> A key type to slot in NormalGroup which can contain a value ready to commit.
 * @param <FULL_KEY> A key type to DelayedGroup which contains a single slot and is
 *     singly-committed.
 * @param <EMIT_KEY> A key type that Emitter can interpret.
 */
public interface KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> {
  class Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> {
    public final PARENT_KEY parentKey;
    public final CHILD_KEY childKey;
    public final FULL_KEY fullKey;

    public Keys(PARENT_KEY parentKey, CHILD_KEY childKey, FULL_KEY fullKey) {
      this.parentKey = parentKey;
      this.childKey = childKey;
      this.fullKey = fullKey;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("parentKey", parentKey)
          .add("childKey", childKey)
          .add("fullKey", fullKey)
          .toString();
    }
  }

  PARENT_KEY generateParentKey();

  FULL_KEY fullKey(PARENT_KEY parentKey, CHILD_KEY childKey);

  boolean isFullKey(Object obj);

  Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keysFromFullKey(FULL_KEY fullKey);

  EMIT_KEY emitKeyFromFullKey(FULL_KEY fullKey);

  EMIT_KEY emitKeyFromParentKey(PARENT_KEY parentKey);
}
