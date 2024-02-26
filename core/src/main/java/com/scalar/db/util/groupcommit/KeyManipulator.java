package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;

public interface KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> {
  class Keys<PARENT_KEY, CHILD_KEY> {
    public final PARENT_KEY parentKey;
    public final CHILD_KEY childKey;

    public Keys(PARENT_KEY parentKey, CHILD_KEY childKey) {
      this.parentKey = parentKey;
      this.childKey = childKey;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("parentKey", parentKey)
          .add("childKey", childKey)
          .toString();
    }
  }

  PARENT_KEY createParentKey();

  FULL_KEY createFullKey(PARENT_KEY parentKey, CHILD_KEY childKey);

  boolean isFullKey(Object obj);

  Keys<PARENT_KEY, CHILD_KEY> fromFullKey(FULL_KEY fullKey);

  EMIT_KEY getEmitKeyFromFullKey(FULL_KEY fullKey);

  EMIT_KEY getEmitKeyFromParentKey(PARENT_KEY parentKey);
}
