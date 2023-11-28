package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;

public interface KeyManipulator<K> {
  class Keys<K> {
    public final K parentKey;
    public final K childKey;

    public Keys(K parentKey, K childKey) {
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

  K createParentKey();

  K createFullKey(K parentKey, K childKey);

  boolean isFullKey(K fullKey);

  Keys<K> fromFullKey(K fullKey);
}
