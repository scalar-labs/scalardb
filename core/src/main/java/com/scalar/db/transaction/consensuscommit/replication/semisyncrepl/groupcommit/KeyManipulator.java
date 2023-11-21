package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public interface KeyManipulator<K> {
  class Keys<K> {
    public final K parentKey;
    public final K childKey;

    public Keys(K parentKey, K childKey) {
      this.parentKey = parentKey;
      this.childKey = childKey;
    }
  }

  K createParentKey();

  K createFullKey(K parentKey, K childKey);

  Keys<K> fromFullKey(K fullKey);
}
