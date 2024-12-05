package com.scalar.db.util.groupcommit;

import com.google.common.base.Splitter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class TestableGroupCommitKeyManipulator
    implements GroupCommitKeyManipulator<String, String, String, String, String> {
  private final AtomicInteger counter = new AtomicInteger();

  @Override
  public String generateParentKey() {
    return String.format("%04d", counter.getAndIncrement());
  }

  @Override
  public String fullKey(String parentKey, String childKey) {
    return parentKey + ":" + childKey;
  }

  @Override
  public boolean isFullKey(Object obj) {
    if (!(obj instanceof String)) {
      return false;
    }
    String key = (String) obj;
    return key.contains(":");
  }

  @Override
  public Keys<String, String, String> keysFromFullKey(String fullKey) {
    List<String> parts = Splitter.on(':').splitToList(fullKey);
    return new Keys<>(parts.get(0), parts.get(1), fullKey);
  }

  @Override
  public String emitFullKeyFromFullKey(String s) {
    return s;
  }

  @Override
  public String emitParentKeyFromParentKey(String s) {
    return s;
  }
}
