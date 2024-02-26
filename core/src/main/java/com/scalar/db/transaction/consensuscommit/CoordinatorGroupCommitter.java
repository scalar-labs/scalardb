package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.util.groupcommit.GroupCommitConfig;
import com.scalar.db.util.groupcommit.GroupCommitter;
import com.scalar.db.util.groupcommit.KeyManipulator;
import java.util.UUID;

public class CoordinatorGroupCommitter
    extends GroupCommitter<String, String, String, String, Snapshot> {
  public CoordinatorGroupCommitter(GroupCommitConfig config) {
    super("coordinator", config, new CoordinatorGroupCommitKeyManipulator());
  }

  static class CoordinatorGroupCommitKeyManipulator
      implements KeyManipulator<String, String, String, String> {
    @Override
    public String createParentKey() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String createFullKey(String parentKey, String childKey) {
      // TODO: Validation
      return parentKey + ":" + childKey;
    }

    @Override
    public boolean isFullKey(Object obj) {
      if (!(obj instanceof String)) {
        return false;
      }
      String key = (String) obj;
      String[] parts = key.split(":");
      return parts.length == 2;
    }

    @Override
    public Keys<String, String> fromFullKey(String fullKey) {
      String[] parts = fullKey.split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid full key. key:" + fullKey);
      }
      return new Keys<>(parts[0], parts[1]);
    }

    @Override
    public String getEmitKeyFromFullKey(String s) {
      // Return the string as is since the value is already String.
      return s;
    }

    @Override
    public String getEmitKeyFromParentKey(String s) {
      // Return the string as is since the value is already String.
      return s;
    }
  }
}
