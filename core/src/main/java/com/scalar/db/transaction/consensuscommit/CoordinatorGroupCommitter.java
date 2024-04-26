package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.util.groupcommit.GroupCommitConfig;
import com.scalar.db.util.groupcommit.GroupCommitter;
import com.scalar.db.util.groupcommit.KeyManipulator;
import java.util.Optional;
import java.util.Random;

public class CoordinatorGroupCommitter
    extends GroupCommitter<String, String, String, String, Snapshot> {
  CoordinatorGroupCommitter(GroupCommitConfig config) {
    super("coordinator", config, new CoordinatorGroupCommitKeyManipulator());
  }

  public CoordinatorGroupCommitter(ConsensusCommitConfig config) {
    this(
        new GroupCommitConfig(
            config.getCoordinatorGroupCommitSlotCapacity(),
            config.getCoordinatorGroupCommitGroupSizeFixTimeoutMillis(),
            config.getCoordinatorGroupCommitDelayedSlotMoveTimeoutMillis(),
            config.getCoordinatorGroupCommitOldGroupAbortTimeoutSeconds(),
            config.getCoordinatorGroupCommitTimeoutCheckIntervalMillis(),
            config.isCoordinatorGroupCommitMetricsConsoleReporterEnabled()));
  }

  public static Optional<CoordinatorGroupCommitter> from(ConsensusCommitConfig config) {
    if (config.isCoordinatorGroupCommitEnabled()) {
      return Optional.of(new CoordinatorGroupCommitter(config));
    } else {
      return Optional.empty();
    }
  }

  static class CoordinatorGroupCommitKeyManipulator
      implements KeyManipulator<String, String, String, String> {
    private static final int PRIMARY_KEY_SIZE = 24;
    private static final char DELIMITER = ':';
    private static final int MAX_FULL_KEY_SIZE = 64;
    private static final int MAX_CHILD_KEY_SIZE =
        MAX_FULL_KEY_SIZE - PRIMARY_KEY_SIZE - 1 /* delimiter */;
    private static final char[] CHARS_FOR_PRIMARY_KEY;
    private static final int CHARS_FOR_PRIMARY_KEY_SIZE;

    static {
      int digitsLen = '9' - '0' + 1;
      int upperCasesLen = 'Z' - 'A' + 1;
      int lowerCasesLen = 'z' - 'a' + 1;
      CHARS_FOR_PRIMARY_KEY = new char[digitsLen + upperCasesLen + lowerCasesLen];

      int index = 0;
      for (char c = '0'; c <= '9'; c++) {
        CHARS_FOR_PRIMARY_KEY[index++] = c;
      }
      for (char c = 'A'; c <= 'Z'; c++) {
        CHARS_FOR_PRIMARY_KEY[index++] = c;
      }
      for (char c = 'a'; c <= 'z'; c++) {
        CHARS_FOR_PRIMARY_KEY[index++] = c;
      }

      CHARS_FOR_PRIMARY_KEY_SIZE = CHARS_FOR_PRIMARY_KEY.length;
    }

    // Use Random instead of ThreadLocalRandom in favor of global randomness.
    private final Random random = new Random();

    @Override
    public String generateParentKey() {
      char[] chars = new char[PRIMARY_KEY_SIZE];
      for (int i = 0; i < PRIMARY_KEY_SIZE; i++) {
        chars[i] = CHARS_FOR_PRIMARY_KEY[random.nextInt(CHARS_FOR_PRIMARY_KEY_SIZE)];
      }
      return new String(chars);
    }

    @Override
    public String fullKey(String parentKey, String childKey) {
      if (parentKey.length() != PRIMARY_KEY_SIZE) {
        throw new IllegalArgumentException(
            String.format(
                "The length of parent key must be %d. ParentKey: %s", PRIMARY_KEY_SIZE, childKey));
      }
      if (childKey.length() > MAX_CHILD_KEY_SIZE) {
        throw new IllegalArgumentException(
            String.format(
                "The length of child key must not exceed %d. ChildKey: %s",
                MAX_CHILD_KEY_SIZE, childKey));
      }
      return parentKey + DELIMITER + childKey;
    }

    @Override
    public boolean isFullKey(Object obj) {
      if (!(obj instanceof String)) {
        return false;
      }
      String key = (String) obj;
      return key.length() > PRIMARY_KEY_SIZE && key.charAt(PRIMARY_KEY_SIZE) == DELIMITER;
    }

    @Override
    public Keys<String, String, String> keysFromFullKey(String fullKey) {
      if (!isFullKey(fullKey)) {
        throw new IllegalArgumentException("Invalid full key. Key:" + fullKey);
      }

      return new Keys<>(
          fullKey.substring(0, PRIMARY_KEY_SIZE),
          fullKey.substring(PRIMARY_KEY_SIZE + 1 /* delimiter */),
          fullKey);
    }

    @Override
    public String emitKeyFromFullKey(String s) {
      // Return the string as is since the value is already String.
      return s;
    }

    @Override
    public String emitKeyFromParentKey(String s) {
      // Return the string as is since the value is already String.
      return s;
    }
  }
}
