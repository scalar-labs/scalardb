package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.Optional;
import java.util.UUID;

public class ConsensusCommitManagerWithGroupCommitTest extends ConsensusCommitManagerTestBase {
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private String parentKey;
  private String childKey;
  private CoordinatorGroupCommitter groupCommitter;

  @Override
  void extraInitialize() {
    // `groupCommitter` is instantiated separately since the timing of the instantiation and calling
    // GroupCommitter.reserve() would be different.
    childKey = UUID.randomUUID().toString();
    String fullKey = groupCommitter.reserve(childKey);
    parentKey = keyManipulator.keysFromFullKey(fullKey).parentKey;
  }

  @Override
  Optional<CoordinatorGroupCommitter> groupCommitter() {
    if (groupCommitter == null) {
      groupCommitter = new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 10));
    }
    return Optional.of(groupCommitter);
  }

  @Override
  String anyTxIdGivenByClient() {
    return childKey;
  }

  @Override
  String anyTxIdAlreadyStarted() {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  @Override
  boolean isGroupCommitEnabled() {
    return true;
  }

  @Override
  void extraCleanup() {
    if (groupCommitter != null) {
      groupCommitter.close();
    }
  }
}
