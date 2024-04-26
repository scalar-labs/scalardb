package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.Optional;
import java.util.UUID;

class CommitHandlerWithGroupCommitTest extends CommitHandlerTest {
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private String parentKey;
  private String childKey;
  private CoordinatorGroupCommitter groupCommitter;

  @Override
  protected void extraInitialize() {
    // `groupCommitter` is instantiated separately since the timing of the instantiation and calling
    // GroupCommitter.reserve() would be different.
    childKey = UUID.randomUUID().toString();
    String fullKey = groupCommitter.reserve(childKey);
    parentKey = keyManipulator.keysFromFullKey(fullKey).parentKey;
  }

  @Override
  protected Optional<CoordinatorGroupCommitter> groupCommitter() {
    if (groupCommitter == null) {
      groupCommitter = new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 60, 10));
    }
    return Optional.of(groupCommitter);
  }

  @Override
  protected String anyId() {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  @Override
  protected String anyGroupCommitParentId() {
    return parentKey;
  }

  @Override
  protected void extraCleanup() {
    groupCommitter.close();
  }
}
