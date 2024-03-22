package com.scalar.db.transaction.consensuscommit;

import java.util.Optional;

class CommitHandlerWithoutGroupCommitTest extends CommitHandlerTestBase {
  private static final String ANY_ID = "id";

  @Override
  Optional<CoordinatorGroupCommitter> groupCommitter() {
    return Optional.empty();
  }

  @Override
  String anyId() {
    return ANY_ID;
  }

  @Override
  String anyGroupCommitParentId() {
    throw new AssertionError("Shouldn't reach here");
  }
}
