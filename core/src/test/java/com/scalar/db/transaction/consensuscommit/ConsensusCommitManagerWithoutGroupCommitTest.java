package com.scalar.db.transaction.consensuscommit;

import java.util.Optional;

public class ConsensusCommitManagerWithoutGroupCommitTest extends ConsensusCommitManagerTestBase {
  private static final String ANY_TX_ID = "any_id";

  @Override
  Optional<CoordinatorGroupCommitter> groupCommitter() {
    return Optional.empty();
  }

  @Override
  String anyTxIdGivenByClient() {
    return ANY_TX_ID;
  }

  @Override
  String anyTxIdAlreadyStarted() {
    return ANY_TX_ID;
  }

  @Override
  boolean isGroupCommitEnabled() {
    return false;
  }
}
