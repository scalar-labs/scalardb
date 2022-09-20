package com.scalar.db.transaction.consensuscommit;

import java.util.Optional;

public class ConsensusCommitAdminWithDefaultConfigTest extends ConsensusCommitAdminTestBase {

  @Override
  protected Optional<String> getCoordinatorNamespaceConfig() {
    return Optional.empty();
  }
}
