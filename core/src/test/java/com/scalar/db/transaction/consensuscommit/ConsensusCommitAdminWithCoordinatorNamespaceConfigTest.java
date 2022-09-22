package com.scalar.db.transaction.consensuscommit;

import java.util.Optional;

public class ConsensusCommitAdminWithCoordinatorNamespaceConfigTest
    extends ConsensusCommitAdminTestBase {

  @Override
  protected Optional<String> getCoordinatorNamespaceConfig() {
    return Optional.of("my_coordinator_ns");
  }
}
