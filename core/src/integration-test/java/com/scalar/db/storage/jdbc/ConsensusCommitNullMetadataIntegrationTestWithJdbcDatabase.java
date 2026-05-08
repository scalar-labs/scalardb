package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitNullMetadataIntegrationTestWithJdbcDatabase
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }
}
