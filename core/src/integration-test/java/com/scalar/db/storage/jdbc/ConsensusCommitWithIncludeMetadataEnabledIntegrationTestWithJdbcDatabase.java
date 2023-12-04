package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithJdbcDatabase
    extends ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
