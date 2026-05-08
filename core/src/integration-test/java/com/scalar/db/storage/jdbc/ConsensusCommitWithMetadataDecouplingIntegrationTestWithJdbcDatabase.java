package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithMetadataDecouplingIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitWithMetadataDecouplingIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      JdbcTestUtils.deleteAllRows(manager, admin, namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }
}
