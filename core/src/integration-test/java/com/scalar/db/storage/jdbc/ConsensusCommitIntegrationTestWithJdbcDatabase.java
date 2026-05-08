package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitIntegrationTestWithJdbcDatabase
    extends ConsensusCommitIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
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
