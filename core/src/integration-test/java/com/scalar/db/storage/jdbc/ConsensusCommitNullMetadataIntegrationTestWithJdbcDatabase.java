package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class ConsensusCommitNullMetadataIntegrationTestWithJdbcDatabase
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;
  private DistributedTransactionManager truncationManager;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      Properties managerProps = new Properties(properties);
      ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(managerProps, testName);
      truncationManager = TransactionFactory.create(managerProps).getTransactionManager();
    }
    return properties;
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      JdbcTestUtils.deleteAllRows(truncationManager, namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    try {
      super.afterAll();
    } finally {
      if (truncationManager != null) {
        truncationManager.close();
      }
    }
  }
}
