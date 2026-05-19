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
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    // With metadata-decoupling, tables are views joining <table>_data and <table>_tx_metadata,
    // so DELETE must target the underlying source tables directly.
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      JdbcAdminTestUtils.deleteAllRowsFromVirtualTableWithSql(rdbEngine, namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }
}
