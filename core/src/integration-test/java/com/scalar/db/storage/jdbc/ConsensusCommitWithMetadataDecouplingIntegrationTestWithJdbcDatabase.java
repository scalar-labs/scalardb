package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithMetadataDecouplingIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class ConsensusCommitWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitWithMetadataDecouplingIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;
  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcEnv.isYugabyte()) {
      // Pre-apply the coordinator suffix the base class will add.
      Properties utilsProps = new Properties();
      utilsProps.putAll(properties);
      ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(utilsProps, testName);
      jdbcAdminTestUtils = new JdbcAdminTestUtils(utilsProps);
    }
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    // With metadata-decoupling, tables are views joining <table>_data and <table>_tx_metadata,
    // so DELETE must target the underlying source tables directly.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsFromVirtualTableWithSql(namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsFromCoordinatorTableWithSql();
      return;
    }
    super.truncateCoordinatorTables();
  }
}
