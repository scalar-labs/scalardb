package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf("isSqliteOrOracle")
public class ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @SuppressWarnings("unused")
  private static boolean isSqliteOrOracle() {
    return JdbcEnv.isSqlite() || JdbcEnv.isOracle();
  }
}
