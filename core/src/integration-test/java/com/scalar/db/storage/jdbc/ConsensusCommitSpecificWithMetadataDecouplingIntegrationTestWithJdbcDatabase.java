package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isOracle")
public class ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase {

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
}
