package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitAdminRepairTableIntegrationTestWithDistributedTransactionAdminService
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize() throws IOException {
    Properties properties = ServerEnv.getServerProperties();
    if (properties != null) {
      server = new ScalarDbServer(TestUtils.addSuffix(properties, TEST_NAME));
      server.start();
    }
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }

  @Override
  protected Properties getProps() {
    return ServerEnv.getProperties();
  }

  /** This test is disabled if {@link #isExternalServerOrCassandraUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerOrCassandraUsed")
  public void repairTableAndCoordinatorTable_ForDeletedMetadataTable_ShouldRepairProperly()
      throws Exception {
    super.repairTableAndCoordinatorTable_ForDeletedMetadataTable_ShouldRepairProperly();
  }

  /** This test is disabled if {@link #isExternalServerOrCassandraUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerOrCassandraUsed")
  public void repairTableAndCoordinatorTable_ForTruncatedMetadataTable_ShouldRepairProperly()
      throws Exception {
    super.repairTableAndCoordinatorTable_ForTruncatedMetadataTable_ShouldRepairProperly();
  }

  @Override
  protected Properties getStorageProperties() {
    return ServerEnv.getServerProperties();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerOrCassandraUsed() {
    Properties properties = ServerEnv.getServerProperties();
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    if (properties == null) {
      return true;
    }
    // These tests are skipped for Cassandra
    return properties.getProperty(DatabaseConfig.STORAGE, "").equals("cassandra");
  }
}
