package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class DistributedStorageAdminServiceRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize() throws IOException {
    ServerConfig config = ServerEnv.getServerConfig();
    if (config != null) {
      server = new ScalarDbServer(config);
      server.start();
    }
  }

  @Override
  protected Properties getProperties() {
    return ServerEnv.getServerConfig().getProperties();
  }

  @Override
  @AfterAll
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }
  /** This test is disabled if {@link #isExternalServerOrCassandraUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerOrCassandraUsed")
  public void repairTable_ForDeletedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForDeletedMetadataTable_ShouldRepairProperly();
  }
  /** This test is disabled if {@link #isExternalServerOrCassandraUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerOrCassandraUsed")
  public void repairTable_ForTruncatedMetadataTable_ShouldRepairProperly() throws Exception {
    super.repairTable_ForTruncatedMetadataTable_ShouldRepairProperly();
  }

  @Override
  protected Properties getStorageProperties() {
    return ServerEnv.getServerConfig().getProperties();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerOrCassandraUsed() {
    ServerConfig config = ServerEnv.getServerConfig();
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    if (config == null) {
      return true;
    }
    // These tests are skipped for Cassandra
    return config.getProperties().getProperty(DatabaseConfig.STORAGE, "").equals("cassandra");
  }
}
