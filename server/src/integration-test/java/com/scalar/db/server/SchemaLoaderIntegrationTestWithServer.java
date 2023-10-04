package com.scalar.db.server;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class SchemaLoaderIntegrationTestWithServer extends SchemaLoaderIntegrationTestBase {

  private ScalarDbServer server;
  boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
    } else {
      isExternalServerUsed = true;
    }
  }

  @Override
  protected Properties getProperties(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties == null) {
      return null;
    }

    return new ServerAdminTestUtils(properties);
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void createTablesThenDropMetadataTableThenRepairTables_ShouldExecuteProperly()
      throws Exception {
    super.createTablesThenDropMetadataTableThenRepairTables_ShouldExecuteProperly();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void
      createTablesThenDropMetadataTableThenRepairTablesWithCoordinator_ShouldExecuteProperly()
          throws Exception {
    super.createTablesThenDropMetadataTableThenRepairTablesWithCoordinator_ShouldExecuteProperly();
  }

  @Test
  @DisabledIf("isExternalServerUsed")
  @Override
  public void createTablesThenDropTablesThenRepairTablesWithCoordinator_ShouldExecuteProperly()
      throws Exception {
    super.createTablesThenDropTablesThenRepairTablesWithCoordinator_ShouldExecuteProperly();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerUsed() {
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    return isExternalServerUsed;
  }
}
