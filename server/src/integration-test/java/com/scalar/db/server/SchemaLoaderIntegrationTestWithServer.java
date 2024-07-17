package com.scalar.db.server;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class SchemaLoaderIntegrationTestWithServer extends SchemaLoaderIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
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

  @Test
  @Disabled("repairAll is not supported with ScalarDB Server")
  @Override
  public void createTablesThenDropTablesThenRepairAllWithoutCoordinator_ShouldExecuteProperly() {}

  @Test
  @Disabled("repairAll is not supported with ScalarDB Server")
  @Override
  public void createTablesThenDropTablesThenRepairAllWithCoordinator_ShouldExecuteProperly() {}

  @Test
  @Disabled("upgrade is not supported with ScalarDB Server")
  @Override
  public void createThenDropNamespacesTableThenUpgrade_ShouldCreateNamespacesTableCorrectly()
      throws Exception {
    super.createThenDropNamespacesTableThenUpgrade_ShouldCreateNamespacesTableCorrectly();
  }

  @Test
  @Disabled("upgrade is not supported with ScalarDB Server")
  @Override
  public void upgrade_WithOldCoordinatorTableSchema_ShouldProperlyUpdateTheSchema() {}
}
