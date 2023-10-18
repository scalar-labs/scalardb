package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DistributedStorageAdminIntegrationTestWithServer
    extends DistributedStorageAdminIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize(String testName) throws IOException {
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
  @Test
  @Disabled("Retrieving the namespace names is not supported in ScalarDB server")
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() {}

  @Test
  @Disabled("upgrade() is not supported in ScalarDB server")
  @Override
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    throw new UnsupportedOperationException();
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }
}
