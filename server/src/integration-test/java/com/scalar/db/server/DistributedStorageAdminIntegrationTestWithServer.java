package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class DistributedStorageAdminIntegrationTestWithServer
    extends DistributedStorageAdminIntegrationTestBase {

  private ScalarDbServer server;
  private boolean isExternalServerUsed;

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

    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    return new ServerAdminTestUtils(properties);
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces()
          throws Exception {
    super
        .upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerUsed() {
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    return isExternalServerUsed;
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
