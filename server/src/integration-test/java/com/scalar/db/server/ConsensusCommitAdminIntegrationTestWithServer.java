package com.scalar.db.server;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithServer
    extends ConsensusCommitAdminIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);

    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

      server = new ScalarDbServer(properties);
      server.start();
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ServerEnv.getClient1Properties(testName);
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
  protected String getSystemNamespaceName(Properties properties) {
    // "Unused for ScalarDB server"
    return null;
  }

  @Override
  @Test
  @Disabled("Retrieving the namespace names is not supported in ScalarDB server")
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() {}
}
