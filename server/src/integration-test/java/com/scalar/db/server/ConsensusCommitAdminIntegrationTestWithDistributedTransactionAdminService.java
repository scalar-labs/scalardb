package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class ConsensusCommitAdminIntegrationTestWithDistributedTransactionAdminService
    extends ConsensusCommitAdminIntegrationTestBase {
  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED =
      Integer.toString(ServerConfig.DEFAULT_PORT + 1);

  private ScalarDbServer server;
  private ScalarDbServer serverWithIncludeMetadataEnabled;

  @Override
  protected void initialize(String testName) throws IOException {
    super.initialize(testName);

    Properties properties = ServerEnv.getServerProperties(testName);
    if (properties != null) {
      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

      // Async commit can cause unexpected lazy recoveries, which can fail the tests. So we disable
      // it for now.
      properties.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "false");

      server = new ScalarDbServer(properties);
      server.start();

      properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
      properties.setProperty(ServerConfig.PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
      serverWithIncludeMetadataEnabled = new ScalarDbServer(properties);
      serverWithIncludeMetadataEnabled.start();
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ServerEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties(testName);
    properties.setProperty(
        DatabaseConfig.CONTACT_PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
    return properties;
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
    if (serverWithIncludeMetadataEnabled != null) {
      serverWithIncludeMetadataEnabled.shutdown();
    }
  }
}
