package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitAdminIntegrationTestWithDistributedTransactionAdminService
    extends ConsensusCommitAdminIntegrationTestBase {

  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED = "60053";

  private ScalarDbServer server;
  private ScalarDbServer serverWithIncludeMetadataEnabled;
  private boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
    super.initialize(testName);

    Properties properties = ServerEnv.getServer1Properties(testName);
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
    } else {
      isExternalServerUsed = true;
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @Override
  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties(testName);
    properties.setProperty(
        DatabaseConfig.CONTACT_PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    Properties properties = ServerEnv.getServerProperties1(testName);

    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    return new ServerAdminTestUtils(properties);
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

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void
      getTableMetadata_WhenIncludeMetadataIsEnabled_ShouldReturnCorrectMetadataWithTransactionMetadataColumns()
          throws ExecutionException {
    super
        .getTableMetadata_WhenIncludeMetadataIsEnabled_ShouldReturnCorrectMetadataWithTransactionMetadataColumns();
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
}
