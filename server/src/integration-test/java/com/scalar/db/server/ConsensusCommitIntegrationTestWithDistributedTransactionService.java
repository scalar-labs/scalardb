package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitIntegrationTestWithDistributedTransactionService
    extends ConsensusCommitIntegrationTestBase {

  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED = "60053";

  private ScalarDbServer server;
  private ScalarDbServer serverWithIncludeMetadataEnabled;
  private boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
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
  public void scan_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    super.scan_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void scan_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    super.scan_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void get_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    super.get_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns();
  }

  /** This test is disabled if {@link #isExternalServerUsed()} return true */
  @Override
  @Test
  @DisabledIf("isExternalServerUsed")
  public void get_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    super.get_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns();
  }

  @SuppressWarnings("unused")
  private boolean isExternalServerUsed() {
    // An external server is used, so we don't have access to the configuration to connect to the
    // underlying storage which makes it impossible to run these tests
    return isExternalServerUsed;
  }
}
