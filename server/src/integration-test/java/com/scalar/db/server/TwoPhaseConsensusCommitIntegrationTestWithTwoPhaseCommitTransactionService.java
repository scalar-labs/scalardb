package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class TwoPhaseConsensusCommitIntegrationTestWithTwoPhaseCommitTransactionService
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED = "60053";

  private ScalarDbServer server1;
  private ScalarDbServer server2;
  private ScalarDbServer serverWithIncludeMetadataEnabled;
  private boolean isExternalServerUsed;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties1 = ServerEnv.getServer1Properties(testName);
    Properties properties2 = ServerEnv.getServer2Properties(testName);
    if (properties1 != null && properties2 != null) {
      server1 = new ScalarDbServer(modifyProperties(properties1, testName));
      server1.start();

      server2 = new ScalarDbServer(modifyProperties(properties2, testName));
      server2.start();

      properties1.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
      properties1.setProperty(ServerConfig.PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
      serverWithIncludeMetadataEnabled = new ScalarDbServer(properties1);
      serverWithIncludeMetadataEnabled.start();
    } else {
      isExternalServerUsed = true;
    }
  }

  private Properties modifyProperties(Properties properties, String testName) {
    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);

    // Async commit can cause unexpected lazy recoveries, which can fail the tests. So we disable
    // it for now.
    properties.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "false");

    return properties;
  }

  @Override
  protected Properties getProps1(String testName) {
    return ServerEnv.getClient1Properties(testName);
  }

  @Override
  protected Properties getProps2(String testName) {
    return ServerEnv.getClient2Properties(testName);
  }

  @Override
  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties1(testName);
    properties.setProperty(
        DatabaseConfig.CONTACT_PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
    return properties;
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();
    if (server1 != null) {
      server1.shutdown();
    }
    if (server2 != null) {
      server2.shutdown();
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
