package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class TwoPhaseConsensusCommitIntegrationTestWithTwoPhaseCommitTransactionService
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED =
      Integer.toString(ServerConfig.DEFAULT_PORT + 1);

  private ScalarDbServer server;
  private ScalarDbServer serverWithIncludeMetadataEnabled;

  @Override
  protected void initialize() throws IOException {
    Properties properties = ServerEnv.getServerProperties();
    if (properties != null) {
      server = new ScalarDbServer(TestUtils.addSuffix(properties, getTestName()));
      server.start();

      properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
      properties.setProperty(ServerConfig.PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
      serverWithIncludeMetadataEnabled =
          new ScalarDbServer(TestUtils.addSuffix(properties, getTestName()));
      serverWithIncludeMetadataEnabled.start();
    }
  }

  @Override
  protected Properties getProps() {
    return ServerEnv.getProperties();
  }

  @Override
  protected Properties getPropsWithIncludeMetadataEnabled() {
    Properties properties = getProperties();
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
