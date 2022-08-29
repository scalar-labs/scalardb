package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class ConsensusCommitIntegrationTestWithDistributedTransactionService
    extends ConsensusCommitIntegrationTestBase {

  private static final String PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED =
      Integer.toString(ServerConfig.DEFAULT_PORT + 1);

  private ScalarDbServer server;
  private ScalarDbServer serverWithIncludeMetadataEnabled;

  @Override
  protected void initialize() throws IOException {
    Properties properties = ServerEnv.getServerProperties();
    if (properties != null) {
      Properties props = TestUtils.addSuffix(properties, getTestName());

      // Async commit can cause unexpected lazy recoveries, which can fail the tests. So we disable
      // it for now.
      props.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "false");

      server = new ScalarDbServer(props);
      server.start();

      props.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
      props.setProperty(ServerConfig.PORT, PORT_FOR_SERVER_WITH_INCLUDE_METADATA_ENABLED);
      serverWithIncludeMetadataEnabled = new ScalarDbServer(props);
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
