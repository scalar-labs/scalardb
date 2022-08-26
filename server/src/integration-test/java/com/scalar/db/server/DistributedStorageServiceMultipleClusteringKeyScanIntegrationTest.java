package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class DistributedStorageServiceMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize() throws IOException {
    Properties properties = ServerEnv.getServerProperties();
    if (properties != null) {
      server = new ScalarDbServer(TestUtils.addSuffix(properties, TEST_NAME));
      server.start();
    }
  }

  @Override
  protected Properties getProperties() {
    return ServerEnv.getProperties();
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
