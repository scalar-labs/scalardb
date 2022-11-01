package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class DistributedStorageServiceMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties = ServerEnv.getServerProperties1(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
    }
  }

  @Override
  protected Properties getProperties(String testName) {
    return ServerEnv.getClientProperties1(testName);
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
