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
    Properties properties = ServerEnv.getServer1Properties(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
    }
  }

  @Override
  protected Properties getProperties(String testName) {
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
}
