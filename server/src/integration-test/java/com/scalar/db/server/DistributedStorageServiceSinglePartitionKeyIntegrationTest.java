package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class DistributedStorageServiceSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize(String testName) throws IOException {
    Properties properties = ServerEnv.getServerProperties(testName);
    if (properties != null) {
      server = new ScalarDbServer(properties);
      server.start();
    }
  }

  @Override
  protected Properties getProperties(String testName) {
    return ServerEnv.getProperties(testName);
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }
}
