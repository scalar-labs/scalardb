package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class DistributedStorageServiceConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

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
  public void afterAll() throws ExecutionException {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }

  @Override
  protected int getThreadNum() {
    // Since Deadlock error sometimes happens in MySQL, change the concurrency to 1
    return 1;
  }
}
