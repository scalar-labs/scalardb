package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;

public class DistributedStorageServiceConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  private ScalarDbServer server;

  @Override
  protected void initialize() throws IOException {
    ServerConfig config = ServerEnv.getServerConfig();
    if (config != null) {
      server = new ScalarDbServer(config);
      server.start();
    }
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return ServerEnv.getGrpcConfig();
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
  }

  @Override
  protected int getThreadNum() {
    // Since Deadlock error sometimes happens in MySQL, change the concurrency to 1
    return 1;
  }
}
