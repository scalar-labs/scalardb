package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.io.IOException;
import org.junit.AfterClass;

public class DistributedStorageServiceIntegrationTest extends StorageIntegrationTestBase {

  private static ScalarDbServer server;

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

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    StorageIntegrationTestBase.tearDownAfterClass();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
  }
}
