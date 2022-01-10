package com.scalar.db.schemaloader.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.server.ScalarDbServer;
import com.scalar.db.server.ServerConfig;
import com.scalar.db.server.ServerEnv;
import java.io.IOException;
import org.junit.AfterClass;

public class SchemaLoaderWithScalarDbServer extends SchemaLoaderIntegrationTestBase {

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
    SchemaLoaderIntegrationTestBase.tearDownAfterClass();
    if (server != null) {
      server.shutdown();
      server.blockUntilShutdown();
    }
  }
}
