package com.scalar.db.server;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class DistributedTransactionAdminServiceIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

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
  protected Properties gerProperties() {
    return ServerEnv.getProperties();
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
}
