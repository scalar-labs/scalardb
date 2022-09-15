package com.scalar.db.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class SchemaLoaderIntegrationTestWithScalarDbServer extends SchemaLoaderIntegrationTestBase {

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

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new ServerAdminTestUtils(ServerEnv.getServerProperties(testName));
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException, IOException {
    super.afterAll();
    if (server != null) {
      server.shutdown();
    }
  }
}
