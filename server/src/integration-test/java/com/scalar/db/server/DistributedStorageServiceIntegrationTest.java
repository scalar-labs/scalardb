package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;

public class DistributedStorageServiceIntegrationTest
    extends DistributedStorageIntegrationTestBase {

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
  protected Properties getProperties() {
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

  @Override
  @Disabled("ScanAll is not yet implemented for GRPC")
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for GRPC")
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for GRPC")
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues() {}

  @Override
  @Disabled("ScanAll is not yet implemented for GRPC")
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues() {}
}
