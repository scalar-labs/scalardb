package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DistributedStorageIntegrationTestWithServer
    extends DistributedStorageIntegrationTestBase {

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

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetWithMatchedConjunctionsGiven_ShouldRetrieveSingleResult() {}

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetWithUnmatchedConjunctionsGiven_ShouldReturnEmpty() {}

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetGivenForIndexedColumnWithMatchedConjunctions_ShouldGet() {}

  @Disabled("ScalarDB Server doesn't support get() with conjunctions")
  @Override
  @Test
  public void get_GetGivenForIndexedColumnWithUnmatchedConjunctions_ShouldReturnEmpty() {}

  @Disabled("ScalarDB Server doesn't support scan() with conjunctions")
  @Override
  @Test
  public void
      scan_ScanWithClusteringKeyRangeAndConjunctionsGiven_ShouldRetrieveResultsOfBothConditions() {}
}
