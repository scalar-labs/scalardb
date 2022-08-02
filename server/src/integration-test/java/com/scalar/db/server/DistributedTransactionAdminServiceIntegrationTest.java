package com.scalar.db.server;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class DistributedTransactionAdminServiceIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

  private ScalarDbServer server;
  private ScalarDbServer serverWithDebug;

  @Override
  protected void initialize() throws IOException {
    ServerConfig config = ServerEnv.getServerConfig();
    if (config != null) {
      server = new ScalarDbServer(config);
      server.start();

      // Create a second server with debug turned on
      Properties serverWithDebugProperties = new Properties();
      serverWithDebugProperties.putAll(config.getProperties());
      String serverWithDebugContactPort = Integer.toString(config.getPort() + 1);
      serverWithDebugProperties.setProperty(ServerConfig.PORT, serverWithDebugContactPort);
      serverWithDebugProperties.setProperty(DatabaseConfig.DEBUG, "true");
      serverWithDebug = new ScalarDbServer(new ServerConfig(serverWithDebugProperties));
      serverWithDebug.start();

      Properties clientForServerWithDebugProperties = new Properties();
      clientForServerWithDebugProperties.putAll(getProperties());
      clientForServerWithDebugProperties.setProperty(
          DatabaseConfig.CONTACT_PORT, serverWithDebugContactPort);
      adminWithDebug =
          TransactionFactory.create(clientForServerWithDebugProperties).getTransactionAdmin();
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
    if (serverWithDebug != null) {
      serverWithDebug.shutdown();
    }
  }

  @Test
  @DisabledIf("isExternalServerUsed")
  @Override
  public void
      getTableMetadata_WhenDebugging_ShouldReturnCorrectMetadataWithTransactionMetadataColumns()
          throws ExecutionException {
    super
        .getTableMetadata_WhenDebugging_ShouldReturnCorrectMetadataWithTransactionMetadataColumns();
  }

  public boolean isExternalServerUsed() {
    return ServerEnv.getServerConfig() == null;
  }
}
