package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class JdbcTransactionAdminIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Disabled("There is no metadata columns when using the jdbc transaction manager")
  @Override
  public void
      getTableMetadata_WhenDebugging_ShouldReturnCorrectMetadataWithTransactionMetadataColumns() {}
}
