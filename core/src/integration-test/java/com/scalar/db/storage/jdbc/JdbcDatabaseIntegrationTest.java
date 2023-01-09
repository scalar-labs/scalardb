package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  private RdbEngine rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineStrategy.create(config).getRdbEngine();
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected int getLargeDataSizeInBytes() {
    if (rdbEngine == RdbEngine.ORACLE) {
      // For Oracle, the max data size for BLOB is 2000 bytes
      return 2000;
    } else {
      return super.getLargeDataSizeInBytes();
    }
  }
}
