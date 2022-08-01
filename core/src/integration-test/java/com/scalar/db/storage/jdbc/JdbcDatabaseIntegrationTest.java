package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }

  @Override
  protected int getLargeDataSizeInBytes() {
    RdbEngine rdbEngine =
        JdbcUtils.getRdbEngine(new JdbcConfig(new DatabaseConfig(getProperties())).getJdbcUrl());

    if (rdbEngine == RdbEngine.ORACLE) {
      // For Oracle, the max data size for BLOB is 2000 bytes
      return 2000;
    } else {
      return super.getLargeDataSizeInBytes();
    }
  }
}
