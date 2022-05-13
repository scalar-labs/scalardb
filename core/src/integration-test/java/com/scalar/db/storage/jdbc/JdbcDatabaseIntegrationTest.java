package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Properties;

public class JdbcDatabaseIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }
}
