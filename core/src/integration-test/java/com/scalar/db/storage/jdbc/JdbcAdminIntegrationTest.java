package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import java.util.Properties;

public class JdbcAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }
}
