package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import java.util.Properties;

public class JdbcAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }
}
