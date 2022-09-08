package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import java.util.Properties;

public class CassandraAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    return CassandraEnv.getProperties();
  }
}
