package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }
}
