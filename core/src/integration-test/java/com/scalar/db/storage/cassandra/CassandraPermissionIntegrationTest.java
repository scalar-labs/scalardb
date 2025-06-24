package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraPermissionIntegrationTest
    extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return CassandraEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new CassandraPermissionTestUtils(getProperties(testName));
  }
}
