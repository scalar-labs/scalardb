package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected void initialize(String testName) {
    Properties properties = getProperties(testName);
    ClusterManager clusterManager = new ClusterManager(new DatabaseConfig(properties));
    // Share the ClusterManager so that the keyspace metadata stay consistent between the Admin and
    // AdminTestUtils
    admin = new CassandraAdmin(clusterManager, new DatabaseConfig(properties));
    adminTestUtils = new CassandraAdminTestUtils(properties, clusterManager);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
