package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitAdminRepairIntegrationTestWithCassandra
    extends ConsensusCommitAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
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
    storageAdmin = new CassandraAdmin(clusterManager, new DatabaseConfig(properties));
    admin = new ConsensusCommitAdmin(storageAdmin, new DatabaseConfig(properties));
    adminTestUtils = new CassandraAdminTestUtils(getProperties(testName), clusterManager);
  }
}
