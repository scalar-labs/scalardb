package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionAdminIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AutoCommitTransactionAdminIntegrationTestWithCassandra
    extends AutoCommitTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
