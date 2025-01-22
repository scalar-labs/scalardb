package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SingleCrudOperationTransactionAdminIntegrationTestWithCassandra
    extends SingleCrudOperationTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME;
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
