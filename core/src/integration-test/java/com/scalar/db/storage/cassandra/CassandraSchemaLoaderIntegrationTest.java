package com.scalar.db.storage.cassandra;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected void waitForNamespacesTableCreation() {
    // After the keyspaces metadata table is created, since it is not readable right away using the
    // Cluster API of the Cassandra driver, we need to wait a bit.
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
  }
}
