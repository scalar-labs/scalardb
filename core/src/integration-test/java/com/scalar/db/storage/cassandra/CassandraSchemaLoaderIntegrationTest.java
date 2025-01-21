package com.scalar.db.storage.cassandra;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

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
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
