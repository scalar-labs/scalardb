package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageCollationIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isCollationTestSupported")
public class JdbcDatabaseCollationIntegrationTest
    extends DistributedStorageCollationIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    if (jdbcAdminTestUtils == null) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void applyCollation(String namespace, String table) throws Exception {
    jdbcAdminTestUtils.alterTableCollation(
        namespace, table, JdbcEnv.getCollationTestTargetCollation());
  }

  @Override
  protected boolean isAccentVariantSupported() {
    // SQL Server's _CI_AI coverage in these tests is constrained to basic-Latin case variants
    return !JdbcEnv.isSqlServer();
  }
}
