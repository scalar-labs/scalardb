package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class JdbcDatabaseMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    if (JdbcEnv.isYugabyte()) {
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
  protected void truncateTable() throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace1(), TABLE1);
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace1(), TABLE2);
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace2(), TABLE1);
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace3(), TABLE1);
      return;
    }
    super.truncateTable();
  }
}
