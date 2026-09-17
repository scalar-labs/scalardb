package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class SingleCrudOperationTransactionIntegrationTestWithJdbcDatabase
    extends SingleCrudOperationTransactionIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    if (JdbcEnv.isYugabyte() && jdbcAdminTestUtils == null) {
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
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }
}
