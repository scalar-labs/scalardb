package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcAdminTestUtils;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JdbcTransactionIntegrationTest extends DistributedTransactionIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected String getTestName() {
    return "tx_jdbc";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, JdbcConfig.TRANSACTION_MANAGER_NAME);
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
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking and is slow.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsFromCoordinatorTableWithSql();
      return;
    }
    super.truncateCoordinatorTables();
  }

  @Disabled("JDBC transactions don't support getState()")
  @Override
  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("JDBC transactions don't support getState()")
  @Override
  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("JDBC transactions don't support abort()")
  @Override
  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("JDBC transactions don't support rollback()")
  @Override
  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}
}
