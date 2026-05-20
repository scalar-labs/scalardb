package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties1(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    if (JdbcEnv.isYugabyte() && jdbcAdminTestUtils == null) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @Override
  protected void truncateTable1(String namespace, String table) throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable1(namespace, table);
  }

  @Override
  protected void truncateTable2(String namespace, String table) throws ExecutionException {
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable2(namespace, table);
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsFromCoordinatorTableWithSql();
      return;
    }
    super.truncateCoordinatorTables();
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }
}
