package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;

public class TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    if (JdbcEnv.isYugabyte()) {
      // Pre-apply the coordinator suffix the base class will add.
      Properties utilsProps = new Properties();
      utilsProps.putAll(properties);
      ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(utilsProps, testName);
      jdbcAdminTestUtils = new JdbcAdminTestUtils(utilsProps);
    }
    return properties;
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
