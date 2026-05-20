package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithJdbcDatabase
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
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

  @Test
  @Override
  @DisabledIf("isSpanner")
  public void
      scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord()
          throws TransactionException {
    super
        .scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord();
  }

  @SuppressWarnings("unused")
  private boolean isSpanner() {
    return JdbcEnv.isSpanner();
  }
}
