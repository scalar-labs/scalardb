package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProps1(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    // Pre-apply the coordinator suffix the base class will add.
    Properties utilsProps = new Properties();
    utilsProps.putAll(properties);
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(utilsProps, testName);
    jdbcAdminTestUtils = new JdbcAdminTestUtils(utilsProps);
    return properties;
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (jdbcAdminTestUtils.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @Override
  protected void truncateTable2(String namespace, String table) throws ExecutionException {
    if (jdbcAdminTestUtils.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable2(namespace, table);
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    if (jdbcAdminTestUtils.isYugabyte()) {
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

  @Test
  @DisabledIf("isSpanner")
  @Override
  public void
      scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecords()
          throws TransactionException {
    super
        .scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecords();
  }

  @SuppressWarnings("unused")
  private boolean isSpanner() {
    return JdbcEnv.isSpanner();
  }
}
