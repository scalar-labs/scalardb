package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitCollationIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf("com.scalar.db.storage.jdbc.JdbcCollationTestUtils#isCollationTestSupported")
public class ConsensusCommitCollationIntegrationTestWithJdbcDatabase
    extends ConsensusCommitCollationIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    if (jdbcAdminTestUtils == null) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @Override
  protected void closeCollationTestResources() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void cleanUpCollationArtifacts(String namespace) throws Exception {
    jdbcAdminTestUtils.dropTestCollation(
        namespace, JdbcCollationTestUtils.getCollationTestTargetCollation());
  }

  @Override
  protected void applyNamespaceCollation(String namespace) throws Exception {
    jdbcAdminTestUtils.alterNamespaceCollation(
        namespace, JdbcCollationTestUtils.getCollationTestTargetCollation());
  }

  @Override
  protected void applyCollation(String namespace, String table) throws Exception {
    jdbcAdminTestUtils.alterTableCollation(
        namespace, table, JdbcCollationTestUtils.getCollationTestTargetCollation());
  }

  @Override
  protected int countRowsInBackendTable(String namespace, String table) throws Exception {
    return jdbcAdminTestUtils.countRows(namespace, table);
  }
}
