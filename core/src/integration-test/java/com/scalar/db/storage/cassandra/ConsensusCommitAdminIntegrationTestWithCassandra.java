package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithCassandra
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CassandraConfig(new DatabaseConfig(properties))
        .getSystemNamespaceName()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected String getCoordinatorNamespaceName(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Override
  protected boolean isGroupCommitEnabled(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .isCoordinatorGroupCommitEnabled();
  }

  @Override
  protected void extraCheckOnCoordinatorTable() {}
}
