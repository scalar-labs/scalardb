package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithCassandra
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }
}
