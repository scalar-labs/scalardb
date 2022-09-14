package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithCassandra
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }
}
