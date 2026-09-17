package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestWithCassandra
    extends TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
