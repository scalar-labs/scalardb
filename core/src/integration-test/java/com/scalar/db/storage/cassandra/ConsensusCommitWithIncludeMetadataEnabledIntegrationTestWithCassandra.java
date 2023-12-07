package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithCassandra
    extends ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
