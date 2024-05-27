package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithCosmos
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    Properties properties = CosmosEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cosmos DB")
  public void scan_CrossPartitionScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
