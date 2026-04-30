package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "scalardb.object_storage.test_group", matches = "consensus_commit")
public class ConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithObjectStorage
    extends ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
