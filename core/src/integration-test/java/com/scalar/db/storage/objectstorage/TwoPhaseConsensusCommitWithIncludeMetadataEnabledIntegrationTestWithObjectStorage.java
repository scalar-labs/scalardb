package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(
    named = "scalardb.object_storage.test_group",
    matches = "two_phase_consensus_commit")
public class TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
