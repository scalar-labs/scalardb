package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(
    named = "scalardb.object_storage.test_group",
    matches = "two_phase_consensus_commit")
public class TwoPhaseConsensusCommitSpecificIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties1(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
