package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class TwoPhaseConsensusCommitIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanOrGetScanner_ScanGivenForIndexColumn_ShouldReturnRecords(ScanType scanType) {}
}
