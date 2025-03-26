package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }

  @Test
  @Override
  @Disabled("Index-related operations are not supported for object storages")
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for object storages")
  public void scan_ScanGivenForIndexColumn_ShouldReturnRecords() {}
}
