package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitNullMetadataIntegrationTestWithObjectStorage
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
