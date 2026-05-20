package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class ConsensusCommitAdminRepairTableIntegrationTestWithObjectStorage
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new ObjectStorageAdminTestUtils(getProperties(testName));
  }
}
