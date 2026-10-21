package com.scalar.db.storage.objectstorage;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Properties;

public class DistributedTransactionBackedConsensusCommitGlobalTransactionTestWithObjectStorage
    extends DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    ObjectStorageTestUtils.executeWithRetryOnRateLimit(() -> super.truncateTable(namespace, table));
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    ObjectStorageTestUtils.executeWithRetryOnRateLimit(super::truncateCoordinatorTables);
  }

  @Override
  protected void dropTable(String namespace, String table) throws ExecutionException {
    ObjectStorageTestUtils.executeWithRetryOnRateLimit(() -> super.dropTable(namespace, table));
  }

  @Override
  protected void dropCoordinatorTables() throws ExecutionException {
    ObjectStorageTestUtils.dropCoordinatorTablesWithRetryOnRateLimit(getProperties(getTestName()));
  }

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
