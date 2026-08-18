package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageCollationIntegrationTestBase;
import java.util.Properties;

/**
 * Runs the storage-layer collation suite on object storage, where ScalarDB performs all ordering
 * and filtering in memory. No backend collation exists, so the {@code applyCollation} step stays
 * the base's no-op: {@code scalar.db.collation=ICU} alone dictates the behavior under test.
 */
public class ObjectStorageCollationIntegrationTest
    extends DistributedStorageCollationIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
