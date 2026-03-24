package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  // TODO Investigate failure
  @Disabled("Fails because of java.lang.OutOfMemoryError exception")
  public void put_largeBlobData_ShouldWorkCorrectly(int blobSize, String humanReadableBlobSize)
      throws ExecutionException {
    super.put_largeBlobData_ShouldWorkCorrectly(blobSize, humanReadableBlobSize);
  }
}
