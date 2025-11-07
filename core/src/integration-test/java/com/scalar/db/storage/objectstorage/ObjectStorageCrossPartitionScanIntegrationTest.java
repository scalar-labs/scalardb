package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ObjectStorageCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Object Storages")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
