package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageCaseSensitivityIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageCaseSensitivityIntegrationTest
    extends DistributedStorageCaseSensitivityIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumn_ShouldGet() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumnWithMatchedConjunctions_ShouldGet() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumnWithUnmatchedConjunctions_ShouldReturnEmpty() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanGivenForIndexedColumn_ShouldScan() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {}
}
