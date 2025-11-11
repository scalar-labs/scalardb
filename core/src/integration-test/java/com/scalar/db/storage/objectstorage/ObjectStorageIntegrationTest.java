package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(getColumnName1(), DataType.INT)
        .addColumn(getColumnName2(), DataType.TEXT)
        .addColumn(getColumnName3(), DataType.INT)
        .addColumn(getColumnName4(), DataType.INT)
        .addColumn(getColumnName5(), DataType.BOOLEAN)
        .addColumn(getColumnName6(), DataType.BLOB)
        .addPartitionKey(getColumnName1())
        .addClusteringKey(getColumnName4())
        .build();
  }

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
