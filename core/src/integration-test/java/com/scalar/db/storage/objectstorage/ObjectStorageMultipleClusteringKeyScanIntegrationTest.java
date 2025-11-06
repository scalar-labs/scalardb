package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ObjectStorageMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected List<DataType> getDataTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    return super.getDataTypes().stream()
        .filter(type -> type != DataType.BLOB)
        .collect(Collectors.toList());
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return ObjectStorageTestUtils.getMinTextValue(columnName);
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return ObjectStorageTestUtils.getMaxTextValue(columnName);
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }
}
