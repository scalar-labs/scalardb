package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BlobSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected List<DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    List<DataType> clusteringKeyTypes = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      if (dataType == DataType.BLOB) {
        continue;
      }
      clusteringKeyTypes.add(dataType);
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return BlobTestUtils.getMinTextValue(columnName);
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return BlobTestUtils.getMaxTextValue(columnName);
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }
}
