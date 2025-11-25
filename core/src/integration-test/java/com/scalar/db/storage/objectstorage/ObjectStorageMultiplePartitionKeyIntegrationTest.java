package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import java.util.stream.IntStream;

public class ObjectStorageMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT && ObjectStorageEnv.isCloudStorage()) {
      // Since Cloud Storage can't handle 0xFF character correctly, we use "ZZZ..." as the max value
      StringBuilder builder = new StringBuilder();
      IntStream.range(0, TestUtils.MAX_TEXT_COUNT).forEach(i -> builder.append('Z'));
      return TextColumn.of(columnName, builder.toString());
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }
}
