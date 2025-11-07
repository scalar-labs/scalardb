package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(COL_NAME1, DataType.INT)
        .addColumn(COL_NAME2, DataType.TEXT)
        .addColumn(COL_NAME3, DataType.TEXT)
        .addColumn(COL_NAME4, DataType.INT)
        .addColumn(COL_NAME5, DataType.INT)
        .addColumn(COL_NAME6, DataType.TEXT)
        .addColumn(COL_NAME7, DataType.BIGINT)
        .addColumn(COL_NAME8, DataType.FLOAT)
        .addColumn(COL_NAME9, DataType.DOUBLE)
        .addColumn(COL_NAME10, DataType.BOOLEAN)
        .addColumn(COL_NAME11, DataType.BLOB)
        .addColumn(COL_NAME12, DataType.DATE)
        .addColumn(COL_NAME13, DataType.TIME)
        .addColumn(COL_NAME14, DataType.TIMESTAMPTZ)
        .addColumn(COL_NAME15, DataType.TIMESTAMP)
        .addPartitionKey(COL_NAME2)
        .addPartitionKey(COL_NAME1)
        .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
        .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
        .build();
  }

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new ObjectStorageAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      repairTable_WhenTableAlreadyExistsWithoutIndexAndMetadataSpecifiesIndex_ShouldCreateIndex() {}
}
