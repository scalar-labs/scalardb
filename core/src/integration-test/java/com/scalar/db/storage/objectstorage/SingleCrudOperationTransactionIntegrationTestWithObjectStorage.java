package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;

public class SingleCrudOperationTransactionIntegrationTestWithObjectStorage
    extends SingleCrudOperationTransactionIntegrationTestBase {

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(ACCOUNT_ID, DataType.INT)
        .addColumn(ACCOUNT_TYPE, DataType.INT)
        .addColumn(BALANCE, DataType.INT)
        .addColumn(SOME_COLUMN, DataType.INT)
        .addColumn(BOOLEAN_COL, DataType.BOOLEAN)
        .addColumn(BIGINT_COL, DataType.BIGINT)
        .addColumn(FLOAT_COL, DataType.FLOAT)
        .addColumn(DOUBLE_COL, DataType.DOUBLE)
        .addColumn(TEXT_COL, DataType.TEXT)
        .addColumn(BLOB_COL, DataType.BLOB)
        .addColumn(DATE_COL, DataType.DATE)
        .addColumn(TIME_COL, DataType.TIME)
        .addColumn(TIMESTAMPTZ_COL, DataType.TIMESTAMPTZ)
        .addColumn(TIMESTAMP_COL, DataType.TIMESTAMP)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .build();
  }

  @Override
  protected Properties getProps(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
