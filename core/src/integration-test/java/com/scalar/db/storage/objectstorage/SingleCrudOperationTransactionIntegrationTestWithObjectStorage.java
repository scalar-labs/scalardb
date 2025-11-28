package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
  protected void populateRecords() throws TransactionException {
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        InsertBuilder.Buildable insert =
            Insert.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey);
        prepareNonKeyColumns(i, j).forEach(insert::value);
        manager.insert(insert.build());
        if (ObjectStorageEnv.isCloudStorage()) {
          // Sleep to mitigate rate limit errors
          Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
