package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitIntegrationTestWithObjectStorage
    extends ConsensusCommitIntegrationTestBase {

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    if (ObjectStorageEnv.isCloudStorage()) {
      // Sleep to mitigate rate limit errors
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

  @AfterEach
  void tearDown() {
    if (ObjectStorageEnv.isCloudStorage()) {
      // Sleep to mitigate rate limit errors
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

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
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanOrGetScanner_ScanGivenForIndexColumn_ShouldReturnRecords(ScanType scanType) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanOrGetScanner_ScanGivenForIndexColumnWithConjunctions_ShouldReturnRecords(
      ScanType scanType) {}
}
