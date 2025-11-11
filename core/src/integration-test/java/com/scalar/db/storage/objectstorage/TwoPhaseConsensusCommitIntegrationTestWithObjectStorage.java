package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class TwoPhaseConsensusCommitIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitIntegrationTestBase {

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
        .addColumn(TIMESTAMP_COL, DataType.TIMESTAMP)
        .addColumn(TIMESTAMPTZ_COL, DataType.TIMESTAMPTZ)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .build();
  }

  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanOrGetScanner_ScanGivenForIndexColumn_ShouldReturnRecords(ScanType scanType) {}
}
