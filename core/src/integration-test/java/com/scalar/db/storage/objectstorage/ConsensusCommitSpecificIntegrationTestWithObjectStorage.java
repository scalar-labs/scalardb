package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Isolation;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitSpecificIntegrationTestWithObjectStorage
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  @BeforeEach
  protected void setUp() throws Exception {
    truncateTables();
    if (ObjectStorageEnv.isCloudStorage()) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Override
  @AfterEach
  public void tearDown() {
    recoveryExecutor.close();
    if (groupCommitter != null) {
      groupCommitter.close();
    }
    if (ObjectStorageEnv.isCloudStorage()) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(ACCOUNT_ID, DataType.INT)
        .addColumn(ACCOUNT_TYPE, DataType.INT)
        .addColumn(BALANCE, DataType.INT)
        .addColumn(SOME_COLUMN, DataType.TEXT)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .build();
  }

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scanWithIndex_PutWithOverlappedIndexKeyAndNonOverlappedConjunctionsGivenBefore_ShouldScan(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scanWithIndex_OverlappingPutWithNonIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scanWithIndex_NonOverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnAndConjunctionsGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanWithIndexGiven_WithSerializable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetWithIndexGiven_WithSerializable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void getAndUpdate_GetWithIndexGiven_ShouldUpdate(Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanAndUpdate_ScanWithIndexGiven_ShouldUpdate(Isolation isolation) {}
}
