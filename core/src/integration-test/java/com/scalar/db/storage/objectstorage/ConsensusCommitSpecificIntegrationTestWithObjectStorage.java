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
    super.setUp();
    if (ObjectStorageEnv.isCloudStorage()) {
      // Sleep to mitigate rate limit errors
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

  @Override
  @AfterEach
  public void tearDown() {
    super.tearDown();
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
  public void scan_ScanWithIndexGiven_WithSerializable_ShouldScan() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetWithIndexGiven_WithSerializable_ShouldGet() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void getAndUpdate_GetWithIndexGiven_ShouldUpdate(Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanAndUpdate_ScanWithIndexGiven_ShouldUpdate(Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexForPreparedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnResult(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetWithIndexForPreparedWhenCoordinatorStateAborted_ShouldRollBackAndReturnResult(
      Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortAndReturnResult(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldThrowException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnEmpty(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetWithIndexForDeletedWhenCoordinatorStateAborted_ShouldRollBackAndReturnResult(
      Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForPreparedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForPreparedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForPreparedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForPreparedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldThrowException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldThrowException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldThrowException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldThrowException(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexForDeletedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanWithIndexForDeletedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      getScanner_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateAborted_ShouldRollBackAndReturnAllRecords(
          Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      commit_GetWithIndexInSerializable_WhenBeforeIndexHasPreparedRecordFromOtherTransaction_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      commit_ScanWithIndexInSerializable_WhenBeforeIndexHasPreparedRecordFromOtherTransaction_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      commit_ScanAllWithIndexConditionInSerializable_WhenBeforeIndexHasPreparedRecordFromOtherTransaction_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      commit_GetScannerWithIndexInSerializable_WhenBeforeIndexHasPreparedRecordFromOtherTransaction_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      commit_GetScannerWithScanAllIndexConditionInSerializable_WhenBeforeIndexHasPreparedRecordFromOtherTransaction_ShouldThrowCommitConflictException() {}
}
