package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Isolation;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitSpecificIntegrationTestWithObjectStorage
    extends ConsensusCommitSpecificIntegrationTestBase {

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
  public void scan_ScanWithIndexGiven_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexGiven_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexGiven_RecordUpdatedByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexGiven_RecordDeletedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      scan_ScanWithIndexGiven_RecordDeletedByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanWithIndexWithLimitGiven_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetWithIndexGiven_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordUpdatedByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordDeletedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordDeletedByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordInsertedIntoIndexRangeByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_RecordInsertedIntoIndexRangeByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_RecordInsertedIntoIndexRangeByMyself_WithSerializable_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_RecordInsertedIntoIndexRangeByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void getAndUpdate_GetWithIndexGiven_ShouldUpdate(Isolation isolation) {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scanAndUpdate_ScanWithIndexGiven_ShouldUpdate(Isolation isolation) {}
}
