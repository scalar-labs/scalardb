package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.test.JdbcConnectionInfo;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.scalar.db.storage.jdbc.test.TestEnv.MYSQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.ORACLE_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRESQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.SQL_SERVER_INFO;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.CREATED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.ACCOUNT_ID;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.ACCOUNT_TYPE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.BALANCE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.NAMESPACE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_2;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class ConsensusCommitWithJdbcDatabaseIntegrationTest {

  private TestEnv testEnv;
  private DistributedStorage storage;
  private ConsensusCommitIntegrationTest test;

  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<JdbcConnectionInfo> jdbcConnectionInfos() {
    return Arrays.asList(MYSQL_INFO, POSTGRESQL_INFO, ORACLE_INFO, SQL_SERVER_INFO);
  }

  @Before
  public void setUp() throws SQLException {
    testEnv = new TestEnv(jdbcConnectionInfo, Optional.empty());

    // For the coordinator table
    testEnv.register(
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        new LinkedHashMap<String, DataType>() {
          {
            put(ID, DataType.TEXT);
            put(STATE, DataType.INT);
            put(CREATED_AT, DataType.BIGINT);
          }
        },
        Collections.singletonList(ID),
        Collections.emptyList(),
        new HashMap<String, Scan.Ordering.Order>() {});

    // For the test tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      testEnv.register(
          NAMESPACE,
          table,
          new LinkedHashMap<String, DataType>() {
            {
              put(ACCOUNT_ID, DataType.INT);
              put(ACCOUNT_TYPE, DataType.INT);
              put(BALANCE, DataType.INT);
              put(ID, DataType.TEXT);
              put(STATE, DataType.INT);
              put(VERSION, DataType.INT);
              put(PREPARED_AT, DataType.BIGINT);
              put(COMMITTED_AT, DataType.BIGINT);
              put(BEFORE_PREFIX + BALANCE, DataType.INT);
              put(BEFORE_ID, DataType.TEXT);
              put(BEFORE_STATE, DataType.INT);
              put(BEFORE_VERSION, DataType.INT);
              put(BEFORE_PREPARED_AT, DataType.BIGINT);
              put(BEFORE_COMMITTED_AT, DataType.BIGINT);
            }
          },
          Collections.singletonList(ACCOUNT_ID),
          Collections.singletonList(ACCOUNT_TYPE),
          new HashMap<String, Scan.Ordering.Order>() {
            {
              put(ACCOUNT_TYPE, Scan.Ordering.Order.ASC);
            }
          });
    }

    testEnv.createTables();

    JdbcDatabaseConfig config = testEnv.getJdbcDatabaseConfig();
    storage = spy(new JdbcDatabase(config));
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, config, coordinator, recovery, commit);

    test = new ConsensusCommitIntegrationTest(manager, storage, coordinator, recovery);
    test.setUp();
  }

  @After
  public void tearDown() throws Exception {
    storage.close();

    testEnv.dropTables();
    testEnv.close();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws Exception {
    test.get_GetGivenForCommittedRecord_ShouldReturnRecord();
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws Exception {
    test.scan_ScanGivenForCommittedRecord_ShouldReturnRecord();
  }

  @Test
  public void get_CalledTwice_ShouldReturnFromSnapshotInSecondTime() throws Exception {
    test.get_CalledTwice_ShouldReturnFromSnapshotInSecondTime();
  }

  @Test
  public void
      get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime()
          throws Exception {
    test
        .get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime();
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws Exception {
    test.get_GetGivenForNonExisting_ShouldReturnEmpty();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws Exception {
    test.scan_ScanGivenForNonExisting_ShouldReturnEmpty();
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    test.get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward();
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    test.scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward();
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback() throws Exception {
    test.get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback();
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws Exception {
    test.scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    test
        .get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction();
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    test
        .scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction();
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws Exception {
    test.get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction();
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws Exception {
    test.scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws Exception {
    test
        .get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly();
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws Exception {
    test
        .scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws Exception {
    test
        .get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly();
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws Exception {
    test
        .scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly();
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    test.get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward();
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws Exception {
    test.scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward();
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback() throws Exception {
    test.get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback();
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws Exception {
    test.scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    test
        .get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction();
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws Exception {
    test
        .scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction();
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws Exception {
    test.get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction();
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws Exception {
    test.scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws Exception {
    test
        .get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly();
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly()
          throws Exception {
    test
        .scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRollforwardedByAnother_ShouldRollforwardProperly();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws Exception {
    test
        .get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly();
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly()
          throws Exception {
    test
        .scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRollbackedByAnother_ShouldRollbackProperly();
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably() throws Exception {
    test.getAndScan_CommitHappenedInBetween_ShouldReadRepeatably();
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws Exception {
    test.putAndCommit_PutGivenForNonExisting_ShouldCreateRecord();
  }

  @Test
  public void put_PutGivenForExistingAfterRead_ShouldUpdateRecord() throws Exception {
    test.putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord();
  }

  @Test
  public void put_PutGivenForExistingAndNeverRead_ShouldFailWithException() throws Exception {
    test.putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowCommitException();
  }

  @Test
  public void put_SinglePartitionMutationsGiven_ShouldAccessStorageOnceForPrepareAndCommit()
      throws Exception {
    test.putAndCommit_SinglePartitionMutationsGiven_ShouldAccessStorageOnceForPrepareAndCommit();
  }

  @Test
  public void put_TwoPartitionsMutationsGiven_ShouldAccessStorageTwiceForPrepareAndCommit()
      throws Exception {
    test.putAndCommit_TwoPartitionsMutationsGiven_ShouldAccessStorageTwiceForPrepareAndCommit();
  }

  @Test
  public void putAndCommit_MultipleGetAndPutGiven_ShouldCommitProperly() throws Exception {
    test.putAndCommit_GetsAndPutsGiven_ShouldCommitProperly();
  }

  @Test
  public void commit_ConflictingPutsGivenForNonExisting_ShouldCommitEither() throws Exception {
    test.commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther();
  }

  @Test
  public void commit_ConflictingPutsGivenForExisting_ShouldCommitEither() throws Exception {
    test.commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther();
  }

  @Test
  public void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete()
      throws Exception {
    test.commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete();
  }

  @Test
  public void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth() throws Exception {
    test.commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth();
  }

  @Test
  public void commit_DeleteGivenWithoutRead_ShouldThrowInvalidUsageException() {
    test.commit_DeleteGivenWithoutRead_ShouldThrowInvalidUsageException();
  }

  @Test
  public void commit_DeleteGivenForNonExisting_ShouldThrowInvalidUsageException() throws Exception {
    test.commit_DeleteGivenForNonExisting_ShouldThrowInvalidUsageException();
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord() throws Exception {
    test.commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord();
  }

  @Test
  public void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws Exception {
    test.commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther();
  }

  @Test
  public void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth() throws Exception {
    test.commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth();
  }

  @Test
  public void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult()
      throws Exception {
    test.commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws Exception {
    test
        .commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitConflictException();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws Exception {
    test
        .commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitConflictException();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException()
          throws Exception {
    test
        .commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowCommitException();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws Exception {
    test
        .commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException()
          throws Exception {
    test
        .commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowCommitException();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowCommitException()
          throws Exception {
    test
        .commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowCommitException();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitException()
          throws Exception {
    test
        .commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitException();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitException()
          throws Exception {
    test
        .commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowCommitException();
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws Exception {
    test.putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults();
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws Exception {
    test.deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults();
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty() throws Exception {
    test.get_DeleteCalledBefore_ShouldReturnEmpty();
  }

  @Test
  public void scan_DeleteCalledBefore_ShouldReturnEmpty() throws Exception {
    test.scan_DeleteCalledBefore_ShouldReturnEmpty();
  }

  @Test
  public void delete_PutCalledBefore_ShouldDelete() throws Exception {
    test.delete_PutCalledBefore_ShouldDelete();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldPut() throws Exception {
    test.put_DeleteCalledBefore_ShouldPut();
  }
}
