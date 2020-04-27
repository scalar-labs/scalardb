package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.base.Joiner;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConsensusCommitWithCassandraIntegrationTest {
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CONTACT_POINT = "localhost";
  private DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;
  private CommitHandler commit;
  private ConsensusCommitManager manager;
  private ConsensusCommitIntegrationTest test;

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    storage = spy(new Cassandra(new DatabaseConfig(props)));
    coordinator = spy(new Coordinator(storage));
    recovery = spy(new RecoveryHandler(storage, coordinator));
    commit = spy(new CommitHandler(storage, coordinator, recovery));
    manager = new ConsensusCommitManager(storage, coordinator, recovery, commit);

    test = new ConsensusCommitIntegrationTest(manager, storage, coordinator, recovery);
    test.setUp();
  }

  @After
  public void tearDown() throws IOException {
    executeStatement(
        truncateTableStatement(
            ConsensusCommitIntegrationTest.NAMESPACE, ConsensusCommitIntegrationTest.TABLE_1));
    executeStatement(
        truncateTableStatement(
            ConsensusCommitIntegrationTest.NAMESPACE, ConsensusCommitIntegrationTest.TABLE_2));
    executeStatement(truncateTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
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
      commit_WriteSkewOnExistingRecordsWithSerializable_OneShouldCommitTheOtherShouldThrowCommitConflictException()
          throws Exception {
    test
        .commit_WriteSkewOnExistingRecordsWithSerializable_OneShouldCommitTheOtherShouldThrowCommitConflictException();
  }

  @Test
  public void commit_WriteSkewOnNonExistingRecordsWithSerializable_ShouldThrowCommitException()
      throws Exception {
    test.commit_WriteSkewOnNonExistingRecordsWithSerializable_ShouldThrowCommitException();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializable_ShouldThrowCommitException()
          throws Exception {
    test.commit_WriteSkewWithScanOnNonExistingRecordsWithSerializable_ShouldThrowCommitException();
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

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, InterruptedException {
    executeStatement(createNamespaceStatement(ConsensusCommitIntegrationTest.NAMESPACE));
    System.out.println(createNamespaceStatement(ConsensusCommitIntegrationTest.NAMESPACE));
    executeStatement(
        createTableStatement(
            ConsensusCommitIntegrationTest.NAMESPACE, ConsensusCommitIntegrationTest.TABLE_1));
    System.out.println(
        createTableStatement(
            ConsensusCommitIntegrationTest.NAMESPACE, ConsensusCommitIntegrationTest.TABLE_1));
    executeStatement(
        createTableStatement(
            ConsensusCommitIntegrationTest.NAMESPACE, ConsensusCommitIntegrationTest.TABLE_2));
    executeStatement(createNamespaceStatement(Coordinator.NAMESPACE));
    System.out.println(createNamespaceStatement(Coordinator.NAMESPACE));
    executeStatement(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
    System.out.println(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));

    // it's supposed to be unnecessary, but just in case schema agreement takes some time
    Thread.sleep(1000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    executeStatement(dropNamespaceStatement(ConsensusCommitIntegrationTest.NAMESPACE));
    executeStatement(dropNamespaceStatement(Coordinator.NAMESPACE));
  }

  private static String createNamespaceStatement(String namespace) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE KEYSPACE",
              namespace,
              "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }",
            });
  }

  private static String dropNamespaceStatement(String namespace) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "DROP KEYSPACE", namespace,
            });
  }

  private static String createTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE",
              namespace + "." + table,
              "(",
              ConsensusCommitIntegrationTest.ACCOUNT_ID,
              "int,",
              ConsensusCommitIntegrationTest.ACCOUNT_TYPE,
              "int,",
              ConsensusCommitIntegrationTest.BALANCE,
              "int,",
              Attribute.ID,
              "text,",
              Attribute.VERSION,
              "int,",
              Attribute.STATE,
              "int,",
              Attribute.PREPARED_AT,
              "bigint,",
              Attribute.COMMITTED_AT,
              "bigint,",
              Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTest.BALANCE,
              "int,",
              Attribute.BEFORE_ID,
              "text,",
              Attribute.BEFORE_VERSION,
              "int,",
              Attribute.BEFORE_STATE,
              "int,",
              Attribute.BEFORE_PREPARED_AT,
              "bigint,",
              Attribute.BEFORE_COMMITTED_AT,
              "bigint,",
              "PRIMARY KEY",
              "((" + ConsensusCommitIntegrationTest.ACCOUNT_ID + "),",
              ConsensusCommitIntegrationTest.ACCOUNT_TYPE + ")",
              ")",
            });
  }

  private static String createCoordinatorTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE",
              namespace + "." + table,
              "(",
              Attribute.ID,
              "text,",
              Attribute.STATE,
              "int,",
              Attribute.CREATED_AT,
              "bigint,",
              "PRIMARY KEY",
              "(" + Attribute.ID + ")",
              ")",
            });
  }

  private static String truncateTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "TRUNCATE", namespace + "." + table,
            });
  }

  private static void executeStatement(String statement) throws IOException {
    ProcessBuilder builder =
        new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", statement);
    builder.redirectErrorStream(true);

    BufferedReader reader = null;
    try {
      Process process = builder.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;
        System.out.println(line);
      }

      int ret = process.waitFor();
      if (ret != 0) {
        Assert.fail("failed to execute " + statement);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
