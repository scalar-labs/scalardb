package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.KeyType;
import com.scalar.db.storage.jdbc.test.BaseStatements;
import com.scalar.db.storage.jdbc.test.JdbcConnectionInfo;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.scalar.db.storage.jdbc.test.BaseStatements.insertMetadataStatement;
import static com.scalar.db.storage.jdbc.test.TestEnv.MY_SQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.ORACLE_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRE_SQL_INFO;
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

  private static final Optional<String> NAMESPACE_PREFIX = Optional.empty();

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<JdbcConnectionInfo> jdbcConnectionInfos() {
    return Arrays.asList(MY_SQL_INFO, POSTGRE_SQL_INFO, ORACLE_INFO, SQL_SERVER_INFO);
  }

  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;

  private TestEnv testEnv;
  private DistributedStorage storage;
  private ConsensusCommitIntegrationTest test;

  @Before
  public void setUp() throws SQLException {
    testEnv =
        new TestEnv(
            jdbcConnectionInfo,
            new BaseStatements() {
              @Override
              public List<String> insertMetadataStatements(Optional<String> schemaPrefix) {
                List<String> ret = new ArrayList<>();

                // The metadata for the coordinator table
                ret.add(
                    insertMetadataStatement(
                        schemaPrefix,
                        Coordinator.NAMESPACE + "." + Coordinator.TABLE,
                        ID,
                        DataType.TEXT,
                        KeyType.PARTITION,
                        null,
                        false,
                        1));
                ret.add(
                    insertMetadataStatement(
                        schemaPrefix,
                        Coordinator.NAMESPACE + "." + Coordinator.TABLE,
                        STATE,
                        DataType.INT,
                        null,
                        null,
                        false,
                        2));
                ret.add(
                    insertMetadataStatement(
                        schemaPrefix,
                        Coordinator.NAMESPACE + "." + Coordinator.TABLE,
                        CREATED_AT,
                        DataType.BIGINT,
                        null,
                        null,
                        false,
                        3));

                // The metadata for the tables
                List<String> tables = Arrays.asList(TABLE_1, TABLE_2);
                for (String table : tables) {
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          ACCOUNT_ID,
                          DataType.INT,
                          KeyType.PARTITION,
                          null,
                          false,
                          1));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          ACCOUNT_TYPE,
                          DataType.INT,
                          KeyType.CLUSTERING,
                          Scan.Ordering.Order.ASC,
                          false,
                          2));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BALANCE,
                          DataType.INT,
                          null,
                          null,
                          false,
                          3));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          ID,
                          DataType.TEXT,
                          null,
                          null,
                          false,
                          4));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          VERSION,
                          DataType.INT,
                          null,
                          null,
                          false,
                          5));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          STATE,
                          DataType.INT,
                          null,
                          null,
                          false,
                          6));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          PREPARED_AT,
                          DataType.BIGINT,
                          null,
                          null,
                          false,
                          7));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          COMMITTED_AT,
                          DataType.BIGINT,
                          null,
                          null,
                          false,
                          8));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_PREFIX + BALANCE,
                          DataType.INT,
                          null,
                          null,
                          false,
                          9));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_ID,
                          DataType.TEXT,
                          null,
                          null,
                          false,
                          10));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_VERSION,
                          DataType.INT,
                          null,
                          null,
                          false,
                          11));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_STATE,
                          DataType.INT,
                          null,
                          null,
                          false,
                          12));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_PREPARED_AT,
                          DataType.BIGINT,
                          null,
                          null,
                          false,
                          13));
                  ret.add(
                      insertMetadataStatement(
                          schemaPrefix,
                          NAMESPACE + "." + table,
                          BEFORE_COMMITTED_AT,
                          DataType.BIGINT,
                          null,
                          null,
                          false,
                          14));
                }
                return ret;
              }

              @Override
              public List<String> schemas(Optional<String> schemaPrefix) {
                return Arrays.asList(Coordinator.NAMESPACE, NAMESPACE);
              }

              @Override
              public List<String> tables(Optional<String> schemaPrefix) {
                return Arrays.asList(
                    Coordinator.NAMESPACE + "." + Coordinator.TABLE,
                    NAMESPACE + "." + TABLE_1,
                    NAMESPACE + "." + TABLE_2);
              }

              @Override
              public List<String> createTableStatements(Optional<String> schemaPrefix) {
                return Arrays.asList(
                    createCoordinatorTableStatement(),
                    createTableStatement(TABLE_1),
                    createTableStatement(TABLE_2));
              }

              private String createCoordinatorTableStatement() {
                return "CREATE TABLE "
                    + Coordinator.NAMESPACE
                    + "."
                    + Coordinator.TABLE
                    + "("
                    + ID
                    + " VARCHAR(100),"
                    + STATE
                    + " INT,"
                    + CREATED_AT
                    + " BIGINT,"
                    + "PRIMARY KEY("
                    + ID
                    + "))";
              }

              private String createTableStatement(String table) {
                return "CREATE TABLE "
                    + NAMESPACE
                    + "."
                    + table
                    + "("
                    + ACCOUNT_ID
                    + " INT,"
                    + ACCOUNT_TYPE
                    + " INT,"
                    + BALANCE
                    + " INT,"
                    + ID
                    + " VARCHAR(100),"
                    + VERSION
                    + " INT,"
                    + STATE
                    + " INT,"
                    + PREPARED_AT
                    + " BIGINT,"
                    + COMMITTED_AT
                    + " BIGINT,"
                    + BEFORE_PREFIX
                    + BALANCE
                    + " INT,"
                    + BEFORE_ID
                    + " VARCHAR(100),"
                    + BEFORE_VERSION
                    + " INT,"
                    + BEFORE_STATE
                    + " INT,"
                    + BEFORE_PREPARED_AT
                    + " BIGINT,"
                    + BEFORE_COMMITTED_AT
                    + " BIGINT,"
                    + "PRIMARY KEY("
                    + ACCOUNT_ID
                    + ","
                    + ACCOUNT_TYPE
                    + "))";
              }
            },
            NAMESPACE_PREFIX);
    testEnv.createMetadataTableAndInsertMetadata();
    testEnv.createTables();

    DatabaseConfig config = testEnv.getDatabaseConfig();
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
    testEnv.dropAllTablesAndSchemas();
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
}
