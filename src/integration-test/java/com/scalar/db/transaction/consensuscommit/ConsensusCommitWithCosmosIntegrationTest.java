package com.scalar.db.transaction.consensuscommit;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosTableMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.NAMESPACE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_2;
import static org.mockito.Mockito.spy;

public class ConsensusCommitWithCosmosIntegrationTest {
  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String STORED_PROCEDURE_PATH =
      "tools/scalar-schema/stored_procedure/mutate.js";
  private static final String PARTITION_KEY = "/concatenatedPartitionKey";

  private static Optional<String> namespacePrefix;
  private static CosmosClient client;
  private static DistributedStorage originalStorage;

  private ConsensusCommitIntegrationTest test;

  @Before
  public void setUp() throws IOException {
    String storedProcedure =
        Files.lines(Paths.get(STORED_PROCEDURE_PATH), StandardCharsets.UTF_8)
            .reduce("", (prev, cur) -> prev + cur + System.getProperty("line.separator"));
    CosmosStoredProcedureProperties properties =
        new CosmosStoredProcedureProperties("mutate.js", storedProcedure);

    // create the coordinator table
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(Coordinator.TABLE, PARTITION_KEY);
    client
        .getDatabase(database(Coordinator.NAMESPACE))
        .createContainerIfNotExists(containerProperties);
    client
        .getDatabase(database(Coordinator.NAMESPACE))
        .getContainer(Coordinator.TABLE)
        .getScripts()
        .createStoredProcedure(properties, new CosmosStoredProcedureRequestOptions());

    // create the user table
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      containerProperties = new CosmosContainerProperties(table, PARTITION_KEY);
      client.getDatabase(database(NAMESPACE)).createContainerIfNotExists(containerProperties);
      client
          .getDatabase(database(NAMESPACE))
          .getContainer(table)
          .getScripts()
          .createStoredProcedure(properties, new CosmosStoredProcedureRequestOptions());
    }

    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "dummy");
    props.setProperty(DatabaseConfig.USERNAME, "dummy");
    props.setProperty(DatabaseConfig.PASSWORD, "dummy");
    ConsensusCommitManager manager =
        new ConsensusCommitManager(
            storage, new DatabaseConfig(props), coordinator, recovery, commit);

    test = new ConsensusCommitIntegrationTest(manager, storage, coordinator, recovery);
    test.setUp();
  }

  @After
  public void tearDown() {
    // delete the tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      client.getDatabase(database(NAMESPACE)).getContainer(table).delete();
    }

    client.getDatabase(database(Coordinator.NAMESPACE)).getContainer(Coordinator.TABLE).delete();
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

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldThrowCrudRuntimeException() throws Exception {
    test.scan_OverlappingPutGivenBefore_ShouldThrowCrudRuntimeException();
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan() throws Exception {
    test.scan_NonOverlappingPutGivenBefore_ShouldScan();
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String database(String database) {
    return namespacePrefix() + database;
  }

  private static String table(String database, String table) {
    return database(database) + "." + table;
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    String contactPoint = System.getProperty("scalardb.cosmos.uri");
    String username = System.getProperty("scalardb.cosmos.username");
    String password = System.getProperty("scalardb.cosmos.password");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    client =
        new CosmosClientBuilder().endpoint(contactPoint).key(password).directMode().buildClient();

    // create a database and a table for metadata
    client.createDatabaseIfNotExists(database(METADATA_DATABASE));
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_TABLE, "/id");
    client.getDatabase(database(METADATA_DATABASE)).createContainerIfNotExists(containerProperties);

    // create a database for the coordinator table
    client.createDatabaseIfNotExists(database(Coordinator.NAMESPACE));

    // create a database for the user tables
    client.createDatabaseIfNotExists(database(NAMESPACE));

    // insert metadata for the coordinator table
    CosmosTableMetadata metadata = new CosmosTableMetadata();
    metadata.setId(table(Coordinator.NAMESPACE, Coordinator.TABLE));
    metadata.setPartitionKeyNames(new HashSet<>(Collections.singletonList(Attribute.ID)));
    metadata.setClusteringKeyNames(Collections.emptySet());
    metadata.setSecondaryIndexNames(Collections.emptySet());
    Map<String, String> columns = new HashMap<>();
    columns.put(Attribute.ID, "text");
    columns.put(Attribute.STATE, "int");
    columns.put(Attribute.CREATED_AT, "bigint");
    metadata.setColumns(columns);
    client
        .getDatabase(database(METADATA_DATABASE))
        .getContainer(METADATA_TABLE)
        .createItem(metadata);

    // insert metadata for the user tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      metadata = new CosmosTableMetadata();
      metadata.setId(table(NAMESPACE, table));
      metadata.setPartitionKeyNames(
          new HashSet<>(Collections.singletonList(ConsensusCommitIntegrationTest.ACCOUNT_ID)));
      metadata.setClusteringKeyNames(
          new HashSet<>(Collections.singletonList(ConsensusCommitIntegrationTest.ACCOUNT_TYPE)));
      metadata.setSecondaryIndexNames(Collections.emptySet());
      columns = new HashMap<>();
      columns.put(ConsensusCommitIntegrationTest.ACCOUNT_ID, "int");
      columns.put(ConsensusCommitIntegrationTest.ACCOUNT_TYPE, "int");
      columns.put(ConsensusCommitIntegrationTest.BALANCE, "int");
      columns.put(Attribute.ID, "text");
      columns.put(Attribute.VERSION, "int");
      columns.put(Attribute.STATE, "int");
      columns.put(Attribute.PREPARED_AT, "bigint");
      columns.put(Attribute.COMMITTED_AT, "bigint");
      columns.put(Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTest.BALANCE, "int");
      columns.put(Attribute.BEFORE_ID, "text");
      columns.put(Attribute.BEFORE_VERSION, "int");
      columns.put(Attribute.BEFORE_STATE, "int");
      columns.put(Attribute.BEFORE_PREPARED_AT, "bigint");
      columns.put(Attribute.BEFORE_COMMITTED_AT, "bigint");
      metadata.setColumns(columns);
      client
          .getDatabase(database(METADATA_DATABASE))
          .getContainer(METADATA_TABLE)
          .createItem(metadata);
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    DatabaseConfig config = new DatabaseConfig(props);
    originalStorage = new Cosmos(config);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    CosmosDatabase database = client.getDatabase(database(METADATA_DATABASE));
    database.getContainer(METADATA_TABLE).delete();
    database.delete();

    client.getDatabase(database(Coordinator.NAMESPACE)).delete();

    client.getDatabase(database(NAMESPACE)).delete();

    client.close();

    originalStorage.close();
  }
}
