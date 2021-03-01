package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.dynamo.Dynamo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.NAMESPACE;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_1;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTest.TABLE_2;
import static org.mockito.Mockito.spy;

public class ConsensusCommitWithDynamoIntegrationTest {
  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String PARTITION_KEY = "concatenatedPartitionKey";
  private static final String CLUSTERING_KEY = "concatenatedClusteringKey";

  private static DynamoDbClient client;
  private static Optional<String> namespacePrefix;
  private ConsensusCommitIntegrationTest test;

  @Before
  public void setUp() {
    DistributedStorage storage = spy(new Dynamo(client, namespacePrefix));
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
    truncateTable(NAMESPACE, TABLE_1);
    truncateTable(NAMESPACE, TABLE_2);
    truncateTable(Coordinator.NAMESPACE, Coordinator.TABLE);
  }

  private static void truncateTable(String namespace, String table) {
    ScanRequest request =
        ScanRequest.builder().tableName(table(namespace, table)).consistentRead(true).build();
    client
        .scan(request)
        .items()
        .forEach(
            i -> {
              Map<String, AttributeValue> key = new HashMap<>();
              key.put(PARTITION_KEY, i.get(PARTITION_KEY));
              key.put(CLUSTERING_KEY, i.get(CLUSTERING_KEY));
              DeleteItemRequest delRequest =
                  DeleteItemRequest.builder().tableName(table(namespace, table)).key(key).build();
              client.deleteItem(delRequest);
            });
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

  @BeforeClass
  public static void setUpBeforeClass() {
    String endpointOverride =
        System.getProperty("scalardb.dynamo.endpoint_override", "http://localhost:8000");
    String region = System.getProperty("scalardb.dynamo.region", "us-west-2");
    String accessKeyId = System.getProperty("scalardb.dynamo.access_key_id", "fakeMyKeyId");
    String secretAccessKey =
        System.getProperty("scalardb.dynamo.secret_access_key", "fakeSecretAccessKey");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    DynamoDbClientBuilder builder = DynamoDbClient.builder();

    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride));
    }

    client =
        builder
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();
    createMetadataTable();
    createCoordinatorTable();
    createUserTables();

    // wait for the creation
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    insertMetadata();
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String table(String database, String table) {
    return namespacePrefix() + database + "." + table;
  }

  @AfterClass
  public static void tearDownAfterClass() {
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      client.deleteTable(DeleteTableRequest.builder().tableName(table(NAMESPACE, table)).build());
    }

    client.deleteTable(
        DeleteTableRequest.builder()
            .tableName(table(Coordinator.NAMESPACE, Coordinator.TABLE))
            .build());

    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
  }

  private static void createCoordinatorTable() {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(Coordinator.NAMESPACE, Coordinator.TABLE));

    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(PARTITION_KEY)
            .attributeType(ScalarAttributeType.S)
            .build());
    builder.attributeDefinitions(attributeDefinitions);

    List<KeySchemaElement> keySchemaElements = new ArrayList<>();
    keySchemaElements.add(
        KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
    builder.keySchema(keySchemaElements);

    client.createTable(builder.build());
  }

  private static void createUserTables() {
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      CreateTableRequest.Builder builder =
          CreateTableRequest.builder()
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(10L)
                      .writeCapacityUnits(10L)
                      .build())
              .tableName(table(NAMESPACE, table));

      List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(PARTITION_KEY)
              .attributeType(ScalarAttributeType.S)
              .build());
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(CLUSTERING_KEY)
              .attributeType(ScalarAttributeType.S)
              .build());
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(ConsensusCommitIntegrationTest.ACCOUNT_TYPE)
              .attributeType(ScalarAttributeType.N)
              .build());
      builder.attributeDefinitions(attributeDefinitions);

      List<KeySchemaElement> keySchemaElements = new ArrayList<>();
      keySchemaElements.add(
          KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
      keySchemaElements.add(
          KeySchemaElement.builder().attributeName(CLUSTERING_KEY).keyType(KeyType.RANGE).build());
      builder.keySchema(keySchemaElements);

      List<KeySchemaElement> indexKeys = new ArrayList<>();
      indexKeys.add(
          KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build());
      indexKeys.add(
          KeySchemaElement.builder()
              .attributeName(ConsensusCommitIntegrationTest.ACCOUNT_TYPE)
              .keyType(KeyType.RANGE)
              .build());
      LocalSecondaryIndex index =
          LocalSecondaryIndex.builder()
              .indexName(
                  NAMESPACE + "." + table + ".index." + ConsensusCommitIntegrationTest.ACCOUNT_TYPE)
              .keySchema(indexKeys)
              .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
              .build();
      builder.localSecondaryIndexes(index);

      client.createTable(builder.build());
    }
  }

  private static void createMetadataTable() {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("table")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder().attributeName("table").keyType(KeyType.HASH).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(METADATA_DATABASE, METADATA_TABLE))
            .build();

    client.createTable(request);
  }

  private static void insertMetadata() {
    final String metadataTable = table(METADATA_DATABASE, METADATA_TABLE);

    // For the coordinator table
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(
        "table",
        AttributeValue.builder().s(table(Coordinator.NAMESPACE, Coordinator.TABLE)).build());
    values.put(
        "partitionKey",
        AttributeValue.builder().ss(Collections.singletonList(Attribute.ID)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(Attribute.ID, AttributeValue.builder().s("text").build());
    columns.put(Attribute.STATE, AttributeValue.builder().s("int").build());
    columns.put(Attribute.CREATED_AT, AttributeValue.builder().s("bigint").build());
    values.put("columns", AttributeValue.builder().m(columns).build());

    client.putItem(PutItemRequest.builder().tableName(metadataTable).item(values).build());

    // For the user tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      values = new HashMap<>();
      values.put("table", AttributeValue.builder().s(table(NAMESPACE, table)).build());
      values.put(
          "partitionKey",
          AttributeValue.builder()
              .ss(Collections.singletonList(ConsensusCommitIntegrationTest.ACCOUNT_ID))
              .build());
      values.put(
          "clusteringKey",
          AttributeValue.builder()
              .ss(Collections.singletonList(ConsensusCommitIntegrationTest.ACCOUNT_TYPE))
              .build());
      columns = new HashMap<>();
      columns.put(
          ConsensusCommitIntegrationTest.ACCOUNT_ID, AttributeValue.builder().s("int").build());
      columns.put(
          ConsensusCommitIntegrationTest.ACCOUNT_TYPE, AttributeValue.builder().s("int").build());
      columns.put(
          ConsensusCommitIntegrationTest.BALANCE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.ID, AttributeValue.builder().s("text").build());
      columns.put(Attribute.VERSION, AttributeValue.builder().s("int").build());
      columns.put(Attribute.STATE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.PREPARED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(Attribute.COMMITTED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(
          Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTest.BALANCE,
          AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_ID, AttributeValue.builder().s("text").build());
      columns.put(Attribute.BEFORE_VERSION, AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_STATE, AttributeValue.builder().s("int").build());
      columns.put(Attribute.BEFORE_PREPARED_AT, AttributeValue.builder().s("bigint").build());
      columns.put(Attribute.BEFORE_COMMITTED_AT, AttributeValue.builder().s("bigint").build());
      values.put("columns", AttributeValue.builder().m(columns).build());

      client.putItem(PutItemRequest.builder().tableName(metadataTable).item(values).build());
    }
  }
}
