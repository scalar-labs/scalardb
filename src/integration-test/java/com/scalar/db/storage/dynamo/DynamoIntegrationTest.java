package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
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
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DynamoIntegrationTest {
  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String DATABASE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String PARTITION_KEY = "concatenatedPartitionKey";
  private static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static Optional<String> namespacePrefix;
  private static DynamoDbClient client;
  private static DistributedStorage storage;
  private List<Put> puts;
  private List<Delete> deletes;

  @Before
  public void setUp() throws Exception {
    storage.with(DATABASE, TABLE);
  }

  @After
  public void tearDown() {
    // truncate the TABLE
    ScanRequest request =
        ScanRequest.builder().tableName(table(DATABASE, TABLE)).consistentRead(true).build();
    client
        .scan(request)
        .items()
        .forEach(
            i -> {
              Map<String, AttributeValue> key = new HashMap<>();
              key.put(PARTITION_KEY, i.get(PARTITION_KEY));
              key.put(CLUSTERING_KEY, i.get(CLUSTERING_KEY));
              DeleteItemRequest delRequest =
                  DeleteItemRequest.builder().tableName(table(DATABASE, TABLE)).key(key).build();
              client.deleteItem(delRequest);
            });
  }

  @Test
  public void operation_NoTargetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(null, TABLE);
    Key partitionKey = new Key(new IntValue(COL_NAME1, 0));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 0));
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(
            () -> {
              storage.get(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetWithPartitionKeyAndClusteringKeyGiven_ShouldRetrieveSingleResult()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Get get = prepareGet(pKey, 0);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void get_GetWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Get get = prepareGet(pKey, cKey);
    get.withProjection(COL_NAME1).withProjection(COL_NAME2).withProjection(COL_NAME3);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue(); // since it's clustering key
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isFalse();
  }

  @Test
  public void get_GetGivenForIndexedColumn_ShouldGet() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    int c3 = 3;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)));

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetGivenForIndexedColumnWithClusteringKey_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    int c3 = 0;
    int c4 = 0;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)), new Key((new IntValue(COL_NAME4, c4))));

    // Act
    assertThatCode(
            () -> {
              storage.get(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanWithPartitionKeyGiven_ShouldRetrieveMultipleResults()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    List<Result> actual = new ArrayList<>();
    storage.scan(scan).forEach(r -> actual.add(r)); // use iterator

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    Scanner scanner = storage.scan(scan);

    // Assert
    Optional<Result> result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    result = scanner.one();
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanWithPartitionGivenThreeTimes_ShouldRetrieveResultsProperlyEveryTime()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    double t1 = System.currentTimeMillis();
    List<Result> actual = storage.scan(scan).all();
    double t2 = System.currentTimeMillis();
    storage.scan(scan);
    double t3 = System.currentTimeMillis();
    storage.scan(scan);
    double t4 = System.currentTimeMillis();

    // Assert
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    System.err.println("first: " + (t2 - t1) + " (ms)");
    System.err.println("second: " + (t3 - t2) + " (ms)");
    System.err.println("third: " + (t4 - t3) + " (ms)");
  }

  @Test
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, pKey)))
            .withStart(new Key(new IntValue(COL_NAME4, 0)), true)
            .withEnd(new Key(new IntValue(COL_NAME4, 1)), true);
    List<Result> actual = storage.scan(scan).all();

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults()
      throws ExecutionException {
    // Arrange
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.ASC));

    // Act
    List<Result> actual = storage.scan(scan).all();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithOrderDescGiven_ShouldReturnDescendingOrderedResults()
      throws ExecutionException {
    // Arrange
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC));

    // Act
    List<Result> actual = storage.scan(scan).all();

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void scan_ScanWithLimitGiven_ShouldReturnGivenNumberOfResults() throws ExecutionException {
    // setup
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC))
            .withLimit(1);

    // exercise
    List<Result> actual = storage.scan(scan).all();

    // verify
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws ExecutionException {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan = new Scan(new Key(new IntValue(COL_NAME3, c3)));

    // Act
    List<Result> actual = storage.scan(scan).all();

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (3,0), (2,1), (1,2)
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 3)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanGivenForIndexedColumnWithOrdering_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    int c3 = 0;
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME3, c3)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.ASC));

    // Act
    assertThatThrownBy(() -> storage.scan(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    String c2 = "test";
    Scan scan = new Scan(new Key(new TextValue(COL_NAME2, c2)));

    // Act Assert
    assertThatThrownBy(() -> storage.scan(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(pKey * 2 + cKey));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, (cKey % 2 == 0) ? true : false)));
  }

  @Test
  public void put_SinglePutWithIfNotExistsGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(0));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, (cKey % 2 == 0) ? true : false)));
  }

  @Test
  public void put_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(scan).all();
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(scan).all();
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGivenWhenOneExists_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    List<Result> results = storage.scan(scan).all();
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
  }

  @Test
  public void
      put_MultiplePutWithDifferentPartitionsWithIfNotExistsGiven_ShouldThrowMultiPartitionException()
          throws ExecutionException {
    // Arrange
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(3).withCondition(new PutIfNotExists());
    puts.get(6).withCondition(new PutIfNotExists());

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 0)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 3)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 6)))).all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentPartitionsGiven_ShouldThrowMultiPartitionException()
      throws ExecutionException {
    // Arrange
    puts = preparePuts();

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 0)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 3)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 6)))).all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly()
      throws ExecutionException {
    // Arrange
    puts = preparePuts();
    storage.put(puts.get(1));
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1)
        .withCondition(
            new PutIf(new ConditionalExpression(COL_NAME2, new TextValue("1"), Operator.EQ)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 0)))).all();
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
  }

  @Test
  public void put_PutWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfExists());
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void put_PutWithIfExistsGivenWhenSuchRecordExists_ShouldUpdateRecord()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0).withCondition(new PutIfExists());
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
  }

  @Test
  public void put_PutWithIfGivenWhenSuchRecordExists_ShouldUpdateRecord()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(COL_NAME3, new IntValue(pKey + cKey), Operator.EQ)));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
  }

  @Test
  public void put_PutWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(COL_NAME3, new IntValue(pKey + cKey + 1), Operator.EQ)));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
  }

  @Test
  public void delete_DeleteWithPartitionKeyGiven_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Delete delete = new Delete(partitionKey);
    assertThatThrownBy(
            () -> {
              storage.delete(delete);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(new Scan(partitionKey)).all();
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey + 1)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey + 2)));
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Delete delete = prepareDelete(pKey, Integer.MAX_VALUE);
    delete.withCondition(new DeleteIfExists());
    assertThatThrownBy(
            () -> {
              storage.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(new DeleteIfExists());
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2, new TextValue(Integer.toString(Integer.MAX_VALUE)), Operator.EQ)));
    assertThatThrownBy(
            () -> {
              storage.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isTrue();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2, new TextValue(Integer.toString(pKey)), Operator.EQ)));
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_MultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    puts = preparePuts();
    deletes = prepareDeletes();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    deletes.get(0).withCondition(new DeleteIfExists());
    deletes
        .get(1)
        .withCondition(
            new DeleteIf(new ConditionalExpression(COL_NAME2, new TextValue("1"), Operator.EQ)));

    // Act
    assertThatCode(
            () -> {
              storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 0)))).all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(scan).all();
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void mutate_MultiplePutWithDifferentPartitionsGiven_ShouldThrowMultiPartitionException()
      throws Exception {
    // Arrange
    puts = preparePuts();

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 0)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 3)))).all();
    assertThat(results.size()).isEqualTo(0);
    results = storage.scan(new Scan(new Key(new IntValue(COL_NAME1, 6)))).all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldUpdateAndDeleteRecordsProperly() throws Exception {
    // Arrange
    populateRecords();
    puts = preparePuts();
    puts.get(1).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    puts.get(2).withValue(new IntValue(COL_NAME3, Integer.MIN_VALUE));

    int pKey = 0;
    int cKey = 0;
    Delete delete = prepareDelete(pKey, cKey);

    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(delete, puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = storage.scan(scan).all();
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
    assertThat(results.get(1).getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MIN_VALUE)));
  }

  @Test
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.mutate(Arrays.asList(puts.get(pKey * 2 + cKey)));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, (cKey % 2 == 0) ? true : false)));
  }

  @Test
  public void mutate_SingleDeleteGiven_ShouldThrowIllegalArgumentException() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Delete delete = new Delete(partitionKey);
    assertThatThrownBy(
            () -> {
              storage.mutate(Arrays.asList(delete));
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void mutate_EmptyListGiven_ShouldThrowIllegalArgumentException() throws Exception {
    // Act
    assertThatCode(
            () -> {
              storage.mutate(new ArrayList<>());
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_PutWithoutClusteringKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Put put = new Put(partitionKey);

    // Act Assert
    assertThatCode(
            () -> {
              storage.put(put);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_IncorrectPutGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Put put = new Put(partitionKey).withValue(new IntValue(COL_NAME4, cKey));

    // Act Assert
    assertThatCode(
            () -> {
              storage.put(put);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void populateRecords() {
    puts = preparePuts();
    puts.forEach(
        p -> {
          assertThatCode(
                  () -> {
                    storage.put(p);
                  })
              .doesNotThrowAnyException();
        });
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    return new Get(partitionKey, clusteringKey);
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              IntStream.range(0, 3)
                  .forEach(
                      j -> {
                        Key partitionKey = new Key(new IntValue(COL_NAME1, i));
                        Key clusteringKey = new Key(new IntValue(COL_NAME4, j));
                        Put put =
                            new Put(partitionKey, clusteringKey)
                                .withValue(new TextValue(COL_NAME2, Integer.toString(i + j)))
                                .withValue(new IntValue(COL_NAME3, i + j))
                                .withValue(
                                    new BooleanValue(COL_NAME5, (j % 2 == 0) ? true : false));
                        puts.add(put);
                      });
            });

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    return new Delete(partitionKey, clusteringKey);
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              IntStream.range(0, 3)
                  .forEach(
                      j -> {
                        Key partitionKey = new Key(new IntValue(COL_NAME1, i));
                        Key clusteringKey = new Key(new IntValue(COL_NAME4, j));
                        Delete delete = new Delete(partitionKey, clusteringKey);
                        deletes.add(delete);
                      });
            });

    return deletes;
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
    createUserTable();

    // wait for the creation
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    insertMetadata();

    storage = new Dynamo(client, namespacePrefix);
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String table(String database, String table) {
    return namespacePrefix() + database + "." + table;
  }

  @AfterClass
  public static void tearDownAfterClass() {
    client.deleteTable(DeleteTableRequest.builder().tableName(table(DATABASE, TABLE)).build());

    client.deleteTable(
        DeleteTableRequest.builder().tableName(table(METADATA_DATABASE, METADATA_TABLE)).build());

    client.close();
  }

  private static void createUserTable() {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .tableName(table(DATABASE, TABLE));

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
            .attributeName(COL_NAME4)
            .attributeType(ScalarAttributeType.N)
            .build());
    attributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(COL_NAME3)
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
        KeySchemaElement.builder().attributeName(COL_NAME4).keyType(KeyType.RANGE).build());
    LocalSecondaryIndex index =
        LocalSecondaryIndex.builder()
            .indexName(DATABASE + "." + TABLE + ".index." + COL_NAME4)
            .keySchema(indexKeys)
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build();
    builder.localSecondaryIndexes(index);

    List<KeySchemaElement> globalIndexKeys = new ArrayList<>();
    globalIndexKeys.add(
        KeySchemaElement.builder().attributeName(COL_NAME3).keyType(KeyType.HASH).build());
    GlobalSecondaryIndex globalIndex =
        GlobalSecondaryIndex.builder()
            .indexName(DATABASE + "." + TABLE + ".global_index." + COL_NAME3)
            .keySchema(globalIndexKeys)
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .build();
    builder.globalSecondaryIndexes(globalIndex);

    client.createTable(builder.build());
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
    Map<String, AttributeValue> values = new HashMap<>();
    values.put("table", AttributeValue.builder().s(table(DATABASE, TABLE)).build());
    values.put(
        "partitionKey",
        AttributeValue.builder().l(AttributeValue.builder().s(COL_NAME1).build()).build());
    values.put(
        "clusteringKey",
        AttributeValue.builder().l(AttributeValue.builder().s(COL_NAME4).build()).build());
    values.put("secondaryIndex", AttributeValue.builder().ss(COL_NAME3).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(COL_NAME1, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME2, AttributeValue.builder().s("text").build());
    columns.put(COL_NAME3, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME4, AttributeValue.builder().s("int").build());
    columns.put(COL_NAME5, AttributeValue.builder().s("boolean").build());
    values.put("columns", AttributeValue.builder().m(columns).build());

    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(table(METADATA_DATABASE, METADATA_TABLE))
            .item(values)
            .build();

    client.putItem(request);
  }
}
