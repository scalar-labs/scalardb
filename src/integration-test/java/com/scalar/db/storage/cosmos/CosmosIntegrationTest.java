package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.InvalidUsageException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CosmosIntegrationTest {
  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";
  private static final String DATABASE = "integration_testing";
  private static final String CONTAINER = "test_table";
  private static final String STORED_PROCEDURE_PATH =
      "tools/scalar-schema/stored_procedure/mutate.js";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String PARTITION_KEY = "/concatenatedPartitionKey";
  private static Optional<String> namespacePrefix;
  private static CosmosClient client;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    storage.with(DATABASE, CONTAINER);
  }

  @After
  public void tearDown() {
    // delete all the data of the container
    CosmosPagedIterable<Record> records =
        client
            .getDatabase(database(DATABASE))
            .getContainer(CONTAINER)
            .queryItems("SELECT * FROM Record", new CosmosQueryRequestOptions(), Record.class);
    for (Record record : records) {
      client
          .getDatabase(database(DATABASE))
          .getContainer(CONTAINER)
          .deleteItem(record, new CosmosItemRequestOptions());
    }
  }

  @Test
  public void operation_NoTargetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(null, CONTAINER);
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
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowInvalidUsageException() {
    // Arrange
    populateRecords();
    int c3 = 3;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)));

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(InvalidUsageException.class);
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
            .withEnd(new Key(new IntValue(COL_NAME4, 2)), false);
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
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, pKey)))
            .withStart(new Key(new IntValue(COL_NAME4, 0)), false)
            .withEnd(new Key(new IntValue(COL_NAME4, 2)), true);
    List<Result> actual = storage.scan(scan).all();

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults()
      throws ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.put(puts.get(0));
    storage.put(puts.get(1));
    storage.put(puts.get(2));
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
    List<Put> puts = preparePuts();
    storage.put(puts.get(0));
    storage.put(puts.get(1));
    storage.put(puts.get(2));
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
    List<Put> puts = preparePuts();
    storage.put(puts.get(0));
    storage.put(puts.get(1));
    storage.put(puts.get(2));

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
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 1)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 3)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
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
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
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
  public void mutate_SingleDeleteGiven_ShouldDeleteRecordProperly() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(delete));
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
  public void mutate_EmptyListGiven_ShouldThrowIllegalArgumentException() throws Exception {
    // Act
    assertThatCode(
            () -> {
              storage.mutate(new ArrayList<>());
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void populateRecords() {
    List<Put> puts = preparePuts();
    puts.forEach(
        p -> {
          assertThatCode(
                  () -> {
                    storage.put(p);
                  })
              .doesNotThrowAnyException();
        });
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
  public static void setUpBeforeClass() throws IOException {
    String contactPoint = System.getProperty("scalardb.cosmos.uri");
    String username = System.getProperty("scalardb.cosmos.username");
    String password = System.getProperty("scalardb.cosmos.password");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    client =
        new CosmosClientBuilder().endpoint(contactPoint).key(password).directMode().buildClient();

    ThroughputProperties autoscaledThroughput =
        ThroughputProperties.createAutoscaledThroughput(4000);

    // create the metadata database and container
    client.createDatabaseIfNotExists(database(METADATA_DATABASE), autoscaledThroughput);
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    client.getDatabase(database(METADATA_DATABASE)).createContainerIfNotExists(containerProperties);

    // insert metadata
    CosmosTableMetadata metadata = new CosmosTableMetadata();
    metadata.setId(table(DATABASE, CONTAINER));
    metadata.setPartitionKeyNames(Collections.singletonList(COL_NAME1));
    metadata.setClusteringKeyNames(Collections.singletonList(COL_NAME4));
    metadata.setSecondaryIndexNames(new HashSet<>(Collections.singletonList(COL_NAME3)));
    Map<String, String> columns = new HashMap<>();
    columns.put(COL_NAME1, "int");
    columns.put(COL_NAME2, "text");
    columns.put(COL_NAME3, "int");
    columns.put(COL_NAME4, "int");
    columns.put(COL_NAME5, "boolean");
    metadata.setColumns(columns);
    client
        .getDatabase(database(METADATA_DATABASE))
        .getContainer(METADATA_CONTAINER)
        .createItem(metadata);

    // create the user database
    client.createDatabaseIfNotExists(database(DATABASE), autoscaledThroughput);

    // create the user container
    containerProperties = new CosmosContainerProperties(CONTAINER, PARTITION_KEY);
    client.getDatabase(database(DATABASE)).createContainerIfNotExists(containerProperties);

    String storedProcedure =
        Files.lines(Paths.get(STORED_PROCEDURE_PATH), StandardCharsets.UTF_8)
            .reduce("", (prev, cur) -> prev + cur + System.getProperty("line.separator"));
    CosmosStoredProcedureProperties properties =
        new CosmosStoredProcedureProperties("mutate.js", storedProcedure);
    client
        .getDatabase(database(DATABASE))
        .getContainer(CONTAINER)
        .getScripts()
        .createStoredProcedure(properties, new CosmosStoredProcedureRequestOptions());

    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    storage = new Cosmos(new DatabaseConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() {
    CosmosDatabase database = client.getDatabase(database(METADATA_DATABASE));
    database.getContainer(METADATA_CONTAINER).delete();
    database.delete();

    client.getDatabase(database(DATABASE)).delete();

    client.close();

    storage.close();
  }
}
