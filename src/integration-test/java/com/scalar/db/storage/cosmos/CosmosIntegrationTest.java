package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CosmosIntegrationTest {
  private static final String METADATA_KEYSPACE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String KEYSPACE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String CONTACT_POINT = System.getenv("COSMOS_URI");
  private static final String USERNAME = "not_used";
  private static final String PASSWORD = System.getenv("COSMOS_PASSWORD");
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String PARTITION_KEY = "/concatPartitionKey";
  private static CosmosClient client;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(TABLE, PARTITION_KEY);
    client.getDatabase(KEYSPACE).createContainerIfNotExists(containerProperties, 400);

    storage.with(KEYSPACE, TABLE);
  }

  @After
  public void tearDown() throws Exception {
    // delete the TABLE
    CosmosContainer container = client.getDatabase(KEYSPACE).getContainer(TABLE);
    container.delete();
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
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    client =
        new CosmosClientBuilder().endpoint(CONTACT_POINT).key(PASSWORD).directMode().buildClient();

    CosmosDatabase database = client.createDatabaseIfNotExists(METADATA_KEYSPACE).getDatabase();
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_TABLE, "/id");
    CosmosContainer container =
        database.createContainerIfNotExists(containerProperties, 400).getContainer();
    TableMetadata metadata = new TableMetadata();
    metadata.setId(KEYSPACE + "." + TABLE);
    metadata.setPartitionKeyNames(new HashSet<>(Arrays.asList(COL_NAME1)));
    metadata.setClusteringKeyNames(new HashSet<>(Arrays.asList(COL_NAME4)));
    Map<String, String> columns = new HashMap<>();
    columns.put(COL_NAME1, "int");
    columns.put(COL_NAME2, "text");
    columns.put(COL_NAME3, "int");
    columns.put(COL_NAME4, "int");
    columns.put(COL_NAME5, "boolean");
    metadata.setColumns(columns);
    container.createItem(metadata);

    client.createDatabaseIfNotExists(KEYSPACE).getDatabase();

    client.close();

    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    storage = new Cosmos(new DatabaseConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CosmosDatabase database = client.getDatabase(METADATA_KEYSPACE);
    CosmosContainer container = database.getContainer(METADATA_TABLE);
    container.delete();
    database.delete();

    database = client.getDatabase(KEYSPACE);
    database.delete();

    client.close();
  }
}
