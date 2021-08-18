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
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.IntegrationTestBase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CosmosIntegrationTest extends IntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";
  private static final String STORED_PROCEDURE_PATH =
      "tools/scalar-schema/stored_procedure/mutate.js";
  private static final String PARTITION_KEY = "/concatenatedPartitionKey";

  private static Optional<String> namespacePrefix;
  private static CosmosClient client;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() {
    // delete all the data of the container
    CosmosPagedIterable<Record> records =
        client
            .getDatabase(database(NAMESPACE))
            .getContainer(TABLE)
            .queryItems("SELECT * FROM Record", new CosmosQueryRequestOptions(), Record.class);
    for (Record record : records) {
      client
          .getDatabase(database(NAMESPACE))
          .getContainer(TABLE)
          .deleteItem(record, new CosmosItemRequestOptions());
    }
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
    metadata.setId(table(NAMESPACE, TABLE));
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
    client.createDatabaseIfNotExists(database(NAMESPACE), autoscaledThroughput);

    // create the user container
    containerProperties = new CosmosContainerProperties(TABLE, PARTITION_KEY);
    client.getDatabase(database(NAMESPACE)).createContainerIfNotExists(containerProperties);

    String storedProcedure;
    try (Stream<String> lines =
        Files.lines(Paths.get(STORED_PROCEDURE_PATH), StandardCharsets.UTF_8)) {
      storedProcedure =
          lines.reduce("", (prev, cur) -> prev + cur + System.getProperty("line.separator"));
    }
    CosmosStoredProcedureProperties properties =
        new CosmosStoredProcedureProperties("mutate.js", storedProcedure);
    client
        .getDatabase(database(NAMESPACE))
        .getContainer(TABLE)
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

    client.getDatabase(database(NAMESPACE)).delete();

    client.close();

    storage.close();
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
}
