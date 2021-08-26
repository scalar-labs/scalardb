package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

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
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosTableMetadata;
import com.scalar.db.storage.cosmos.Record;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithCosmosIntegrationTest extends ConsensusCommitIntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";
  private static final String STORED_PROCEDURE_PATH =
      "tools/scalar-schema/stored_procedure/mutate.js";
  private static final String PARTITION_KEY = "/concatenatedPartitionKey";

  private static Optional<String> namespacePrefix;
  private static CosmosClient client;
  private static DistributedStorage originalStorage;
  private static DatabaseConfig config;

  @Before
  public void setUp() {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, config, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() {
    // delete the containers
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      truncateContainer(database(NAMESPACE), table);
    }

    truncateContainer(database(Coordinator.NAMESPACE), Coordinator.TABLE);
  }

  private void truncateContainer(String database, String container) {
    CosmosPagedIterable<Record> records =
        client
            .getDatabase(database)
            .getContainer(container)
            .queryItems("SELECT * FROM Record", new CosmosQueryRequestOptions(), Record.class);
    for (Record record : records) {
      client
          .getDatabase(database)
          .getContainer(container)
          .deleteItem(record, new CosmosItemRequestOptions());
    }
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

    String storedProcedure;
    try (Stream<String> lines =
        Files.lines(Paths.get(STORED_PROCEDURE_PATH), StandardCharsets.UTF_8)) {
      storedProcedure =
          lines.reduce("", (prev, cur) -> prev + cur + System.getProperty("line.separator"));
    }
    CosmosStoredProcedureProperties properties =
        new CosmosStoredProcedureProperties("mutate.js", storedProcedure);

    // create the the coordinator database amd container
    client.createDatabaseIfNotExists(database(Coordinator.NAMESPACE), autoscaledThroughput);

    containerProperties = new CosmosContainerProperties(Coordinator.TABLE, PARTITION_KEY);
    client
        .getDatabase(database(Coordinator.NAMESPACE))
        .createContainerIfNotExists(containerProperties);
    client
        .getDatabase(database(Coordinator.NAMESPACE))
        .getContainer(Coordinator.TABLE)
        .getScripts()
        .createStoredProcedure(properties, new CosmosStoredProcedureRequestOptions());

    // create the user database and containers
    client.createDatabaseIfNotExists(database(NAMESPACE), autoscaledThroughput);

    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      containerProperties = new CosmosContainerProperties(table, PARTITION_KEY);
      client.getDatabase(database(NAMESPACE)).createContainerIfNotExists(containerProperties);
      client
          .getDatabase(database(NAMESPACE))
          .getContainer(table)
          .getScripts()
          .createStoredProcedure(properties, new CosmosStoredProcedureRequestOptions());
    }

    // insert metadata for the coordinator table
    CosmosTableMetadata metadata = new CosmosTableMetadata();
    metadata.setId(table(Coordinator.NAMESPACE, Coordinator.TABLE));
    metadata.setPartitionKeyNames(Collections.singletonList(Attribute.ID));
    metadata.setClusteringKeyNames(Collections.emptyList());
    metadata.setSecondaryIndexNames(Collections.emptySet());
    Map<String, String> columns = new HashMap<>();
    columns.put(Attribute.ID, "text");
    columns.put(Attribute.STATE, "int");
    columns.put(Attribute.CREATED_AT, "bigint");
    metadata.setColumns(columns);
    client
        .getDatabase(database(METADATA_DATABASE))
        .getContainer(METADATA_CONTAINER)
        .createItem(metadata);

    // insert metadata for the user tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      metadata = new CosmosTableMetadata();
      metadata.setId(table(NAMESPACE, table));
      metadata.setPartitionKeyNames(
          Collections.singletonList(ConsensusCommitIntegrationTestBase.ACCOUNT_ID));
      metadata.setClusteringKeyNames(
          Collections.singletonList(ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE));
      metadata.setSecondaryIndexNames(Collections.emptySet());
      columns = new HashMap<>();
      columns.put(ConsensusCommitIntegrationTestBase.ACCOUNT_ID, "int");
      columns.put(ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE, "int");
      columns.put(ConsensusCommitIntegrationTestBase.BALANCE, "int");
      columns.put(Attribute.ID, "text");
      columns.put(Attribute.VERSION, "int");
      columns.put(Attribute.STATE, "int");
      columns.put(Attribute.PREPARED_AT, "bigint");
      columns.put(Attribute.COMMITTED_AT, "bigint");
      columns.put(Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTestBase.BALANCE, "int");
      columns.put(Attribute.BEFORE_ID, "text");
      columns.put(Attribute.BEFORE_VERSION, "int");
      columns.put(Attribute.BEFORE_STATE, "int");
      columns.put(Attribute.BEFORE_PREPARED_AT, "bigint");
      columns.put(Attribute.BEFORE_COMMITTED_AT, "bigint");
      metadata.setColumns(columns);
      client
          .getDatabase(database(METADATA_DATABASE))
          .getContainer(METADATA_CONTAINER)
          .createItem(metadata);
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new DatabaseConfig(props);
    originalStorage = new Cosmos(config);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    CosmosDatabase database = client.getDatabase(database(METADATA_DATABASE));
    database.getContainer(METADATA_CONTAINER).delete();
    database.delete();

    client.getDatabase(database(Coordinator.NAMESPACE)).delete();

    client.getDatabase(database(NAMESPACE)).delete();

    client.close();

    originalStorage.close();
  }
}
