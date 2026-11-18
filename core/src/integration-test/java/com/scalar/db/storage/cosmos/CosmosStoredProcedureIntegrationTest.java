package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.cosmos.CosmosClient;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for the stored procedure that mutates records in Cosmos DB. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CosmosStoredProcedureIntegrationTest {

  private static final String TEST_NAME = "stored_procedure";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String PARTITION_KEY = "pk";
  private static final String CLUSTERING_KEY = "ck";
  private static final String VALUE = "value";

  // A query in the stored procedure returns up to 100 records in a page by default, so the result
  // of a query that matches this number of records consists of multiple pages
  private static final int RECORD_COUNT = 120;

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private CosmosClient client;
  private DeleteStatementHandler deleteStatementHandler;

  @BeforeAll
  void beforeAll() throws ExecutionException {
    Properties properties = CosmosEnv.getProperties(TEST_NAME);
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();

    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    CosmosConfig config = new CosmosConfig(databaseConfig);
    client = CosmosUtils.buildCosmosClient(config);
    deleteStatementHandler =
        new DeleteStatementHandler(
            client,
            new TableMetadataManager(
                new CosmosAdmin(client, config),
                databaseConfig.getMetadataCacheExpirationTimeSecs()));

    Map<String, String> options = CosmosEnv.getCreationOptions();
    admin.createNamespace(NAMESPACE, true, options);
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, DataType.INT)
            .addColumn(CLUSTERING_KEY, DataType.INT)
            .addColumn(VALUE, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
            .addClusteringKey(CLUSTERING_KEY)
            .build(),
        true,
        options);
    admin.truncateTable(NAMESPACE, TABLE);
  }

  @AfterAll
  void afterAll() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE, true);
    admin.dropNamespace(NAMESPACE, true);
    admin.close();
    storage.close();
    client.close();
  }

  @Test
  void handle_DeleteMatchingRecordsInMultiplePagesGiven_ShouldDeleteAllRecords()
      throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = Key.ofInt(PARTITION_KEY, 1);
    for (int i = 0; i < RECORD_COUNT; i++) {
      storage.put(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(TABLE)
              .partitionKey(partitionKey)
              .clusteringKey(Key.ofInt(CLUSTERING_KEY, i))
              .intValue(VALUE, i)
              .build());
    }

    // For a delete without the clustering key, the stored procedure gets all the records in the
    // partition with a query and deletes them. The storage doesn't accept such a delete, so this
    // passes it to the handler directly
    Delete delete =
        Delete.newBuilder().namespace(NAMESPACE).table(TABLE).partitionKey(partitionKey).build();

    // Act
    deleteStatementHandler.handle(delete);

    // Assert
    assertThat(scanAll(partitionKey)).isEmpty();
  }

  private List<Result> scanAll(Key partitionKey) throws ExecutionException, IOException {
    Scan scan =
        Scan.newBuilder().namespace(NAMESPACE).table(TABLE).partitionKey(partitionKey).build();
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }
}
