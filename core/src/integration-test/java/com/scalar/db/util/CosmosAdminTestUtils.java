package com.scalar.db.util;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.storage.cosmos.Record;
import java.util.Properties;

public class CosmosAdminTestUtils extends AdminTestUtils {

  private final CosmosClient client;

  public CosmosAdminTestUtils(Properties properties) {
    super();
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(properties));
    client =
        new CosmosClientBuilder()
            .endpoint(config.getEndpoint())
            .key(config.getKey())
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();
    metadataNamespace =
        new CosmosConfig(new DatabaseConfig(properties))
            .getTableMetadataDatabase()
            .orElse(CosmosAdmin.METADATA_DATABASE);
    metadataTable = CosmosAdmin.METADATA_CONTAINER;
  }

  @Override
  public void dropMetadataTable() {
    client.getDatabase(metadataNamespace).delete();
    try {
      client.getDatabase(metadataNamespace).read();
    } catch (CosmosException e) {
      if (e.getStatusCode() != 404) {
        throw new RuntimeException("Dropping the metadata table failed", e);
      }
    }
  }

  @Override
  public void truncateMetadataTable() {
    CosmosContainer container = client.getDatabase(metadataNamespace).getContainer(metadataTable);

    CosmosPagedIterable<Record> records =
        container.queryItems("SELECT t.id FROM t", new CosmosQueryRequestOptions(), Record.class);
    records.forEach(
        record ->
            container.deleteItem(
                record.getId(), new PartitionKey(record.getId()), new CosmosItemRequestOptions()));
  }

  /**
   * Retrieve the stored procedure for the given table
   *
   * @param namespace a namespace
   * @param table a table
   * @return the stored procedure
   */
  public CosmosStoredProcedure getTableStoredProcedure(String namespace, String table) {
    return client
        .getDatabase(namespace)
        .getContainer(table)
        .getScripts()
        .getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME);
  }
}
