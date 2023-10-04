package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

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
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.assertj.core.util.Sets;

public class CosmosAdminTestUtils extends AdminTestUtils {

  private final CosmosClient client;
  private final String metadataDatabase;

  public CosmosAdminTestUtils(Properties properties) {
    super(properties);
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(properties));
    client =
        new CosmosClientBuilder()
            .endpoint(config.getEndpoint())
            .key(config.getKey())
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();
    metadataDatabase =
        new CosmosConfig(new DatabaseConfig(properties))
            .getMetadataDatabase()
            .orElse(CosmosAdmin.METADATA_DATABASE);
  }

  @Override
  public void dropMetadataTable() {
    dropTable(metadataDatabase, CosmosAdmin.TABLE_METADATA_CONTAINER);
  }

  @Override
  public void truncateMetadataTable() {
    CosmosContainer container =
        client.getDatabase(metadataDatabase).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);

    CosmosPagedIterable<Record> records =
        container.queryItems("SELECT t.id FROM t", new CosmosQueryRequestOptions(), Record.class);
    records.forEach(
        record ->
            container.deleteItem(
                record.getId(), new PartitionKey(record.getId()), new CosmosItemRequestOptions()));
  }

  @Override
  public void corruptMetadata(String namespace, String table) {
    CosmosTableMetadata corruptedMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("corrupted"))
            .build();

    CosmosContainer container =
        client.getDatabase(metadataDatabase).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
    container.upsertItem(corruptedMetadata);
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

  @Override
  public boolean tableExists(String namespace, String table) {
    try {
      client.getDatabase(namespace).getContainer(table).read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw e;
    }
    return true;
  }

  @Override
  public void dropTable(String namespace, String table) {
    client.getDatabase(namespace).getContainer(table).delete();
  }
}
