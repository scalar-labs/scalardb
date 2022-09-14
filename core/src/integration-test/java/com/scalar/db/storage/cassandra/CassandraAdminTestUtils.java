package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class CassandraAdminTestUtils extends AdminTestUtils {

  private final ClusterManager clusterManager;
  private final String metadataKeyspace;

  public CassandraAdminTestUtils(Properties properties) {
    super(properties);
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    CassandraConfig cassandraConfig = new CassandraConfig(databaseConfig);
    metadataKeyspace =
        cassandraConfig.getMetadataKeyspace().orElse(CassandraAdmin.METADATA_KEYSPACE);
    clusterManager = new ClusterManager(databaseConfig);
  }

  @Override
  public void dropMetadataTable() {
    // Do nothing
  }

  @Override
  public void truncateMetadataTable() {
    // Do nothing
  }

  @Override
  public void corruptMetadata(String namespace, String table) {
    // Do nothing
  }

  @Override
  public void dropNamespacesTable() {
    String dropKeyspaceQuery =
        SchemaBuilder.dropKeyspace(quoteIfNecessary(metadataKeyspace)).getQueryString();
    clusterManager.getSession().execute(dropKeyspaceQuery);
  }
}
