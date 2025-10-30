package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class CassandraAdminTestUtils extends AdminTestUtils {
  private final ClusterManager clusterManager;

  public CassandraAdminTestUtils(Properties properties) {
    super(properties);
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
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
  public void deleteMetadata(String namespace, String table) throws Exception {
    // Do nothing
  }

  @Override
  public boolean namespaceExists(String namespace) {
    return clusterManager.getSession().getCluster().getMetadata().getKeyspace(namespace) != null;
  }

  @Override
  public boolean tableExists(String namespace, String table) {
    return clusterManager.getMetadata(namespace, table) != null;
  }

  @Override
  public void dropTable(String namespace, String table) {
    String dropTableQuery =
        SchemaBuilder.dropTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
            .getQueryString();
    clusterManager.getSession().execute(dropTableQuery);
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
