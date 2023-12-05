package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.querybuilder.QueryBuilder;
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
    clusterManager = new ClusterManager(databaseConfig);
    CassandraConfig cassandraConfig = new CassandraConfig(databaseConfig);
    metadataKeyspace = cassandraConfig.getMetadataKeyspace();
  }

  public CassandraAdminTestUtils(Properties properties, ClusterManager clusterManager) {
    super(properties);
    this.clusterManager = clusterManager;
    CassandraConfig cassandraConfig = new CassandraConfig(new DatabaseConfig(properties));
    metadataKeyspace = cassandraConfig.getMetadataKeyspace();
  }

  @Override
  public void dropNamespacesTable() {
    String dropTableQuery =
        SchemaBuilder.dropTable(
                quoteIfNecessary(metadataKeyspace),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .getQueryString();
    clusterManager.getSession().execute(dropTableQuery);
  }

  @Override
  public void truncateNamespacesTable() {
    String truncateTableQuery =
        QueryBuilder.truncate(
                quoteIfNecessary(metadataKeyspace),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .getQueryString();
    clusterManager.getSession().execute(truncateTableQuery);
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
  public void dropNamespace(String namespace) {
    String dropKeyspaceQuery =
        SchemaBuilder.dropKeyspace(quoteIfNecessary(namespace)).getQueryString();
    clusterManager.getSession().execute(dropKeyspaceQuery);
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
  public void dropMetadataTable() {
    // Do nothing
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
