package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraAdmin implements DistributedStorageAdmin {

  public static final String NETWORK_STRATEGY = "network-strategy";
  public static final String COMPACTION_STRATEGY = "compaction-strategy";
  public static final String REPLICATION_FACTOR = "replication-factor";
  public final ClusterManager clusterManager;
  private final CassandraTableMetadataManager metadataManager;
  private final Optional<String> namespacePrefix;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    clusterManager = new ClusterManager(config);
    clusterManager.getSession();
    metadataManager =
        new CassandraTableMetadataManager(clusterManager, config.getNamespacePrefix());
    namespacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(DatabaseConfig config, ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    this.clusterManager.getSession();
    metadataManager =
        new CassandraTableMetadataManager(clusterManager, config.getNamespacePrefix());
    namespacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(CassandraTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    clusterManager = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    String fullNamespace = fullNamespace(namespace);
    createNamespace(fullNamespace, options);
    createTableInternal(fullNamespace, table, metadata, options);
    createSecondaryIndex(fullNamespace, table, metadata.getSecondaryIndexNames());
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    String dropTableQuery =
        SchemaBuilder.dropTable(fullNamespace(namespace), table).getQueryString();
    try {
      clusterManager.getSession().execute(dropTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("dropping the %s.%s table failed", fullNamespace(namespace), table), e);
    }
    metadataManager.deleteTableMetadata(namespace, table);
    // TODO Replace with TableManager.getTableNames()
    if (clusterManager
        .getSession()
        .getCluster()
        .getMetadata()
        .getKeyspace(fullNamespace(namespace))
        .getTables()
        .isEmpty()) {
      String dropKeyspace =
          SchemaBuilder.dropKeyspace(fullNamespace(namespace)).ifExists().getQueryString();
      try {
        clusterManager.getSession().execute(dropKeyspace);
      } catch (RuntimeException e) {
        throw new ExecutionException(
            String.format("dropping the %s namespace failed", fullNamespace(namespace)), e);
      }
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableQuery =
        QueryBuilder.truncate(fullNamespace(namespace), table).getQueryString();
    try {
      clusterManager.getSession().execute(truncateTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("truncating the %s.%s table failed", fullNamespace(namespace), table), e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @VisibleForTesting
  void createNamespace(String fullNamespace, Map<String, String> options)
      throws ExecutionException {
    CreateKeyspace query = SchemaBuilder.createKeyspace(fullNamespace).ifNotExists();
    String replicationFactor = options.getOrDefault(REPLICATION_FACTOR, "1");
    NetworkStrategy networkStrategy =
        options.containsKey(NETWORK_STRATEGY)
            ? NetworkStrategy.fromString(options.get(NETWORK_STRATEGY))
            : NetworkStrategy.SIMPLE_STRATEGY;
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    if (networkStrategy == NetworkStrategy.SIMPLE_STRATEGY) {
      replicationOptions.put("class", NetworkStrategy.SIMPLE_STRATEGY.toString());
      replicationOptions.put("replication_factor", replicationFactor);
    } else if (networkStrategy == NetworkStrategy.NETWORK_TOPOLOGY_STRATEGY) {
      replicationOptions.put("class", NetworkStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
      replicationOptions.put("dc1_name", replicationFactor);
    }
    try {
      clusterManager
          .getSession()
          .execute(query.with().replication(replicationOptions).getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("creating the %s namespace failed", fullNamespace), e);
    }
  }

  @VisibleForTesting
  void createTableInternal(
      String fullNamespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    Create createTable = SchemaBuilder.createTable(fullNamespace, table);
    // Add columns
    for (String pk : metadata.getPartitionKeyNames()) {
      createTable =
          createTable.addPartitionKey(pk, metadata.getColumnDataType(pk).toCassandraDataType());
    }
    for (String ck : metadata.getClusteringKeyNames()) {
      createTable =
          createTable.addClusteringColumn(ck, metadata.getColumnDataType(ck).toCassandraDataType());
    }
    for (String column : metadata.getColumnNames()) {
      if (metadata.getPartitionKeyNames().contains(column)
          || metadata.getClusteringKeyNames().contains(column)) {
        continue;
      }
      createTable =
          createTable.addColumn(column, metadata.getColumnDataType(column).toCassandraDataType());
    }
    // Add clustering order
    Create.Options createTableWithOptions = createTable.withOptions();
    for (String ck : metadata.getClusteringKeyNames()) {
      Direction direction =
          metadata.getClusteringOrder(ck) == Order.ASC ? Direction.ASC : Direction.DESC;
      createTableWithOptions = createTableWithOptions.clusteringOrder(ck, direction);
    }
    // Add compaction strategy
    CompactionStrategy compactionStrategy =
        CompactionStrategy.valueOf(
            options.getOrDefault(COMPACTION_STRATEGY, CompactionStrategy.STCS.toString()));

    TableOptions.CompactionOptions strategy;
    switch (compactionStrategy) {
      case LCS:
        strategy = SchemaBuilder.leveledStrategy();
        break;
      case TWCS:
        strategy = SchemaBuilder.timeWindowCompactionStrategy();
        break;
      default:
        strategy = SchemaBuilder.sizedTieredStategy();
    }
    createTableWithOptions = createTableWithOptions.compactionOptions(strategy);

    try {
      clusterManager.getSession().execute(createTableWithOptions.getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("creating the table %s.%s failed", fullNamespace, table), e);
    }
  }

  @VisibleForTesting
  void createSecondaryIndex(String fullNamespace, String table, Set<String> secondaryIndexNames)
      throws ExecutionException {
    for (String index : secondaryIndexNames) {
      String indexName =
          String.format("%s_%s_%s", table, DistributedStorageAdmin.INDEX_NAME_PREFIX, index);
      SchemaStatement createIndex =
          SchemaBuilder.createIndex(indexName).onTable(fullNamespace, table).andColumn(index);
      try {
        clusterManager.getSession().execute(createIndex.getQueryString());
      } catch (RuntimeException e) {
        throw new ExecutionException(
            String.format(
                "creating the secondary index for %s.%s.%s failed", fullNamespace, table, index),
            e);
      }
    }
  }

  @Override
  public void close() {
    clusterManager.close();
  }

  public enum CompactionStrategy {
    STCS,
    LCS,
    TWCS
  }

  public enum NetworkStrategy {
    SIMPLE_STRATEGY("SimpleStrategy"),
    NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");
    private final String strategyName;

    NetworkStrategy(final String strategyName) {
      this.strategyName = strategyName;
    }

    public static NetworkStrategy fromString(String text) {
      for (NetworkStrategy strategy : NetworkStrategy.values()) {
        if (strategy.strategyName.equalsIgnoreCase(text)) {
          return strategy;
        }
      }
      throw new IllegalArgumentException(
          String.format("The network strategy %s does not exist", text));
    }

    @Override
    public String toString() {
      return strategyName;
    }
  }
}
