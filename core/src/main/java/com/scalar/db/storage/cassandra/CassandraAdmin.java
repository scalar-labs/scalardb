package com.scalar.db.storage.cassandra;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

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
  private final ClusterManager clusterManager;
  private final CassandraTableMetadataManager metadataManager;
  private final Optional<String> namespacePrefix;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    clusterManager = new ClusterManager(config);
    metadataManager =
        new CassandraTableMetadataManager(clusterManager, config.getNamespacePrefix());
    namespacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(
      CassandraTableMetadataManager metadataManager,
      ClusterManager clusterManager,
      Optional<String> namespacePrefix) {
    this.clusterManager = clusterManager;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    String fullNamespace = fullKeyspace(namespace);
    createNamespace(fullNamespace, options);
    createTableInternal(fullNamespace, table, metadata, options);
    createSecondaryIndex(fullNamespace, table, metadata.getSecondaryIndexNames());
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    String dropTableQuery =
        SchemaBuilder.dropTable(fullKeyspace(namespace), table).getQueryString();
    try {
      clusterManager.getSession().execute(dropTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "dropping the %s table failed", getFullTableName(namespacePrefix, namespace, table)),
          e);
    }
    metadataManager.deleteTableMetadata(namespace, table);
    if (metadataManager.getTableNames(namespace).isEmpty()) {
      String dropKeyspace =
          SchemaBuilder.dropKeyspace(fullKeyspace(namespace)).ifExists().getQueryString();
      try {
        clusterManager.getSession().execute(dropKeyspace);
      } catch (RuntimeException e) {
        throw new ExecutionException(
            String.format("dropping the %s namespace failed", fullKeyspace(namespace)), e);
      }
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableQuery =
        QueryBuilder.truncate(fullKeyspace(namespace), table).getQueryString();
    try {
      clusterManager.getSession().execute(truncateTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "truncating the %s table failed",
              getFullTableName(namespacePrefix, namespace, table)),
          e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(namespace, table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private String fullKeyspace(String namespace) {
    return getFullNamespaceName(namespacePrefix, namespace);
  }

  @VisibleForTesting
  void createNamespace(String fullkeyspace, Map<String, String> options) throws ExecutionException {
    CreateKeyspace query = SchemaBuilder.createKeyspace(fullkeyspace).ifNotExists();
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
          String.format("creating the keyspace %s failed", fullkeyspace), e);
    }
  }

  @VisibleForTesting
  void createTableInternal(
      String fullKeyspace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    Create createTable = SchemaBuilder.createTable(fullKeyspace, table);
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
          String.format("creating the table %s.%s failed", fullKeyspace, table), e);
    }
  }

  @VisibleForTesting
  void createSecondaryIndex(String fullKeyspace, String table, Set<String> secondaryIndexNames)
      throws ExecutionException {
    for (String index : secondaryIndexNames) {
      String indexName =
          String.format("%s_%s_%s", table, DistributedStorageAdmin.INDEX_NAME_PREFIX, index);
      SchemaStatement createIndex =
          SchemaBuilder.createIndex(indexName).onTable(fullKeyspace, table).andColumn(index);
      try {
        clusterManager.getSession().execute(createIndex.getQueryString());
      } catch (RuntimeException e) {
        throw new ExecutionException(
            String.format(
                "creating the secondary index for %s.%s.%s failed", fullKeyspace, table, index),
            e);
      }
    }
  }

  @Override
  public void close() {
    clusterManager.close();
  }

  enum CompactionStrategy {
    STCS,
    LCS,
    TWCS
  }

  enum NetworkStrategy {
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
