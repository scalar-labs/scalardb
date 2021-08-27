package com.scalar.db.storage.cassandra;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
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
  private final Optional<String> keyspacePrefix;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    clusterManager = new ClusterManager(config);
    metadataManager =
        new CassandraTableMetadataManager(clusterManager, config.getNamespacePrefix());
    keyspacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(
      CassandraTableMetadataManager metadataManager,
      ClusterManager clusterManager,
      Optional<String> keyspacePrefix) {
    this.clusterManager = clusterManager;
    this.metadataManager = metadataManager;
    this.keyspacePrefix = keyspacePrefix;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    String fullKeyspace = fullKeyspace(namespace);
    createTableInternal(fullKeyspace, table, metadata, options);
    createSecondaryIndex(fullKeyspace, table, metadata.getSecondaryIndexNames());
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    CreateKeyspace query =
        SchemaBuilder.createKeyspace(getFullNamespaceName(keyspacePrefix, namespace));
    String replicationFactor = options.getOrDefault(REPLICATION_FACTOR, "1");
    ReplicationStrategy replicationStrategy =
        options.containsKey(NETWORK_STRATEGY)
            ? ReplicationStrategy.fromString(options.get(NETWORK_STRATEGY))
            : ReplicationStrategy.SIMPLE_STRATEGY;
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    if (replicationStrategy == ReplicationStrategy.SIMPLE_STRATEGY) {
      replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
      replicationOptions.put("replication_factor", replicationFactor);
    } else if (replicationStrategy == ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY) {
      replicationOptions.put("class", ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
      replicationOptions.put("dc1", replicationFactor);
    }
    try {
      clusterManager
          .getSession()
          .execute(query.with().replication(replicationOptions).getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "creating the keyspace %s failed", getFullNamespaceName(keyspacePrefix, namespace)),
          e);
    }
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
              "dropping the %s table failed", getFullTableName(keyspacePrefix, namespace, table)),
          e);
    }
    metadataManager.deleteTableMetadata(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    String dropKeyspace = SchemaBuilder.dropKeyspace(fullKeyspace(namespace)).getQueryString();
    try {
      clusterManager.getSession().execute(dropKeyspace);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("dropping the %s keyspace failed", fullKeyspace(namespace)), e);
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
              "truncating the %s table failed", getFullTableName(keyspacePrefix, namespace, table)),
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

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      return metadataManager.getTableNames(namespace);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("retrieving the table names of the namespace failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    try {
      KeyspaceMetadata keyspace =
          clusterManager
              .getSession()
              .getCluster()
              .getMetadata()
              .getKeyspace(fullKeyspace(namespace));
      return keyspace != null;
    } catch (RuntimeException e) {
      throw new ExecutionException("checking if the namespace exists failed", e);
    }
  }

  private String fullKeyspace(String keyspace) {
    return getFullNamespaceName(keyspacePrefix, keyspace);
  }

  @VisibleForTesting
  void createTableInternal(
      String fullKeyspace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    Create createTable = SchemaBuilder.createTable(fullKeyspace, table);
    // Add columns
    for (String pk : metadata.getPartitionKeyNames()) {
      createTable =
          createTable.addPartitionKey(pk, toCassandraDataType(metadata.getColumnDataType(pk)));
    }
    for (String ck : metadata.getClusteringKeyNames()) {
      createTable =
          createTable.addClusteringColumn(ck, toCassandraDataType(metadata.getColumnDataType(ck)));
    }
    for (String column : metadata.getColumnNames()) {
      if (metadata.getPartitionKeyNames().contains(column)
          || metadata.getClusteringKeyNames().contains(column)) {
        continue;
      }
      createTable =
          createTable.addColumn(column, toCassandraDataType(metadata.getColumnDataType(column)));
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

    CompactionOptions<?> strategy;
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
      String indexName = String.format("%s_%s_%s", table, INDEX_NAME_PREFIX, index);
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

  /**
   * Returns the equivalent {@link com.datastax.driver.core.DataType}
   *
   * @return the equivalent {@link com.datastax.driver.core.DataType} to the Scalar DB data type
   */
  private com.datastax.driver.core.DataType toCassandraDataType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return com.datastax.driver.core.DataType.cboolean();
      case INT:
        return com.datastax.driver.core.DataType.cint();
      case BIGINT:
        return com.datastax.driver.core.DataType.bigint();
      case FLOAT:
        return com.datastax.driver.core.DataType.cfloat();
      case DOUBLE:
        return com.datastax.driver.core.DataType.cdouble();
      case TEXT:
        return com.datastax.driver.core.DataType.text();
      case BLOB:
        return com.datastax.driver.core.DataType.blob();
      default:
        throw new UnsupportedOperationException(String.format("%s is not yet supported", dataType));
    }
  }

  enum CompactionStrategy {
    STCS,
    LCS,
    TWCS
  }

  enum ReplicationStrategy {
    SIMPLE_STRATEGY("SimpleStrategy"),
    NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");
    private final String strategyName;

    ReplicationStrategy(final String strategyName) {
      this.strategyName = strategyName;
    }

    public static ReplicationStrategy fromString(String text) {
      for (ReplicationStrategy strategy : ReplicationStrategy.values()) {
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
