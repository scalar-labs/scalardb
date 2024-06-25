package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
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
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraAdmin implements DistributedStorageAdmin {

  public static final String REPLICATION_STRATEGY = "replication-strategy";
  public static final String COMPACTION_STRATEGY = "compaction-strategy";
  public static final String REPLICATION_FACTOR = "replication-factor";
  @VisibleForTesting static final String INDEX_NAME_PREFIX = "index";

  private final ClusterManager clusterManager;
  private final String systemNamespace;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    clusterManager = new ClusterManager(config);
    systemNamespace =
        new CassandraConfig(config)
            .getSystemNamespaceName()
            .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  CassandraAdmin(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    systemNamespace = DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME;
  }

  @VisibleForTesting
  CassandraAdmin(ClusterManager clusterManager, DatabaseConfig config) {
    this.clusterManager = clusterManager;
    systemNamespace =
        new CassandraConfig(config)
            .getSystemNamespaceName()
            .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    createTableInternal(namespace, table, metadata, options);
    createSecondaryIndexes(namespace, table, metadata.getSecondaryIndexNames(), options);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    CreateKeyspace query = SchemaBuilder.createKeyspace(quoteIfNecessary(namespace));
    String replicationFactor = options.getOrDefault(REPLICATION_FACTOR, "1");
    ReplicationStrategy replicationStrategy =
        options.containsKey(REPLICATION_STRATEGY)
            ? ReplicationStrategy.fromString(options.get(REPLICATION_STRATEGY))
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
      throw new ExecutionException(String.format("Creating the keyspace %s failed", namespace), e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    String dropTableQuery =
        SchemaBuilder.dropTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
            .getQueryString();
    try {
      clusterManager.getSession().execute(dropTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Dropping the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    String dropKeyspace = SchemaBuilder.dropKeyspace(quoteIfNecessary(namespace)).getQueryString();
    try {
      clusterManager.getSession().execute(dropKeyspace);
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Dropping the %s keyspace failed", namespace), e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableQuery =
        QueryBuilder.truncate(quoteIfNecessary(namespace), quoteIfNecessary(table))
            .getQueryString();
    try {
      clusterManager.getSession().execute(truncateTableQuery);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Truncating the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    String indexName = getIndexName(table, columnName);
    SchemaStatement createIndex =
        SchemaBuilder.createIndex(indexName)
            .onTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
            .andColumn(quoteIfNecessary(columnName));
    try {
      clusterManager.getSession().execute(createIndex.getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Creating the secondary index for %s.%s.%s failed", namespace, table, columnName),
          e);
    }
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    String indexName = getIndexName(table, columnName);
    SchemaStatement dropIndex = SchemaBuilder.dropIndex(quoteIfNecessary(namespace), indexName);
    try {
      clusterManager.getSession().execute(dropIndex.getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Dropping the secondary index for %s.%s.%s failed", namespace, table, columnName),
          e);
    }
  }

  private String getIndexName(String table, String columnName) {
    return String.format("%s_%s_%s", table, INDEX_NAME_PREFIX, columnName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      com.datastax.driver.core.TableMetadata metadata =
          clusterManager.getMetadata(namespace, table);
      if (metadata == null) {
        return null;
      }
      return createTableMetadata(metadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("Getting a table metadata failed", e);
    }
  }

  private TableMetadata createTableMetadata(com.datastax.driver.core.TableMetadata metadata)
      throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    for (ColumnMetadata column : metadata.getColumns()) {
      builder.addColumn(column.getName(), fromCassandraDataType(column.getType().getName()));
    }
    metadata.getPartitionKey().forEach(c -> builder.addPartitionKey(c.getName()));
    for (int i = 0; i < metadata.getClusteringColumns().size(); i++) {
      String clusteringColumnName = metadata.getClusteringColumns().get(i).getName();
      ClusteringOrder clusteringOrder = metadata.getClusteringOrder().get(i);
      builder.addClusteringKey(clusteringColumnName, convertOrder(clusteringOrder));
    }
    metadata.getIndexes().forEach(i -> builder.addSecondaryIndex(i.getTarget()));
    return builder.build();
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table) {
    throw new UnsupportedOperationException(
        CoreError.CASSANDRA_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType) {
    throw new UnsupportedOperationException(
        CoreError.CASSANDRA_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options) {
    throw new UnsupportedOperationException(
        CoreError.CASSANDRA_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  private Scan.Ordering.Order convertOrder(ClusteringOrder clusteringOrder) {
    switch (clusteringOrder) {
      case ASC:
        return Scan.Ordering.Order.ASC;
      case DESC:
        return Scan.Ordering.Order.DESC;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      KeyspaceMetadata keyspace =
          clusterManager
              .getSession()
              .getCluster()
              .getMetadata()
              .getKeyspace(quoteIfNecessary(namespace));
      if (keyspace == null) {
        return Collections.emptySet();
      }
      return keyspace.getTables().stream()
          .map(com.datastax.driver.core.TableMetadata::getName)
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the table names of the namespace failed", e);
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
              .getKeyspace(quoteIfNecessary(namespace));
      return keyspace != null;
    } catch (RuntimeException e) {
      throw new ExecutionException("Checking if the namespace exists failed", e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    // We have this check to stay consistent with the behavior of the other admins classes
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          "The table " + getFullTableName(namespace, table) + "  does not exist");
    }
    // The table metadata are not managed by ScalarDB, so we don't need to do anything here
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      String alterTableQuery =
          SchemaBuilder.alterTable(namespace, table)
              .addColumn(columnName)
              .type(toCassandraDataType(columnType))
              .getQueryString();

      clusterManager.getSession().execute(alterTableQuery);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new column %s to the %s.%s table failed", columnName, namespace, table),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      // Retrieve user keyspace and filter out system ones. A downside is that this may include
      // keyspace not created by ScalarDB.
      return Stream.concat(
              clusterManager.getSession().getCluster().getMetadata().getKeyspaces().stream()
                  .map(KeyspaceMetadata::getName)
                  .filter(name -> !name.startsWith("system")),
              Stream.of(systemNamespace))
          .collect(Collectors.toSet());

    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the existing keyspace names failed", e);
    }
  }

  @VisibleForTesting
  void createTableInternal(
      String keyspace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    Create createTable =
        SchemaBuilder.createTable(quoteIfNecessary(keyspace), quoteIfNecessary(table));
    // Add columns
    for (String pk : metadata.getPartitionKeyNames()) {
      createTable =
          createTable.addPartitionKey(
              quoteIfNecessary(pk), toCassandraDataType(metadata.getColumnDataType(pk)));
    }
    for (String ck : metadata.getClusteringKeyNames()) {
      createTable =
          createTable.addClusteringColumn(
              quoteIfNecessary(ck), toCassandraDataType(metadata.getColumnDataType(ck)));
    }
    for (String column : metadata.getColumnNames()) {
      if (metadata.getPartitionKeyNames().contains(column)
          || metadata.getClusteringKeyNames().contains(column)) {
        continue;
      }
      createTable =
          createTable.addColumn(
              quoteIfNecessary(column), toCassandraDataType(metadata.getColumnDataType(column)));
    }
    // Add clustering order
    Create.Options createTableWithOptions = createTable.withOptions();
    for (String ck : metadata.getClusteringKeyNames()) {
      Direction direction =
          metadata.getClusteringOrder(ck) == Order.ASC ? Direction.ASC : Direction.DESC;
      createTableWithOptions =
          createTableWithOptions.clusteringOrder(quoteIfNecessary(ck), direction);
    }
    // Add compaction strategy
    CompactionStrategy compactionStrategy =
        CompactionStrategy.valueOf(
            options
                .getOrDefault(COMPACTION_STRATEGY, CompactionStrategy.STCS.toString())
                .toUpperCase(Locale.ROOT));

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
          String.format("Creating the table %s.%s failed", keyspace, table), e);
    }
  }

  @VisibleForTesting
  void createSecondaryIndexes(
      String keyspace, String table, Set<String> secondaryIndexNames, Map<String, String> options)
      throws ExecutionException {
    for (String name : secondaryIndexNames) {
      createIndex(keyspace, table, name, options);
    }
  }

  @Override
  public void close() {
    clusterManager.close();
  }

  /**
   * Return the ScalarDB datatype value that is equivalent to {@link
   * com.datastax.driver.core.DataType}
   *
   * @return ScalarDB datatype that is equivalent {@link com.datastax.driver.core.DataType}
   */
  private DataType fromCassandraDataType(
      com.datastax.driver.core.DataType.Name cassandraDataTypeName) throws ExecutionException {
    switch (cassandraDataTypeName) {
      case INT:
        return DataType.INT;
      case BIGINT:
        return DataType.BIGINT;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case TEXT:
        return DataType.TEXT;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case BLOB:
        return DataType.BLOB;
      default:
        throw new ExecutionException(
            String.format("%s is not yet supported", cassandraDataTypeName));
    }
  }

  /**
   * Returns the equivalent {@link com.datastax.driver.core.DataType}
   *
   * @return the equivalent {@link com.datastax.driver.core.DataType} to the ScalarDB data type
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
        throw new AssertionError();
    }
  }

  public enum CompactionStrategy {
    STCS,
    LCS,
    TWCS
  }

  public enum ReplicationStrategy {
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
          CoreError.CASSANDRA_NETWORK_STRATEGY_NOT_FOUND.buildMessage(text));
    }

    @Override
    public String toString() {
      return strategyName;
    }
  }
}
