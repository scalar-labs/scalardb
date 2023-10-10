package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateIndex;
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
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraAdmin implements DistributedStorageAdmin {

  public static final String REPLICATION_STRATEGY = "replication-strategy";
  public static final String COMPACTION_STRATEGY = "compaction-strategy";
  public static final String REPLICATION_FACTOR = "replication-factor";
  public static final String METADATA_KEYSPACE = "scalardb";
  public static final String NAMESPACES_TABLE = "namespaces";
  public static final String NAMESPACES_NAME_COL = "name";
  private static final String DEFAULT_REPLICATION_FACTOR = "3";
  @VisibleForTesting static final String INDEX_NAME_PREFIX = "index";
  private final ClusterManager clusterManager;
  private final String metadataKeyspace;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    this(new ClusterManager(config), config);
  }

  CassandraAdmin(ClusterManager clusterManager, DatabaseConfig config) {
    this.clusterManager = clusterManager;
    CassandraConfig cassandraConfig = new CassandraConfig(config);
    metadataKeyspace = cassandraConfig.getMetadataKeyspace().orElse(METADATA_KEYSPACE);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(namespace, table, metadata, false, options);
      createSecondaryIndexes(namespace, table, metadata.getSecondaryIndexNames(), false);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Creating the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      createKeyspace(namespace, options, false);
      createKeyspace(metadataKeyspace, options, true);
      createNamespacesTableIfNotExists();
      upsertIntoNamespacesTable(namespace);
    } catch (IllegalArgumentException e) {
      // thrown by ReplicationStrategy.fromString() when the given replication strategy is unknown
      throw e;
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Creating the %s keyspace failed", namespace), e);
    }
  }

  private void createKeyspace(String keyspace, Map<String, String> options, boolean ifNotExists) {
    CreateKeyspace query = SchemaBuilder.createKeyspace(quoteIfNecessary(keyspace));
    if (ifNotExists) {
      query = query.ifNotExists();
    }
    Map<String, Object> replicationOptions = prepareReplicationOptions(options);
    String queryString = query.with().replication(replicationOptions).getQueryString();

    clusterManager.getSession().execute(queryString);
  }

  private Map<String, Object> prepareReplicationOptions(Map<String, String> options) {
    String replicationFactor = options.getOrDefault(REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR);
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
    return replicationOptions;
  }

  private void upsertIntoNamespacesTable(String keyspace) {
    // Cassandra insert behaves like an upsert
    String insertQuery =
        QueryBuilder.insertInto(
                quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
            .value(NAMESPACES_NAME_COL, quoteIfNecessary(keyspace))
            .toString();
    clusterManager.getSession().execute(insertQuery);
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
    try {
      dropKeyspace(namespace);
      deleteFromNamespacesTable(namespace);
      dropNamespacesTableIfEmpty();
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Dropping the %s keyspace failed", namespace), e);
    }
  }

  private void dropKeyspace(String keyspace) {
    String dropKeyspaceQuery =
        SchemaBuilder.dropKeyspace(quoteIfNecessary(keyspace)).getQueryString();
    clusterManager.getSession().execute(dropKeyspaceQuery);
  }

  private void deleteFromNamespacesTable(String keyspace) {
    String deleteQuery =
        QueryBuilder.delete()
            .from(quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
            .where(QueryBuilder.eq(NAMESPACES_NAME_COL, quoteIfNecessary(keyspace)))
            .toString();
    clusterManager.getSession().execute(deleteQuery);
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
    createIndexInternal(namespace, table, columnName, false);
  }

  public void createIndexInternal(
      String namespace, String table, String columnName, boolean ifNotExists)
      throws ExecutionException {
    String indexName = getIndexName(table, columnName);
    CreateIndex createIndex = SchemaBuilder.createIndex(indexName);
    if (ifNotExists) {
      createIndex = createIndex.ifNotExists();
    }
    SchemaStatement createIndexStatement =
        createIndex
            .onTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
            .andColumn(quoteIfNecessary(columnName));

    try {
      clusterManager.getSession().execute(createIndexStatement.getQueryString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Creating the secondary index on the %s column of the %s table failed",
              columnName, getFullTableName(namespace, table)),
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
              "Dropping the secondary index on the %s column of the %s table failed",
              columnName, getFullTableName(namespace, table)),
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
      throw new ExecutionException(
          String.format(
              "Getting a table metadata for the %s table failed",
              getFullTableName(namespace, table)),
          e);
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
        "Import-related functionality is not supported in Cassandra");
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType) {
    throw new UnsupportedOperationException(
        "Import-related functionality is not supported in Cassandra");
  }

  @Override
  public void importTable(String namespace, String table) {
    throw new UnsupportedOperationException(
        "Import-related functionality is not supported in Cassandra");
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
      throw new ExecutionException(
          String.format("Retrieving the table names of the %s keyspace failed", namespace), e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    try {
      if (clusterManager.getMetadata(metadataKeyspace, NAMESPACES_TABLE) == null) {
        return false;
      }

      String query =
          QueryBuilder.select(NAMESPACES_NAME_COL)
              .from(quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
              .where(QueryBuilder.eq(NAMESPACES_NAME_COL, quoteIfNecessary(namespace)))
              .toString();
      ResultSet resultSet = clusterManager.getSession().execute(query);

      return resultSet.one() != null;
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Checking if the %s keyspace exists failed", namespace), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(namespace, table, metadata, true, options);
      createSecondaryIndexes(namespace, table, metadata.getSecondaryIndexNames(), true);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          String.format("Repairing the %s table failed", getFullTableName(namespace, table)), e);
    }
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
              "Adding the new %s column to the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      if (clusterManager.getMetadata(metadataKeyspace, NAMESPACES_TABLE) == null) {
        return Collections.emptySet();
      }

      Set<String> keyspaceNames = new HashSet<>();
      String selectQuery =
          QueryBuilder.select(NAMESPACES_NAME_COL)
              .from(quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
              .getQueryString();
      for (Row row : clusterManager.getSession().execute(selectQuery).all()) {
        keyspaceNames.add(row.getString(NAMESPACES_NAME_COL));
      }

      return keyspaceNames;
    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the existing keyspace names failed", e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      createKeyspace(namespace, options, true);
      createKeyspace(metadataKeyspace, options, true);
      createNamespacesTableIfNotExists();
      upsertIntoNamespacesTable(namespace);
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Repairing the %s keyspace failed", namespace), e);
    }
  }

  private void createNamespacesTableIfNotExists() {
    String createTableQuery =
        SchemaBuilder.createTable(
                quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
            .ifNotExists()
            .addPartitionKey(NAMESPACES_NAME_COL, com.datastax.driver.core.DataType.text())
            .getQueryString();
    clusterManager.getSession().execute(createTableQuery);
  }

  private void dropNamespacesTableIfEmpty() {
    String selectQuery =
        QueryBuilder.select(NAMESPACES_NAME_COL)
            .from(quoteIfNecessary(metadataKeyspace), quoteIfNecessary(NAMESPACES_TABLE))
            .limit(1)
            .getQueryString();
    boolean isKeyspacesTableEmpty = clusterManager.getSession().execute(selectQuery).one() == null;
    if (isKeyspacesTableEmpty) {
      dropKeyspace(metadataKeyspace);
    }
  }

  @VisibleForTesting
  void createTableInternal(
      String keyspace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    Create createTable =
        SchemaBuilder.createTable(quoteIfNecessary(keyspace), quoteIfNecessary(table));
    if (ifNotExists) {
      createTable = createTable.ifNotExists();
    }
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
          String.format("Creating the %s table failed", getFullTableName(keyspace, table)), e);
    }
  }

  @VisibleForTesting
  void createSecondaryIndexes(
      String keyspace, String table, Set<String> secondaryIndexNames, boolean ifNotExists)
      throws ExecutionException {
    for (String name : secondaryIndexNames) {
      createIndexInternal(keyspace, table, name, ifNotExists);
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
          String.format("The %s network strategy does not exist", text));
    }

    @Override
    public String toString() {
      return strategyName;
    }
  }
}
