package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quote;
import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAdminTest {

  private static final String METADATA_KEYSPACE = "scalardb";

  private CassandraAdmin cassandraAdmin;
  @Mock private ClusterManager clusterManager;
  @Mock private Session cassandraSession;
  @Mock private Cluster cluster;
  @Mock private Metadata metadata;
  @Mock private KeyspaceMetadata keyspaceMetadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(clusterManager.getSession()).thenReturn(cassandraSession);
    Properties cassandraConfigProperties = new Properties();
    cassandraConfigProperties.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, METADATA_KEYSPACE);
    cassandraAdmin =
        new CassandraAdmin(clusterManager, new DatabaseConfig(cassandraConfigProperties));
  }

  @Test
  public void getTableMetadata_ClusterManagerShouldBeCalledProperly() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";

    // Act
    cassandraAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(clusterManager).getMetadata(namespace, table);
  }

  @Test
  public void createNamespace_UnknownNetworkStrategyOption_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.REPLICATION_STRATEGY, "xxx_strategy");

    // Act
    // Assert
    Assertions.assertThatThrownBy(() -> cassandraAdmin.createNamespace(namespace, options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_UseSimpleStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String replicationFactor = "3";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.REPLICATION_STRATEGY, ReplicationStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", replicationFactor);
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void createNamespace_UseNetworkTopologyStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String replicationFactor = "5";

    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.REPLICATION_STRATEGY,
        ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());

    options.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    replicationOptions.put("dc1", replicationFactor);
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void
      createNamespace_WithoutReplicationStrategyNorReplicationFactor_ShouldExecuteCreateKeyspaceStatementWithSimpleStrategy()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void createNamespace_WithReservedKeywords_ShouldExecuteCreateKeyspaceStatementProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    Map<String, String> options = Collections.emptyMap();

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(quote(namespace)).with().replication(replicationOptions);

    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  private void verifyCreateMetadataKeyspaceQuery(Map<String, Object> replicationOptions) {
    CreateKeyspace query =
        SchemaBuilder.createKeyspace(quoteIfNecessary(METADATA_KEYSPACE)).ifNotExists();
    String queryString = query.with().replication(replicationOptions).getQueryString();
    verify(cassandraSession).execute(queryString);
  }

  private void verifyCreateKeyspacesTableQuery() {
    String query =
        SchemaBuilder.createTable(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .addPartitionKey(
                CassandraAdmin.NAMESPACES_NAME_COL, com.datastax.driver.core.DataType.text())
            .getQueryString();
    verify(cassandraSession).execute(query);
  }

  private void verifyInsertIntoKeyspacesTableQuery(String keyspace) {
    String query =
        QueryBuilder.insertInto(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .value(CassandraAdmin.NAMESPACES_NAME_COL, quoteIfNecessary(keyspace))
            .toString();
    verify(cassandraSession).execute(query);
  }

  @Test
  public void
      createTableInternal_WithoutSettingCompactionStrategy_ShouldExecuteCreateTableStatementWithStcs()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c4")
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.BIGINT)
            .addColumn("c5", DataType.BOOLEAN)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();

    // Act
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, false, new HashMap<>());

    // Assert
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(namespace, table)
            .addPartitionKey("c1", com.datastax.driver.core.DataType.cint())
            .addClusteringColumn("c4", com.datastax.driver.core.DataType.bigint())
            .addColumn("c2", com.datastax.driver.core.DataType.text())
            .addColumn("c3", com.datastax.driver.core.DataType.blob())
            .addColumn("c5", com.datastax.driver.core.DataType.cboolean())
            .addColumn("c6", com.datastax.driver.core.DataType.cdouble())
            .addColumn("c7", com.datastax.driver.core.DataType.cfloat())
            .addColumn("c8", com.datastax.driver.core.DataType.date())
            .addColumn("c9", com.datastax.driver.core.DataType.time())
            .addColumn("c10", com.datastax.driver.core.DataType.timestamp())
            .withOptions()
            .clusteringOrder("c4", Direction.ASC)
            .compactionOptions(SchemaBuilder.sizedTieredStategy());
    verify(cassandraSession).execute(createTableStatement.getQueryString());
  }

  @Test
  public void
      createTableInternal_WithLcsCompaction_ShouldExecuteCreateTableStatementWithLcsCompaction()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addPartitionKey("c7")
            .addClusteringKey("c4")
            .addClusteringKey("c6", Order.DESC)
            .addClusteringKey("c9", Order.ASC)
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.DOUBLE)
            .addColumn("c5", DataType.BIGINT)
            .addColumn("c6", DataType.BOOLEAN)
            .addColumn("c7", DataType.TEXT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();
    HashMap<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.COMPACTION_STRATEGY, CompactionStrategy.LCS.toString());

    // Act
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, false, options);

    // Assert
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(namespace, table)
            .addPartitionKey("c1", com.datastax.driver.core.DataType.cint())
            .addPartitionKey("c7", com.datastax.driver.core.DataType.text())
            .addClusteringColumn("c4", com.datastax.driver.core.DataType.cdouble())
            .addClusteringColumn("c6", com.datastax.driver.core.DataType.cboolean())
            .addClusteringColumn("c9", com.datastax.driver.core.DataType.time())
            .addColumn("c2", com.datastax.driver.core.DataType.text())
            .addColumn("c3", com.datastax.driver.core.DataType.blob())
            .addColumn("c5", com.datastax.driver.core.DataType.bigint())
            .addColumn("c8", com.datastax.driver.core.DataType.date())
            .addColumn("c10", com.datastax.driver.core.DataType.timestamp())
            .withOptions()
            .clusteringOrder("c4", Direction.ASC)
            .clusteringOrder("c6", Direction.DESC)
            .clusteringOrder("c9", Direction.ASC)
            .compactionOptions(SchemaBuilder.leveledStrategy());
    verify(cassandraSession).execute(createTableStatement.getQueryString());
  }

  @Test
  public void createTableInternal_ReservedKeywords_ShouldExecuteCreateTableStatementProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    String table = "table";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("from")
            .addPartitionKey("c7")
            .addClusteringKey("two")
            .addClusteringKey("c6", Order.DESC)
            .addColumn("from", DataType.INT)
            .addColumn("to", DataType.TEXT)
            .addColumn("one", DataType.BLOB)
            .addColumn("two", DataType.DOUBLE)
            .addColumn("password", DataType.BIGINT)
            .addColumn("c6", DataType.BOOLEAN)
            .addColumn("c7", DataType.TEXT)
            .addSecondaryIndex("to")
            .addSecondaryIndex("two")
            .build();
    HashMap<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.COMPACTION_STRATEGY, CompactionStrategy.LCS.toString());

    // Act
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, false, options);

    // Assert
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(quote(namespace), quote(table))
            .addPartitionKey("\"from\"", com.datastax.driver.core.DataType.cint())
            .addPartitionKey("c7", com.datastax.driver.core.DataType.text())
            .addClusteringColumn("\"two\"", com.datastax.driver.core.DataType.cdouble())
            .addClusteringColumn("c6", com.datastax.driver.core.DataType.cboolean())
            .addColumn("\"to\"", com.datastax.driver.core.DataType.text())
            .addColumn("\"one\"", com.datastax.driver.core.DataType.blob())
            .addColumn("\"password\"", com.datastax.driver.core.DataType.bigint())
            .withOptions()
            .clusteringOrder("\"two\"", Direction.ASC)
            .clusteringOrder("c6", Direction.DESC)
            .compactionOptions(SchemaBuilder.leveledStrategy());
    verify(cassandraSession).execute(createTableStatement.getQueryString());
  }

  @Test
  public void createSecondaryIndex_WithTwoIndexesNames_ShouldCreateBothIndexes()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";
    Set<String> indexes = new HashSet<>();
    indexes.add("c1");
    indexes.add("c5");

    // Act
    cassandraAdmin.createSecondaryIndexes(namespace, table, indexes, false);

    // Assert
    SchemaStatement c1IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_c1")
            .onTable(namespace, table)
            .andColumn("c1");
    SchemaStatement c5IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_c5")
            .onTable(namespace, table)
            .andColumn("c5");
    verify(cassandraSession).execute(c1IndexStatement.getQueryString());
    verify(cassandraSession).execute(c5IndexStatement.getQueryString());
  }

  @Test
  public void createSecondaryIndex_WithReservedKeywordsIndexesNames_ShouldCreateBothIndexes()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";
    Set<String> indexes = new HashSet<>();
    indexes.add("from");
    indexes.add("to");

    // Act
    cassandraAdmin.createSecondaryIndexes(namespace, table, indexes, false);

    // Assert
    SchemaStatement c1IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_from")
            .onTable(namespace, table)
            .andColumn("\"from\"");
    SchemaStatement c2IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_to")
            .onTable(namespace, table)
            .andColumn("\"to\"");
    verify(cassandraSession).execute(c1IndexStatement.getQueryString());
    verify(cassandraSession).execute(c2IndexStatement.getQueryString());
  }

  @Test
  public void dropTable_WithCorrectParameters_ShouldDropTable() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";

    ResultSet selectQueryResult = mock(ResultSet.class);
    Row row1 = mock(Row.class);
    when(row1.getString("name")).thenReturn(METADATA_KEYSPACE);
    Row row2 = mock(Row.class);
    when(row2.getString("name")).thenReturn(namespace);
    when(selectQueryResult.all()).thenReturn(Arrays.asList(row1, row2));
    when(cassandraSession.execute(anyString())).thenReturn(null, selectQueryResult);

    // Act
    cassandraAdmin.dropTable(namespace, table);

    // Assert
    String dropTableStatement = SchemaBuilder.dropTable(namespace, table).getQueryString();
    verify(cassandraSession).execute(dropTableStatement);
    verifySelectTwoFromKeyspacesTableQuery();
  }

  @Test
  public void dropTable_WithReservedKeywords_ShouldDropTable() throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    String table = "table";

    ResultSet selectQueryResult = mock(ResultSet.class);
    Row row1 = mock(Row.class);
    when(row1.getString("name")).thenReturn(METADATA_KEYSPACE);
    Row row2 = mock(Row.class);
    when(row2.getString("name")).thenReturn(namespace);
    when(selectQueryResult.all()).thenReturn(Arrays.asList(row1, row2));
    when(cassandraSession.execute(anyString())).thenReturn(null, selectQueryResult);

    // Act
    cassandraAdmin.dropTable(namespace, table);

    // Assert
    String dropTableStatement =
        SchemaBuilder.dropTable(quote(namespace), quote(table)).getQueryString();
    verify(cassandraSession).execute(dropTableStatement);
    verifySelectTwoFromKeyspacesTableQuery();
  }

  @Test
  public void
      dropNamespace_WithCorrectParametersWithOnlyMetadataKeyspacesLeft_ShouldDropKeyspaceAndMetadataKeyspace()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    ResultSet selectQueryResult = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("name")).thenReturn(METADATA_KEYSPACE);
    when(selectQueryResult.all()).thenReturn(Collections.singletonList(row));
    when(cassandraSession.execute(anyString())).thenReturn(null, null, selectQueryResult, null);

    com.datastax.driver.core.TableMetadata tableMetadata =
        mock(com.datastax.driver.core.TableMetadata.class);
    when(tableMetadata.getName()).thenReturn(CassandraAdmin.NAMESPACES_TABLE);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTables()).thenReturn(Collections.singletonList(tableMetadata));

    // Act
    cassandraAdmin.dropNamespace(namespace);

    // Assert
    String dropKeyspaceStatement = SchemaBuilder.dropKeyspace(namespace).getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
    verifyDeleteFromKeyspacesTableQuery(namespace);
    verifySelectTwoFromKeyspacesTableQuery();
    verifyGetTablesInMetadataKeyspace();
    verifyDropMetadataKeyspaceQuery();
  }

  @Test
  public void dropNamespace_WithCorrectParametersWithSomeKeyspacesLeft_ShouldOnlyDropKeyspace()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    ResultSet selectQueryResult = mock(ResultSet.class);
    when(selectQueryResult.all()).thenReturn(Arrays.asList(mock(Row.class), mock(Row.class)));
    when(cassandraSession.execute(anyString())).thenReturn(null, null, selectQueryResult, null);

    // Act
    cassandraAdmin.dropNamespace(namespace);

    // Assert
    String dropKeyspaceStatement = SchemaBuilder.dropKeyspace(namespace).getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
    verifyDeleteFromKeyspacesTableQuery(namespace);
    verifySelectTwoFromKeyspacesTableQuery();
  }

  @Test
  public void dropNamespace_WithReservedKeywordNamespace_ShouldDropKeyspace()
      throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    ResultSet selectQueryResult = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(row.getString("name")).thenReturn(METADATA_KEYSPACE);
    when(selectQueryResult.all()).thenReturn(Collections.singletonList(row));
    when(cassandraSession.execute(anyString())).thenReturn(null, null, selectQueryResult, null);

    com.datastax.driver.core.TableMetadata tableMetadata =
        mock(com.datastax.driver.core.TableMetadata.class);
    when(tableMetadata.getName()).thenReturn(CassandraAdmin.NAMESPACES_TABLE);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTables()).thenReturn(Collections.singletonList(tableMetadata));

    // Act
    cassandraAdmin.dropNamespace(namespace);

    // Assert
    String dropKeyspaceStatement = SchemaBuilder.dropKeyspace(quote(namespace)).getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
    verifyDeleteFromKeyspacesTableQuery(namespace);
    verifySelectTwoFromKeyspacesTableQuery();
    verifyGetTablesInMetadataKeyspace();
    verifyDropMetadataKeyspaceQuery();
  }

  private void verifyDeleteFromKeyspacesTableQuery(String keyspace) {
    String query =
        QueryBuilder.delete()
            .from(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .where(QueryBuilder.eq(CassandraAdmin.NAMESPACES_NAME_COL, quoteIfNecessary(keyspace)))
            .toString();
    verify(cassandraSession).execute(query);
  }

  private void verifySelectTwoFromKeyspacesTableQuery() {
    String query =
        QueryBuilder.select(CassandraAdmin.NAMESPACES_NAME_COL)
            .from(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .limit(2)
            .getQueryString();
    verify(cassandraSession).execute(query);
  }

  private void verifyGetTablesInMetadataKeyspace() {
    verify(cassandraSession).getCluster();
    verify(cluster).getMetadata();
    verify(metadata).getKeyspace(METADATA_KEYSPACE);
    verify(keyspaceMetadata).getTables();
  }

  private void verifyDropMetadataKeyspaceQuery() {
    String query = SchemaBuilder.dropKeyspace(quoteIfNecessary(METADATA_KEYSPACE)).getQueryString();
    verify(cassandraSession).execute(query);
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnTableNames() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    List<String> expectedTableNames = ImmutableList.of("t1", "t2");
    com.datastax.driver.core.TableMetadata tableMetadata1 =
        mock(com.datastax.driver.core.TableMetadata.class);
    when(tableMetadata1.getName()).thenReturn("t1");
    com.datastax.driver.core.TableMetadata tableMetadata2 =
        mock(com.datastax.driver.core.TableMetadata.class);
    when(tableMetadata2.getName()).thenReturn("t2");
    List<com.datastax.driver.core.TableMetadata> tableMetadataList =
        ImmutableList.of(tableMetadata1, tableMetadata2);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTables()).thenReturn(tableMetadataList);

    // Act
    Set<String> actualTableNames = cassandraAdmin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).isEqualTo(ImmutableSet.copyOf(expectedTableNames));
  }

  @Test
  public void truncateTable_WithCorrectParameters_ShouldTruncateTable() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";

    // Act
    cassandraAdmin.truncateTable(namespace, table);

    // Assert
    String truncateTableStatement = QueryBuilder.truncate(namespace, table).getQueryString();
    verify(cassandraSession).execute(truncateTableStatement);
  }

  @Test
  public void truncateTable_WithReservedKeywordsParameters_ShouldTruncateTable()
      throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    String table = "table";

    // Act
    cassandraAdmin.truncateTable(namespace, table);

    // Assert
    String truncateTableStatement =
        QueryBuilder.truncate(quote(namespace), quote(table)).getQueryString();
    verify(cassandraSession).execute(truncateTableStatement);
  }

  @Test
  public void namespaceExists_WithExistingNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    when(clusterManager.getMetadata(anyString(), anyString()))
        .thenReturn(mock(com.datastax.driver.core.TableMetadata.class));
    ResultSet resultSet = mock(ResultSet.class);
    when(cassandraSession.execute(anyString())).thenReturn(resultSet);
    when(resultSet.one()).thenReturn(mock(Row.class));

    // Act
    // Assert
    assertThat(cassandraAdmin.namespaceExists(namespace)).isTrue();

    verify(clusterManager).getMetadata(METADATA_KEYSPACE, CassandraAdmin.NAMESPACES_TABLE);
    String query =
        QueryBuilder.select(CassandraAdmin.NAMESPACES_NAME_COL)
            .from(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .where(QueryBuilder.eq(CassandraAdmin.NAMESPACES_NAME_COL, quoteIfNecessary(namespace)))
            .toString();
    verify(cassandraSession).execute(query);
  }

  @Test
  public void namespaceExists_WithNonExistingNamespace_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    when(clusterManager.getMetadata(anyString(), anyString()))
        .thenReturn(mock(com.datastax.driver.core.TableMetadata.class));
    ResultSet resultSet = mock(ResultSet.class);
    when(cassandraSession.execute(anyString())).thenReturn(resultSet);
    when(resultSet.one()).thenReturn(null);

    // Act
    // Assert
    assertThat(cassandraAdmin.namespaceExists(namespace)).isFalse();

    verify(clusterManager).getMetadata(METADATA_KEYSPACE, CassandraAdmin.NAMESPACES_TABLE);
    String query =
        QueryBuilder.select(CassandraAdmin.NAMESPACES_NAME_COL)
            .from(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .where(QueryBuilder.eq(CassandraAdmin.NAMESPACES_NAME_COL, quoteIfNecessary(namespace)))
            .toString();
    verify(cassandraSession).execute(query);
  }

  @Test
  public void namespaceExists_WithNonExistingKeyspacesTable_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    when(clusterManager.getMetadata(anyString(), anyString())).thenReturn(null);

    // Act
    // Assert
    assertThat(cassandraAdmin.namespaceExists(namespace)).isFalse();

    verify(clusterManager).getMetadata(METADATA_KEYSPACE, CassandraAdmin.NAMESPACES_TABLE);
  }

  @Test
  public void createIndex_ShouldExecuteProperCql() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String columnName = "col";

    // Act
    cassandraAdmin.createIndex(namespace, table, columnName, Collections.emptyMap());

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(cassandraSession).execute(captor.capture());
    assertThat(captor.getValue().trim())
        .isEqualTo("CREATE INDEX tbl_index_col ON sample_ns.tbl(col)");
  }

  @Test
  public void dropIndex_ShouldExecuteProperCql() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String columnName = "col";

    // Act
    cassandraAdmin.dropIndex(namespace, table, columnName);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(cassandraSession).execute(captor.capture());
    assertThat(captor.getValue().trim()).isEqualTo("DROP INDEX sample_ns.tbl_index_col");
  }

  @Test
  public void repairTable_ShouldCreateTableAndIndexesIfTheyDoNotExists() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c4")
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.INT)
            .addColumn("c5", DataType.BOOLEAN)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();
    // Act
    cassandraAdmin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert table creation
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(namespace, table)
            .ifNotExists()
            .addPartitionKey("c1", com.datastax.driver.core.DataType.cint())
            .addClusteringColumn("c4", com.datastax.driver.core.DataType.cint())
            .addColumn("c2", com.datastax.driver.core.DataType.text())
            .addColumn("c3", com.datastax.driver.core.DataType.blob())
            .addColumn("c5", com.datastax.driver.core.DataType.cboolean())
            .withOptions()
            .clusteringOrder("c4", Direction.ASC)
            .compactionOptions(SchemaBuilder.sizedTieredStategy());
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(cassandraSession, times(3)).execute(captor.capture());
    Assertions.assertThat(captor.getAllValues().get(0))
        .isEqualTo(createTableStatement.getQueryString());
    // Assert index creation
    Iterator<String> indexIterator = tableMetadata.getSecondaryIndexNames().iterator();
    for (int i = 1; indexIterator.hasNext(); i++) {
      String index = indexIterator.next();
      assertThat(captor.getAllValues().get(i).trim())
          .isEqualTo(
              String.format(
                  "CREATE INDEX IF NOT EXISTS tbl_index_%1$s ON sample_ns.tbl(%1$s)", index));
    }
  }

  @Test
  public void addNewColumnToTable_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String column = "c2";
    com.datastax.driver.core.TableMetadata tableMetadata =
        mock(com.datastax.driver.core.TableMetadata.class);
    ColumnMetadata c1 = mock(ColumnMetadata.class);
    when(c1.getName()).thenReturn("c1");
    when(c1.getType()).thenReturn(com.datastax.driver.core.DataType.text());
    when(tableMetadata.getPartitionKey()).thenReturn(Collections.singletonList(c1));
    when(tableMetadata.getClusteringColumns()).thenReturn(Collections.emptyList());
    when(tableMetadata.getIndexes()).thenReturn(Collections.emptyList());
    when(tableMetadata.getColumns()).thenReturn(Collections.singletonList(c1));
    when(clusterManager.getMetadata(any(), any())).thenReturn(tableMetadata);
    when(clusterManager.getSession()).thenReturn(cassandraSession);

    // Act
    cassandraAdmin.addNewColumnToTable(namespace, table, column, DataType.TEXT);

    // Assert
    String alterTableQuery =
        SchemaBuilder.alterTable(namespace, table)
            .addColumn(column)
            .type(com.datastax.driver.core.DataType.text())
            .getQueryString();
    verify(cassandraSession).execute(alterTableQuery);
  }

  @Test
  public void getNamespacesNames_WithNonExistingKeyspaces_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    when(clusterManager.getMetadata(anyString(), anyString())).thenReturn(null);

    // Act
    Set<String> actualKeyspaceNames = cassandraAdmin.getNamespaceNames();

    // Assert
    assertThat(actualKeyspaceNames).isEmpty();
    verify(clusterManager).getMetadata(METADATA_KEYSPACE, CassandraAdmin.NAMESPACES_TABLE);
  }

  @Test
  public void getNamespacesNames_WithExistingKeyspaces_ShouldReturnKeyspacesNames()
      throws ExecutionException {
    // Arrange
    when(clusterManager.getMetadata(anyString(), anyString()))
        .thenReturn(mock(com.datastax.driver.core.TableMetadata.class));
    ResultSet resultSet = mock(ResultSet.class);
    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(row1.getString(CassandraAdmin.NAMESPACES_NAME_COL)).thenReturn("ns1");
    when(row2.getString(CassandraAdmin.NAMESPACES_NAME_COL)).thenReturn("ns2");
    when(resultSet.all()).thenReturn(Arrays.asList(row1, row2));
    when(cassandraSession.execute(anyString())).thenReturn(resultSet);

    // Act
    Set<String> actualKeyspaceNames = cassandraAdmin.getNamespaceNames();

    // Assert
    assertThat(actualKeyspaceNames).containsOnly("ns1", "ns2");
    verify(clusterManager).getMetadata(METADATA_KEYSPACE, CassandraAdmin.NAMESPACES_TABLE);
    String selectQuery =
        QueryBuilder.select(CassandraAdmin.NAMESPACES_NAME_COL)
            .from(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .getQueryString();
    verify(cassandraSession).execute(selectQuery);
  }

  @Test
  public void unsupportedOperations_ShouldThrowUnsupportedException() {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String column = "col";

    // Act
    Throwable thrown1 =
        catchThrowable(
            () -> cassandraAdmin.getImportTableMetadata(namespace, table, Collections.emptyMap()));
    Throwable thrown2 =
        catchThrowable(
            () -> cassandraAdmin.addRawColumnToTable(namespace, table, column, DataType.INT));
    Throwable thrown3 =
        catchThrowable(
            () ->
                cassandraAdmin.importTable(
                    namespace, table, Collections.emptyMap(), Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown2).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown3).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void repairNamespace_WithoutOptions_ShouldCreateNamespaceIfNotExistsAndUpsertMetadata()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";

    // Act
    cassandraAdmin.repairNamespace(namespace, Collections.emptyMap());

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void
      repairNamespace_WithReplicationFactorAndSimpleStategyAsOptions_ShouldCreateNamespaceIfNotExistsAndUpsertMetadata()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = ImmutableMap.of(CassandraAdmin.REPLICATION_FACTOR, "5");

    // Act
    cassandraAdmin.repairNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "5");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void
      repairNamespace_WithNetworkTopologyStrategyAsOptions_ShouldCreateNamespaceIfNotExistsAndUpsertMetadata()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options =
        ImmutableMap.of(
            CassandraAdmin.REPLICATION_STRATEGY,
            ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());

    // Act
    cassandraAdmin.repairNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    replicationOptions.put("dc1", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verifyInsertIntoKeyspacesTableQuery(namespace);
  }

  @Test
  public void upgrade_withExistingUserKeyspaces_ShouldUpsertKeyspacesIntoNamespacesTable()
      throws ExecutionException {
    // Arrange
    String replicationFactor = "5";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.REPLICATION_STRATEGY, ReplicationStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspace1 = mock(KeyspaceMetadata.class);
    KeyspaceMetadata keyspace2 = mock(KeyspaceMetadata.class);
    KeyspaceMetadata keyspace3 = mock(KeyspaceMetadata.class);
    KeyspaceMetadata keyspace4 = mock(KeyspaceMetadata.class);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspaces())
        .thenReturn(ImmutableList.of(keyspace1, keyspace2, keyspace3, keyspace4));
    when(keyspace1.getName()).thenReturn("system_foo");
    when(keyspace2.getName()).thenReturn(METADATA_KEYSPACE);
    when(keyspace3.getName()).thenReturn("ks1");
    when(keyspace4.getName()).thenReturn("ks2");

    // Act
    cassandraAdmin.upgrade(options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", replicationFactor);
    verifyCreateMetadataKeyspaceQuery(replicationOptions);
    verifyCreateKeyspacesTableQuery();
    verifyInsertIntoKeyspacesTableQuery(METADATA_KEYSPACE);
    verify(clusterManager, times(6)).getSession();
    String upsertKs1Query =
        QueryBuilder.insertInto(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .value(CassandraAdmin.NAMESPACES_NAME_COL, "ks1")
            .toString();

    String upsertKs2Query =
        QueryBuilder.insertInto(
                quoteIfNecessary(METADATA_KEYSPACE),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .value(CassandraAdmin.NAMESPACES_NAME_COL, "ks2")
            .toString();
    verify(cassandraSession).execute(upsertKs1Query);
    verify(cassandraSession).execute(upsertKs2Query);
  }
}
