package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAdminTest {

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
    cassandraAdmin = new CassandraAdmin(clusterManager);
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
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.REPLICATION_STRATEGY, ReplicationStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "3");

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void createNamespace_UseNetworkTopologyStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.REPLICATION_STRATEGY,
        ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "5");

    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    replicationOptions.put("dc1", "5");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
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
    replicationOptions.put("replication_factor", "1");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(namespace).with().replication(replicationOptions);

    verify(cassandraSession).execute(query.getQueryString());
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
    replicationOptions.put("replication_factor", "1");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(quote(namespace)).with().replication(replicationOptions);

    verify(cassandraSession).execute(query.getQueryString());
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
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, new HashMap<>());

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
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, options);

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
    cassandraAdmin.createTableInternal(namespace, table, tableMetadata, options);

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
    cassandraAdmin.createSecondaryIndexes(namespace, table, indexes, Collections.emptyMap());

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
    cassandraAdmin.createSecondaryIndexes(namespace, table, indexes, Collections.emptyMap());

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

    // Act
    cassandraAdmin.dropTable(namespace, table);

    // Assert
    String dropTableStatement = SchemaBuilder.dropTable(namespace, table).getQueryString();
    verify(cassandraSession).execute(dropTableStatement);
  }

  @Test
  public void dropTable_WithReservedKeywords_ShouldDropTable() throws ExecutionException {
    // Arrange
    String namespace = "keyspace";
    String table = "table";

    // Act
    cassandraAdmin.dropTable(namespace, table);

    // Assert
    String dropTableStatement =
        SchemaBuilder.dropTable(quote(namespace), quote(table)).getQueryString();
    verify(cassandraSession).execute(dropTableStatement);
  }

  @Test
  public void dropNamespace_WithCorrectParameters_ShouldDropKeyspace() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";

    // Act
    cassandraAdmin.dropNamespace(namespace);

    // Assert
    String dropKeyspaceStatement = SchemaBuilder.dropKeyspace(namespace).getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
  }

  @Test
  public void dropNamespace_WithReservedKeywordNamespace_ShouldDropKeyspace()
      throws ExecutionException {
    // Arrange
    String namespace = "keyspace";

    // Act
    cassandraAdmin.dropNamespace(namespace);

    // Assert
    String dropKeyspaceStatement = SchemaBuilder.dropKeyspace(quote(namespace)).getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
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
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspace);

    // Act
    // Assert
    assertThat(cassandraAdmin.namespaceExists(namespace)).isTrue();

    verify(metadata).getKeyspace(namespace);
  }

  @Test
  public void namespaceExists_WithSystemNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThat(cassandraAdmin.namespaceExists(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME))
        .isTrue();
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
  public void repairTable_WithExistingTable_ShouldDoNothing() {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    com.datastax.driver.core.TableMetadata tableMetadata1 =
        mock(com.datastax.driver.core.TableMetadata.class);
    when(tableMetadata1.getName()).thenReturn(table);
    List<com.datastax.driver.core.TableMetadata> tableMetadataList =
        ImmutableList.of(tableMetadata1);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTables()).thenReturn(tableMetadataList);

    // Act
    assertThatCode(
            () ->
                cassandraAdmin.repairTable(
                    namespace, table, mock(TableMetadata.class), Collections.emptyMap()))
        .doesNotThrowAnyException();

    // Assert
    verify(metadata).getKeyspace(namespace);
  }

  @Test
  public void repairTable_WithNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTables()).thenReturn(ImmutableList.of());

    // Act
    assertThatThrownBy(
            () ->
                cassandraAdmin.repairTable(
                    namespace, table, mock(TableMetadata.class), Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(metadata).getKeyspace(namespace);
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

  @ParameterizedTest
  @ValueSource(strings = {"my_system_ns", ""})
  public void getNamespaceNames_withExistingTables_ShouldWorkProperly(String systemNamespaceName)
      throws ExecutionException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(CassandraConfig.SYSTEM_NAMESPACE_NAME, systemNamespaceName);
    cassandraAdmin = new CassandraAdmin(clusterManager, new DatabaseConfig(properties));

    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspace1 = mock(KeyspaceMetadata.class);
    KeyspaceMetadata keyspace3 = mock(KeyspaceMetadata.class);
    KeyspaceMetadata keyspace4 = mock(KeyspaceMetadata.class);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspaces()).thenReturn(ImmutableList.of(keyspace1, keyspace3, keyspace4));
    when(keyspace1.getName()).thenReturn("system_foo");
    when(keyspace3.getName()).thenReturn("ks1");
    when(keyspace4.getName()).thenReturn("ks2");

    // Act
    Set<String> actual = cassandraAdmin.getNamespaceNames();

    // Assert
    if (systemNamespaceName.isEmpty()) {
      assertThat(actual).containsOnly("ks1", "ks2", DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      assertThat(actual).containsOnly("ks1", "ks2", systemNamespaceName);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"my_system_ns", ""})
  public void getNamespaceNames_withoutExistingTables_ShouldReturnSystemNamespaceOnly(
      String systemNamespaceName) throws ExecutionException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(CassandraConfig.SYSTEM_NAMESPACE_NAME, systemNamespaceName);
    cassandraAdmin = new CassandraAdmin(clusterManager, new DatabaseConfig(properties));

    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);

    when(cassandraSession.getCluster()).thenReturn(cluster);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspaces()).thenReturn(Collections.emptyList());

    // Act
    Set<String> actual = cassandraAdmin.getNamespaceNames();

    // Assert
    if (systemNamespaceName.isEmpty()) {
      assertThat(actual).containsOnly(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      assertThat(actual).containsOnly(systemNamespaceName);
    }
  }
}
