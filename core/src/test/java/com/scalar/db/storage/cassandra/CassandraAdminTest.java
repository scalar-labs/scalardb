package com.scalar.db.storage.cassandra;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.NetworkStrategy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAdminTest {

  private static final String SAMPLE_PREFIX = "sample_prefix_";
  private CassandraAdmin cassandraAdmin;
  @Mock private CassandraTableMetadataManager metadataManager;
  @Mock private ClusterManager clusterManager;
  @Mock private Session cassandraSession;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(clusterManager.getSession()).thenReturn(cassandraSession);
    cassandraAdmin =
        new CassandraAdmin(metadataManager, clusterManager, Optional.of(SAMPLE_PREFIX));
  }

  @Test
  public void
      getTableMetadata_ConstructedWithoutNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
          throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.empty();
    String namespace = "ns";
    String table = "table";

    CassandraAdmin admin = new CassandraAdmin(metadataManager, clusterManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
      throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.of("prefix");
    String namespace = "ns";
    String table = "table";

    CassandraAdmin admin = new CassandraAdmin(metadataManager, clusterManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void createNamespace_UnknownNetworkStrategyOption_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.NETWORK_STRATEGY, "xxx_strategy");

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> cassandraAdmin.createKeyspaceIfNotExists(namespace, options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_UseSimpleStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.NETWORK_STRATEGY, NetworkStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "3");

    // Act
    cassandraAdmin.createKeyspaceIfNotExists(SAMPLE_PREFIX + namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", NetworkStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void createNamespace_UseNetworkTopologyStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.NETWORK_STRATEGY, NetworkStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "5");

    // Act
    cassandraAdmin.createKeyspaceIfNotExists(SAMPLE_PREFIX + namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", NetworkStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    replicationOptions.put("dc1_name", "5");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void
      createNamespace_WithoutStrategyNorReplicationFactor_ShouldExecuteCreateKeyspaceStatementWithSimpleStrategy()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();

    // Act
    cassandraAdmin.createKeyspaceIfNotExists(SAMPLE_PREFIX + namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", NetworkStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "1");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);

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
            .addColumn("c4", DataType.INT)
            .addColumn("c5", DataType.BOOLEAN)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();
    // Act
    cassandraAdmin.createTableInternal(
        SAMPLE_PREFIX + namespace, table, tableMetadata, new HashMap<>());

    // Assert
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(SAMPLE_PREFIX + namespace, table)
            .addPartitionKey("c1", com.datastax.driver.core.DataType.cint())
            .addClusteringColumn("c4", com.datastax.driver.core.DataType.cint())
            .addColumn("c2", com.datastax.driver.core.DataType.text())
            .addColumn("c3", com.datastax.driver.core.DataType.blob())
            .addColumn("c5", com.datastax.driver.core.DataType.cboolean())
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
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.DOUBLE)
            .addColumn("c5", DataType.BIGINT)
            .addColumn("c6", DataType.BOOLEAN)
            .addColumn("c7", DataType.TEXT)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();
    HashMap<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.COMPACTION_STRATEGY, CompactionStrategy.LCS.toString());

    // Act
    cassandraAdmin.createTableInternal(SAMPLE_PREFIX + namespace, table, tableMetadata, options);

    // Assert
    TableOptions<Options> createTableStatement =
        SchemaBuilder.createTable(SAMPLE_PREFIX + namespace, table)
            .addPartitionKey("c1", com.datastax.driver.core.DataType.cint())
            .addPartitionKey("c7", com.datastax.driver.core.DataType.text())
            .addClusteringColumn("c4", com.datastax.driver.core.DataType.cdouble())
            .addClusteringColumn("c6", com.datastax.driver.core.DataType.cboolean())
            .addColumn("c2", com.datastax.driver.core.DataType.text())
            .addColumn("c3", com.datastax.driver.core.DataType.blob())
            .addColumn("c5", com.datastax.driver.core.DataType.bigint())
            .withOptions()
            .clusteringOrder("c4", Direction.ASC)
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
    cassandraAdmin.createSecondaryIndex(SAMPLE_PREFIX + namespace, table, indexes);

    // Assert
    SchemaStatement c1IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_c1")
            .onTable(SAMPLE_PREFIX + namespace, table)
            .andColumn("c1");
    SchemaStatement c5IndexStatement =
        SchemaBuilder.createIndex(table + "_" + CassandraAdmin.INDEX_NAME_PREFIX + "_c5")
            .onTable(SAMPLE_PREFIX + namespace, table)
            .andColumn("c5");
    verify(cassandraSession).execute(c1IndexStatement.getQueryString());
    verify(cassandraSession).execute(c5IndexStatement.getQueryString());
  }

  @Test
  public void dropTable_WithCorrectParameters_ShouldDropTableAndNamespace()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";
    when(metadataManager.getTableNames(any())).thenReturn(Collections.emptySet());

    // Act
    cassandraAdmin.dropTable(namespace, table);

    // Assert
    String dropTableStatement =
        SchemaBuilder.dropTable(SAMPLE_PREFIX + namespace, table).getQueryString();
    verify(cassandraSession).execute(dropTableStatement);
    verify(metadataManager).getTableNames(namespace);
    String dropKeyspaceStatement =
        SchemaBuilder.dropKeyspace(SAMPLE_PREFIX + namespace).ifExists().getQueryString();
    verify(cassandraSession).execute(dropKeyspaceStatement);
  }

  @Test
  public void truncateTable_WithCorrectParameters_ShouldTruncateTable() throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    String table = "sample_table";

    // Act
    cassandraAdmin.truncateTable(namespace, table);

    // Assert
    String truncateTableStatement =
        QueryBuilder.truncate(SAMPLE_PREFIX + namespace, table).getQueryString();
    verify(cassandraSession).execute(truncateTableStatement);
  }
}
