package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CompositePath;
import com.azure.cosmos.models.CompositePathSortOrder;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Abstraction that defines unit tests for the {@link CosmosAdmin}. The class purpose is to be able
 * to run the {@link CosmosAdmin} unit tests with different values for the {@link CosmosConfig},
 * notably {@link CosmosConfig#TABLE_METADATA_DATABASE}.
 */
public abstract class CosmosAdminTestBase {
  @Mock private CosmosClient client;
  @Mock private CosmosConfig config;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  private CosmosAdmin admin;
  private String metadataDatabaseName;
  // The container properties mock returned by setUpRepairTableMocks, for stubbing the current
  // indexing policy in indexing-policy repair tests.
  private CosmosContainerProperties repairContainerProperties;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getTableMetadataDatabase()).thenReturn(getTableMetadataDatabaseConfig());
    admin = new CosmosAdmin(client, config);

    metadataDatabaseName = getTableMetadataDatabaseConfig().orElse("scalardb");
  }

  /**
   * This sets the {@link CosmosConfig#TABLE_METADATA_DATABASE} value that will be used to run the
   * tests.
   *
   * @return {@link CosmosConfig#TABLE_METADATA_DATABASE} value
   */
  abstract Optional<String> getTableMetadataDatabaseConfig();

  @Test
  public void getTableMetadata_ShouldReturnCorrectTableMetadata() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String fullName = getFullTableName(namespace, table);

    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> response = mock(CosmosItemResponse.class);

    when(client.getDatabase(metadataDatabaseName)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.METADATA_CONTAINER)).thenReturn(container);
    when(container.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(response);
    Map<String, String> columnsMap =
        new ImmutableMap.Builder<String, String>()
            .put("c1", "int")
            .put("c2", "text")
            .put("c3", "bigint")
            .put("c4", "boolean")
            .put("c5", "blob")
            .put("c6", "float")
            .put("c7", "double")
            .put("c8", "date")
            .put("c9", "time")
            .put("c10", "timestamp")
            .put("c11", "timestamptz")
            .build();
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .clusteringKeyNames(Sets.newLinkedHashSet("c2", "c3"))
            .clusteringOrders(ImmutableMap.of("c2", "ASC", "c3", "DESC"))
            .secondaryIndexNames(ImmutableSet.of("c4", "c9"))
            .columns(columnsMap)
            .build();

    when(response.getItem()).thenReturn(cosmosTableMetadata);

    // Act
    TableMetadata actual = admin.getTableMetadata(namespace, table);

    // Assert
    assertThat(actual)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addPartitionKey("c1")
                .addClusteringKey("c2", Order.ASC)
                .addClusteringKey("c3", Order.DESC)
                .addSecondaryIndex("c4")
                .addSecondaryIndex("c9")
                .addColumn("c1", DataType.INT)
                .addColumn("c2", DataType.TEXT)
                .addColumn("c3", DataType.BIGINT)
                .addColumn("c4", DataType.BOOLEAN)
                .addColumn("c5", DataType.BLOB)
                .addColumn("c6", DataType.FLOAT)
                .addColumn("c7", DataType.DOUBLE)
                .addColumn("c8", DataType.DATE)
                .addColumn("c9", DataType.TIME)
                .addColumn("c10", DataType.TIMESTAMP)
                .addColumn("c11", DataType.TIMESTAMPTZ)
                .build());

    verify(client).getDatabase(metadataDatabaseName);
    verify(database).getContainer(CosmosAdmin.METADATA_CONTAINER);
    verify(container).readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class);
    verify(response).getItem();
  }

  @Test
  public void createNamespace_WithCustomRuBelow4000_ShouldCreateDatabaseWithManualThroughput()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "2000";

    // Act
    admin.createNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
  }

  @Test
  public void createNamespace_WithCustomRuEqualTo4000_ShouldCreateDatabaseWithAutoscaledThroughput()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";

    // Act
    admin.createNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createAutoscaledThroughput(Integer.parseInt(throughput))));
  }

  @Test
  public void
      createNamespace_WithCustomRuEqualTo4000AndNoScaling_ShouldCreateDatabaseWithManualThroughput()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";
    String noScaling = "true";

    // Act
    admin.createNamespace(
        namespace,
        ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, throughput, CosmosAdmin.NO_SCALING, noScaling));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
  }

  @Test
  public void createTable_ShouldCreateContainer() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1", Order.DESC)
            .addClusteringKey("c2", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c4")
            .build();

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    ArgumentCaptor<CosmosContainerProperties> containerPropertiesCaptor =
        ArgumentCaptor.forClass(CosmosContainerProperties.class);

    verify(database).createContainer(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId()).isEqualTo(table);

    // check index related info
    IndexingPolicy indexingPolicy = containerPropertiesCaptor.getValue().getIndexingPolicy();
    assertThat(indexingPolicy.getIncludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getIncludedPaths().get(0).getPath()).isEqualTo("/values/c4/?");
    assertThat(indexingPolicy.getExcludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getExcludedPaths().get(0).getPath()).isEqualTo("/*");
    assertThat(indexingPolicy.getCompositeIndexes().size()).isEqualTo(1);
    assertThat(indexingPolicy.getCompositeIndexes().get(0).size()).isEqualTo(3);
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(0).getPath())
        .isEqualTo("/concatenatedPartitionKey");
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(0).getOrder())
        .isEqualTo(CompositePathSortOrder.ASCENDING);
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(1).getPath())
        .isEqualTo("/clusteringKey/c1");
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(1).getOrder())
        .isEqualTo(CompositePathSortOrder.DESCENDING);
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(2).getPath())
        .isEqualTo("/clusteringKey/c2");
    assertThat(indexingPolicy.getCompositeIndexes().get(0).get(2).getOrder())
        .isEqualTo(CompositePathSortOrder.ASCENDING);

    verify(cosmosScripts).createStoredProcedure(any(CosmosStoredProcedureProperties.class));

    // for metadata table
    verify(client)
        .createDatabaseIfNotExists(
            eq(metadataDatabaseName),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("400"))));
    verify(metadataDatabase).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId())
        .isEqualTo(CosmosAdmin.METADATA_CONTAINER);
    assertThat(containerPropertiesCaptor.getValue().getPartitionKeyDefinition().getPaths())
        .containsExactly("/id");
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c3"))
            .clusteringKeyNames(Sets.newLinkedHashSet("c1", "c2"))
            .clusteringOrders(ImmutableMap.of("c1", "DESC", "c2", "ASC"))
            .columns(
                new ImmutableMap.Builder<String, String>()
                    .put("c1", "text")
                    .put("c2", "bigint")
                    .put("c3", "boolean")
                    .put("c4", "blob")
                    .put("c5", "int")
                    .put("c6", "double")
                    .put("c7", "float")
                    .put("c8", "date")
                    .put("c9", "time")
                    .put("c10", "timestamp")
                    .put("c11", "timestamptz")
                    .build())
            .secondaryIndexNames(ImmutableSet.of("c4"))
            .build();
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
  }

  @Test
  public void createTable_WithoutClusteringKeys_ShouldCreateContainerWithCompositeIndex()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c4")
            .build();

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    ArgumentCaptor<CosmosContainerProperties> containerPropertiesCaptor =
        ArgumentCaptor.forClass(CosmosContainerProperties.class);

    verify(database).createContainer(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId()).isEqualTo(table);

    // check index related info
    IndexingPolicy indexingPolicy = containerPropertiesCaptor.getValue().getIndexingPolicy();
    assertThat(indexingPolicy.getIncludedPaths().size()).isEqualTo(2);
    assertThat(indexingPolicy.getIncludedPaths().get(0).getPath())
        .isEqualTo("/concatenatedPartitionKey/?");
    assertThat(indexingPolicy.getIncludedPaths().get(1).getPath()).isEqualTo("/values/c4/?");
    assertThat(indexingPolicy.getExcludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getExcludedPaths().get(0).getPath()).isEqualTo("/*");
    assertThat(indexingPolicy.getCompositeIndexes()).isEmpty();

    verify(cosmosScripts).createStoredProcedure(any(CosmosStoredProcedureProperties.class));

    // for metadata table
    verify(client)
        .createDatabaseIfNotExists(
            eq(metadataDatabaseName),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("400"))));
    verify(metadataDatabase).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId())
        .isEqualTo(CosmosAdmin.METADATA_CONTAINER);
    assertThat(containerPropertiesCaptor.getValue().getPartitionKeyDefinition().getPaths())
        .containsExactly("/id");
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c3"))
            .secondaryIndexNames(ImmutableSet.of("c4"))
            .columns(
                new ImmutableMap.Builder<String, String>()
                    .put("c1", "text")
                    .put("c2", "bigint")
                    .put("c3", "boolean")
                    .put("c4", "blob")
                    .put("c5", "int")
                    .put("c6", "double")
                    .put("c7", "float")
                    .put("c8", "date")
                    .put("c9", "time")
                    .put("c10", "timestamptz")
                    .build())
            .build();
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
  }

  @Test
  public void createTable_WithBlobClusteringKey_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1")
            .addColumn("c1", DataType.BLOB)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .build();

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace, table, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropTable_WithNoMetadataLeft_ShouldDropContainerAndDeleteMetadataAndDatabase()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(metadataContainer.queryItems(anyString(), any(), eq(Object.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.empty());

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();

    // for metadata table
    verify(client, atLeastOnce()).getDatabase(metadataDatabaseName);
    verify(metadataDatabase, atLeastOnce()).getContainer(CosmosAdmin.METADATA_CONTAINER);
    String fullTable = getFullTableName(namespace, table);
    verify(metadataContainer)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(metadataContainer).delete();
    verify(metadataDatabase).delete();
  }

  @Test
  public void dropTable_WithMetadataLeft_ShouldDropContainerAndOnlyDeleteMetadata()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(metadataContainer.queryItems(anyString(), any(), eq(Object.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(new CosmosTableMetadata()));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();

    // for metadata table
    verify(client, atLeastOnce()).getDatabase(metadataDatabaseName);
    verify(metadataDatabase, atLeastOnce()).getContainer(CosmosAdmin.METADATA_CONTAINER);
    String fullTable = getFullTableName(namespace, table);
    verify(metadataContainer)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(metadataContainer, never()).delete();
    verify(metadataDatabase, never()).delete();
  }

  @Test
  public void dropNamespace_WithExistingDatabase_ShouldDropDatabase() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    when(client.getDatabase(any())).thenReturn(database);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosContainerProperties> emptyContainerIterable =
        mock(CosmosPagedIterable.class);
    when(emptyContainerIterable.stream()).thenReturn(Stream.empty());
    when(database.readAllContainers()).thenReturn(emptyContainerIterable);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(database).delete();
  }

  @Test
  public void dropNamespace_WithNonScalarDBTableLeft_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "ns";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(namespace)).thenReturn(database);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(metadataDatabase.getContainer(anyString())).thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosContainerProperties> containerPagedIterable =
        mock(CosmosPagedIterable.class);
    when(containerPagedIterable.stream())
        .thenReturn(Stream.of(mock(CosmosContainerProperties.class)));
    when(database.readAllContainers()).thenReturn(containerPagedIterable);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.queryItems(anyString(), any(), any())).thenReturn(pagedIterable);
    when(pagedIterable.stream()).thenReturn(Stream.empty());

    // Act Assert
    assertThatCode(() -> admin.dropNamespace(namespace))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void truncateTable_WithExistingRecords_ShouldDeleteAllRecords() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    when(client.getDatabase(any())).thenReturn(database);
    when(database.getContainer(any())).thenReturn(container);
    Record record1 = mock(Record.class);
    Record record2 = mock(Record.class);
    when(record1.getId()).thenReturn("id1");
    when(record1.getConcatenatedPartitionKey()).thenReturn("p1");
    when(record2.getId()).thenReturn("id2");
    when(record2.getConcatenatedPartitionKey()).thenReturn("p2");
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Record> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(Record.class))).thenReturn(queryResults);
    @SuppressWarnings("unchecked")
    Iterator<Record> mockIterator = mock(Iterator.class);
    doCallRealMethod().when(queryResults).forEach(ArgumentMatchers.<Consumer<Record>>any());
    when(queryResults.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(true, true, false);
    when(mockIterator.next()).thenReturn(record1, record2);

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(container)
        .queryItems(
            eq("SELECT t.id, t.concatenatedPartitionKey FROM " + "t"),
            any(CosmosQueryRequestOptions.class),
            eq(Record.class));
    verify(container)
        .deleteItem(eq("id1"), refEq(new PartitionKey("p1")), any(CosmosItemRequestOptions.class));
    verify(container)
        .deleteItem(eq("id2"), refEq(new PartitionKey("p2")), any(CosmosItemRequestOptions.class));
  }

  @Test
  public void getNamespaceTableNames_ShouldGetTableNamesProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";

    CosmosTableMetadata t1 =
        CosmosTableMetadata.newBuilder().id(getFullTableName(namespace, "t1")).build();
    CosmosTableMetadata t2 =
        CosmosTableMetadata.newBuilder().id(getFullTableName(namespace, "t2")).build();

    when(client.getDatabase(metadataDatabaseName)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.METADATA_CONTAINER)).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(CosmosTableMetadata.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(t1, t2));

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).containsExactly("t1", "t2");
    verify(client, atLeastOnce()).getDatabase(metadataDatabaseName);
    verify(database, atLeastOnce()).getContainer(CosmosAdmin.METADATA_CONTAINER);
    verify(container)
        .queryItems(
            eq("SELECT * FROM metadata WHERE metadata.id LIKE 'ns.%'"),
            any(CosmosQueryRequestOptions.class),
            eq(CosmosTableMetadata.class));
  }

  @Test
  public void createIndex_ShouldCreateIndexProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(namespace)).thenReturn(database);
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(database.getContainer(table)).thenReturn(container);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> itemResponse = mock(CosmosItemResponse.class);
    when(metadataContainer.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(itemResponse);

    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .secondaryIndexNames(ImmutableSet.of("c2"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();
    when(itemResponse.getItem()).thenReturn(cosmosTableMetadata);

    // Act
    admin.createIndex(namespace, table, "c3");

    // Assert
    verify(database).createContainerIfNotExists(table, "/concatenatedPartitionKey");

    ArgumentCaptor<IndexingPolicy> indexingPolicyCaptor =
        ArgumentCaptor.forClass(IndexingPolicy.class);
    verify(properties).setIndexingPolicy(indexingPolicyCaptor.capture());
    IndexingPolicy indexingPolicy = indexingPolicyCaptor.getValue();
    assertThat(indexingPolicy.getIncludedPaths().size()).isEqualTo(3);
    assertThat(indexingPolicy.getIncludedPaths().get(0).getPath())
        .isEqualTo("/concatenatedPartitionKey/?");
    assertThat(indexingPolicy.getIncludedPaths().get(1).getPath()).isEqualTo("/values/c3/?");
    assertThat(indexingPolicy.getIncludedPaths().get(2).getPath()).isEqualTo("/values/c2/?");
    assertThat(indexingPolicy.getExcludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getExcludedPaths().get(0).getPath()).isEqualTo("/*");
    assertThat(indexingPolicy.getCompositeIndexes()).isEmpty();

    verify(container).replace(properties);

    // for metadata table
    CosmosTableMetadata expected =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .secondaryIndexNames(ImmutableSet.of("c2", "c3"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();
    verify(metadataContainer).upsertItem(expected);
  }

  @Test
  public void dropIndex_ShouldDropIndexProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(namespace)).thenReturn(database);
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(database.getContainer(table)).thenReturn(container);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> itemResponse = mock(CosmosItemResponse.class);
    when(metadataContainer.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(itemResponse);

    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .secondaryIndexNames(ImmutableSet.of("c2", "c3"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();
    when(itemResponse.getItem()).thenReturn(cosmosTableMetadata);

    // Act
    admin.dropIndex(namespace, table, "c2");

    // Assert
    verify(database).createContainerIfNotExists(table, "/concatenatedPartitionKey");

    ArgumentCaptor<IndexingPolicy> indexingPolicyCaptor =
        ArgumentCaptor.forClass(IndexingPolicy.class);
    verify(properties).setIndexingPolicy(indexingPolicyCaptor.capture());
    IndexingPolicy indexingPolicy = indexingPolicyCaptor.getValue();
    assertThat(indexingPolicy.getIncludedPaths().size()).isEqualTo(2);
    assertThat(indexingPolicy.getIncludedPaths().get(0).getPath())
        .isEqualTo("/concatenatedPartitionKey/?");
    assertThat(indexingPolicy.getIncludedPaths().get(1).getPath()).isEqualTo("/values/c3/?");
    assertThat(indexingPolicy.getExcludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getExcludedPaths().get(0).getPath()).isEqualTo("/*");
    assertThat(indexingPolicy.getCompositeIndexes()).isEmpty();

    verify(container).replace(properties);

    // for metadata table
    CosmosTableMetadata expected =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .secondaryIndexNames(ImmutableSet.of("c3"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();
    verify(metadataContainer).upsertItem(expected);
  }

  @Test
  public void repairTable_withStoredProcedure_ShouldNotAddStoredProcedure()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .build();
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    CosmosScripts scripts = mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(scripts);
    CosmosStoredProcedure storedProcedure = mock(CosmosStoredProcedure.class);
    when(scripts.getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME))
        .thenReturn(storedProcedure);

    // Existing container properties (for updateIndexingPolicy)
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(properties.getIndexingPolicy()).thenReturn(new IndexingPolicy());

    // Act Assert
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(client).createDatabaseIfNotExists(eq(metadataDatabaseName), any());

    verify(metadataDatabase).createContainerIfNotExists(any());
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
    verify(storedProcedure).read();
    // The stored procedure already exists, so it must not be created again
    verify(scripts, never()).createStoredProcedure(any());
  }

  @Test
  public void repairTable_withoutStoredProcedure_ShouldCreateStoredProcedure()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .build();
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build();
    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    // Metadata container
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    // Missing stored procedure
    CosmosScripts scripts = mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(scripts);
    CosmosStoredProcedure storedProcedure = mock(CosmosStoredProcedure.class);
    CosmosException cosmosException = mock(CosmosException.class);
    when(scripts.getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME))
        .thenReturn(storedProcedure);
    when(cosmosException.getStatusCode()).thenReturn(404);
    when(storedProcedure.read()).thenThrow(cosmosException);

    // Existing container properties (for updateIndexingPolicy)
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(properties.getIndexingPolicy()).thenReturn(new IndexingPolicy());

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(client).createDatabaseIfNotExists(eq(metadataDatabaseName), any());

    verify(metadataDatabase).createContainerIfNotExists(any());
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
    verify(storedProcedure).read();
    verify(scripts).createStoredProcedure(any());
  }

  @Test
  public void repairTable_ShouldCreateTableIfNotExistsAndUpsertMetadata()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .secondaryIndexNames(ImmutableSet.of("c3"))
            .build();
    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    // Metadata container
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    // Missing stored procedure
    CosmosScripts scripts = mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(scripts);
    CosmosStoredProcedure storedProcedure = mock(CosmosStoredProcedure.class);
    CosmosException cosmosException = mock(CosmosException.class);
    when(scripts.getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME))
        .thenReturn(storedProcedure);
    when(cosmosException.getStatusCode()).thenReturn(404);
    when(storedProcedure.read()).thenThrow(cosmosException);

    // Existing container properties
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(properties.getIndexingPolicy()).thenReturn(new IndexingPolicy());

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    ArgumentCaptor<CosmosContainerProperties> containerPropertiesCaptor =
        ArgumentCaptor.forClass(CosmosContainerProperties.class);

    verify(database).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId()).isEqualTo(table);

    // check index related info
    IndexingPolicy indexingPolicy = containerPropertiesCaptor.getValue().getIndexingPolicy();
    assertThat(indexingPolicy.getIncludedPaths().size()).isEqualTo(2);
    assertThat(indexingPolicy.getIncludedPaths().get(0).getPath())
        .isEqualTo("/concatenatedPartitionKey/?");
    assertThat(indexingPolicy.getIncludedPaths().get(1).getPath()).isEqualTo("/values/c3/?");
    assertThat(indexingPolicy.getExcludedPaths().size()).isEqualTo(1);
    assertThat(indexingPolicy.getExcludedPaths().get(0).getPath()).isEqualTo("/*");
    assertThat(indexingPolicy.getCompositeIndexes().size()).isEqualTo(0);

    verify(client).createDatabaseIfNotExists(eq(metadataDatabaseName), any());

    verify(metadataDatabase).createContainerIfNotExists(any());
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
    verify(storedProcedure).read();
    verify(scripts).createStoredProcedure(any());
  }

  private CosmosContainer setUpRepairTableMocks(String namespace, String table) {
    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    // Missing stored procedure so the physical container repair path runs fully
    CosmosScripts scripts = mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(scripts);
    CosmosStoredProcedure storedProcedure = mock(CosmosStoredProcedure.class);
    CosmosException cosmosException = mock(CosmosException.class);
    when(scripts.getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME))
        .thenReturn(storedProcedure);
    when(cosmosException.getStatusCode()).thenReturn(404);
    when(storedProcedure.read()).thenThrow(cosmosException);

    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    // A real container always has an indexing policy; default to an empty one so it does not match
    // the desired policy (tests that exercise the up-to-date path override this).
    when(properties.getIndexingPolicy()).thenReturn(new IndexingPolicy());
    repairContainerProperties = properties;

    return metadataContainer;
  }

  @SuppressWarnings("unchecked")
  private void stubStoredTableMetadata(
      CosmosContainer metadataContainer, CosmosTableMetadata stored) {
    CosmosItemResponse<CosmosTableMetadata> metadataResponse = mock(CosmosItemResponse.class);
    when(metadataContainer.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(metadataResponse);
    when(metadataResponse.getItem()).thenReturn(stored);
  }

  @Test
  public void repairTable_WhenStoredMetadataEqualsDesired_ShouldNotUpsertMetadata()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .secondaryIndexNames(ImmutableSet.of("c3"))
            .build());

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: physical container repair still runs, but the metadata upsert is skipped
    verify(database).createContainerIfNotExists(any(CosmosContainerProperties.class));
    verify(metadataContainer, never()).upsertItem(any());
  }

  @Test
  public void repairTable_WhenStoredMetadataDiffersFromDesired_ShouldUpsertMetadata()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    // Stored metadata is missing the secondary index, so it differs from the desired metadata
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build());

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(metadataContainer).upsertItem(any(CosmosTableMetadata.class));
  }

  @Test
  public void repairTable_WhenIndexingPolicyAlreadyUpToDate_ShouldNotReplaceContainer()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .secondaryIndexNames(ImmutableSet.of("c3"))
            .build());
    // The container's actual indexing policy already matches the desired one
    IndexingPolicy currentPolicy = new IndexingPolicy();
    currentPolicy.setIncludedPaths(
        Arrays.asList(
            new IncludedPath("/concatenatedPartitionKey/?"), new IncludedPath("/values/c3/?")));
    when(repairContainerProperties.getIndexingPolicy()).thenReturn(currentPolicy);

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: the indexing policy is already correct, so the container is not replaced
    verify(container, never()).replace(any(CosmosContainerProperties.class));
  }

  @Test
  public void repairTable_WhenIndexingPolicyDiffers_ShouldReplaceContainer()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .secondaryIndexNames(ImmutableSet.of("c3"))
            .build());
    // The container's actual indexing policy is missing the c3 secondary index path
    IndexingPolicy currentPolicy = new IndexingPolicy();
    currentPolicy.setIncludedPaths(
        Collections.singletonList(new IncludedPath("/concatenatedPartitionKey/?")));
    when(repairContainerProperties.getIndexingPolicy()).thenReturn(currentPolicy);

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: the indexing policy is stale, so the container is replaced to fix it
    verify(container).replace(any(CosmosContainerProperties.class));
  }

  @Test
  public void repairTable_WhenReadingStoredMetadataThrows_ShouldFailOpenAndUpsertMetadata()
      throws ExecutionException {
    // Arrange: reading the current metadata throws (e.g. a corrupt record); the guard must fail
    // open and write rather than skip the metadata update
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    when(metadataContainer.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenThrow(new RuntimeException("corrupted"));

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: no exception propagates and the metadata is rewritten
    verify(metadataContainer).upsertItem(any(CosmosTableMetadata.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void repairTable_WhenStoredMetadataAbsent_ShouldUpsertMetadata()
      throws ExecutionException {
    // Arrange: no stored metadata (read returns null); the guard must write
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    CosmosItemResponse<CosmosTableMetadata> metadataResponse = mock(CosmosItemResponse.class);
    when(metadataContainer.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(metadataResponse);
    when(metadataResponse.getItem()).thenReturn(null);

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(metadataContainer).upsertItem(any(CosmosTableMetadata.class));
  }

  @Test
  public void repairTable_WhenCompositeIndexUpToDate_ShouldNotReplaceContainer()
      throws ExecutionException {
    // Arrange: a table with a clustering key, so the desired indexing policy has a composite index
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addColumn("c4", DataType.INT)
            .addPartitionKey("c1")
            .addClusteringKey("c2", Order.ASC)
            .addSecondaryIndex("c4")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .clusteringKeyNames(Sets.newLinkedHashSet("c2"))
            .clusteringOrders(ImmutableMap.of("c2", "ASC"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint", "c4", "int"))
            .secondaryIndexNames(ImmutableSet.of("c4"))
            .build());
    // The container's actual indexing policy already matches the desired one, including the
    // composite index derived from the clustering key
    IndexingPolicy currentPolicy = new IndexingPolicy();
    currentPolicy.setIncludedPaths(Collections.singletonList(new IncludedPath("/values/c4/?")));
    currentPolicy.setCompositeIndexes(
        Collections.singletonList(
            Arrays.asList(
                compositePath("/concatenatedPartitionKey", CompositePathSortOrder.ASCENDING),
                compositePath("/clusteringKey/c2", CompositePathSortOrder.ASCENDING))));
    when(repairContainerProperties.getIndexingPolicy()).thenReturn(currentPolicy);

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: the composite index already matches, so the container is not replaced
    verify(container, never()).replace(any(CosmosContainerProperties.class));
  }

  @Test
  public void repairTable_WhenCompositeIndexDiffers_ShouldReplaceContainer()
      throws ExecutionException {
    // Arrange: a table with a clustering key, so the desired indexing policy has a composite index
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addColumn("c4", DataType.INT)
            .addPartitionKey("c1")
            .addClusteringKey("c2", Order.ASC)
            .addSecondaryIndex("c4")
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .clusteringKeyNames(Sets.newLinkedHashSet("c2"))
            .clusteringOrders(ImmutableMap.of("c2", "ASC"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint", "c4", "int"))
            .secondaryIndexNames(ImmutableSet.of("c4"))
            .build());
    // The included paths match, but the composite index is missing the clustering-key path, so the
    // policy differs only on the composite-index dimension
    IndexingPolicy currentPolicy = new IndexingPolicy();
    currentPolicy.setIncludedPaths(Collections.singletonList(new IncludedPath("/values/c4/?")));
    currentPolicy.setCompositeIndexes(
        Collections.singletonList(
            Collections.singletonList(
                compositePath("/concatenatedPartitionKey", CompositePathSortOrder.ASCENDING))));
    when(repairContainerProperties.getIndexingPolicy()).thenReturn(currentPolicy);

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert: the composite index is stale, so the container is replaced to fix it
    verify(container).replace(any(CosmosContainerProperties.class));
  }

  @Test
  public void repairTable_WhenClusteringKeyButNoSecondaryIndex_ShouldNotThrow()
      throws ExecutionException {
    // Arrange: a table with a clustering key and no secondary index. computeIndexingPolicy then
    // sets compositeIndexes but never calls setIncludedPaths, so the desired policy's included
    // paths come straight from the SDK getter. This verifies that comparison does not throw (the
    // SDK lazily initializes getIncludedPaths()/getCompositeIndexes() to empty lists, never null).
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addClusteringKey("c2", Order.ASC)
            .build();
    CosmosContainer metadataContainer = setUpRepairTableMocks(namespace, table);
    stubStoredTableMetadata(
        metadataContainer,
        CosmosTableMetadata.newBuilder()
            .id(getFullTableName(namespace, table))
            .partitionKeyNames(Sets.newLinkedHashSet("c1"))
            .clusteringKeyNames(Sets.newLinkedHashSet("c2"))
            .clusteringOrders(ImmutableMap.of("c2", "ASC"))
            .columns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"))
            .build());

    // Act Assert
    assertThatCode(() -> admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap()))
        .doesNotThrowAnyException();
  }

  private static CompositePath compositePath(String path, CompositePathSortOrder order) {
    CompositePath compositePath = new CompositePath();
    compositePath.setPath(path);
    compositePath.setOrder(order);
    return compositePath;
  }

  @Test
  public void repairTable_WhenTableAlreadyExistsWithoutIndex_ShouldCreateIndex()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .addSecondaryIndex("c3")
            .build();
    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);
    // Metadata container
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    // Stored procedure exists
    CosmosScripts scripts = mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(scripts);
    CosmosStoredProcedure storedProcedure = mock(CosmosStoredProcedure.class);
    when(scripts.getStoredProcedure(CosmosAdmin.STORED_PROCEDURE_FILE_NAME))
        .thenReturn(storedProcedure);
    // Existing container properties
    CosmosContainerResponse response = mock(CosmosContainerResponse.class);
    when(database.createContainerIfNotExists(table, "/concatenatedPartitionKey"))
        .thenReturn(response);
    CosmosContainerProperties properties = mock(CosmosContainerProperties.class);
    when(response.getProperties()).thenReturn(properties);
    when(properties.getIndexingPolicy()).thenReturn(new IndexingPolicy());

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(container, times(1)).replace(properties);
    verify(properties, times(1)).setIndexingPolicy(any(IndexingPolicy.class));
  }

  @Test
  public void addNewColumnToTable_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String currentColumn = "c1";
    String newColumn = "c2";
    String fullTableName = getFullTableName(namespace, table);
    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> response = mock(CosmosItemResponse.class);

    when(client.getDatabase(metadataDatabaseName)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.METADATA_CONTAINER)).thenReturn(container);
    when(container.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(response);

    CosmosTableMetadata cosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .partitionKeyNames(Sets.newLinkedHashSet(currentColumn))
            .columns(ImmutableMap.of(currentColumn, "text"))
            .build();

    when(response.getItem()).thenReturn(cosmosTableMetadata);

    // Act
    admin.addNewColumnToTable(namespace, table, newColumn, DataType.INT);

    // Assert
    verify(container)
        .readItem(fullTableName, new PartitionKey(fullTableName), CosmosTableMetadata.class);

    CosmosTableMetadata expectedCosmosTableMetadata =
        CosmosTableMetadata.newBuilder()
            .id(fullTableName)
            .partitionKeyNames(Sets.newLinkedHashSet(currentColumn))
            .columns(ImmutableMap.of(currentColumn, "text", newColumn, "int"))
            .build();
    verify(container).upsertItem(expectedCosmosTableMetadata);
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
            () -> admin.getImportTableMetadata(namespace, table, Collections.emptyMap()));
    Throwable thrown2 =
        catchThrowable(() -> admin.addRawColumnToTable(namespace, table, column, DataType.INT));
    Throwable thrown3 =
        catchThrowable(
            () ->
                admin.importTable(
                    namespace, table, Collections.emptyMap(), Collections.emptyMap()));

    // Assert
    assertThat(thrown1).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown2).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown3).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getNamespaceNames_WithExistingTables_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer tableMetadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(tableMetadataContainer);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> cosmosPagedIterable = mock(CosmosPagedIterable.class);
    CosmosTableMetadata tableMetadata1 = CosmosTableMetadata.newBuilder().id("ns1.tbl1").build();
    CosmosTableMetadata tableMetadata2 = CosmosTableMetadata.newBuilder().id("ns1.tbl2").build();
    CosmosTableMetadata tableMetadata3 = CosmosTableMetadata.newBuilder().id("ns2.tbl3").build();
    when(cosmosPagedIterable.stream())
        .thenReturn(Stream.of(tableMetadata1, tableMetadata2, tableMetadata3));
    when(tableMetadataContainer.queryItems(anyString(), any(), eq(CosmosTableMetadata.class)))
        .thenReturn(cosmosPagedIterable);

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    verify(tableMetadataContainer).read();
    verify(tableMetadataContainer)
        .queryItems(
            eq("SELECT container.id FROM container"),
            any(CosmosQueryRequestOptions.class),
            eq(CosmosTableMetadata.class));
    assertThat(actual).containsOnly("ns1", "ns2", metadataDatabaseName);
  }

  @Test
  public void getNamespaceNames_WithoutExistingTables_ShouldReturnMetadataDatabaseOnly()
      throws ExecutionException {
    // Arrange
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer tableMetadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(tableMetadataContainer);
    CosmosException cosmosException = mock(CosmosException.class);
    when(cosmosException.getStatusCode()).thenReturn(CosmosErrorCode.NOT_FOUND.get());
    when(tableMetadataContainer.read()).thenThrow(cosmosException);

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    verify(tableMetadataContainer).read();
    assertThat(actual).containsOnly(metadataDatabaseName);
  }

  @Test
  public void namespaceExists_WithExistingNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    CosmosDatabase database = mock(CosmosDatabase.class);
    when(client.getDatabase(namespace)).thenReturn(database);

    // Act
    boolean actual = admin.namespaceExists(namespace);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void namespaceExists_WithMetadataDatabase_ShouldReturnTrue() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThat(admin.namespaceExists(metadataDatabaseName)).isTrue();
  }
}
