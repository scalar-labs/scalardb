package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
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
import com.azure.cosmos.models.CompositePathSortOrder;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
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

public class CosmosAdminTest {

  private static final String METADATA_DATABASE = "scalardb";

  @Mock private CosmosClient client;
  @Mock private CosmosConfig config;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private CosmosException notFoundException;
  private CosmosAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getMetadataDatabase()).thenReturn(METADATA_DATABASE);
    admin = new CosmosAdmin(client, config);

    when(notFoundException.getStatusCode()).thenReturn(CosmosErrorCode.NOT_FOUND.get());
  }

  @Test
  public void getTableMetadata_ShouldReturnCorrectTableMetadata() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String fullName = getFullTableName(namespace, table);

    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> response = mock(CosmosItemResponse.class);

    when(client.getDatabase(METADATA_DATABASE)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER)).thenReturn(container);
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

    verify(client).getDatabase(METADATA_DATABASE);
    verify(database).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
    verify(container).readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class);
    verify(response).getItem();
  }

  @Test
  public void createNamespace_WithCustomRuBelow4000_ShouldCreateDatabaseWithManualThroughput()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "2000";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.createNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).createItem(new CosmosNamespace(namespace));
  }

  @Test
  public void createNamespace_WithCustomRuEqualTo4000_ShouldCreateDatabaseWithAutoscaledThroughput()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.createNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createAutoscaledThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).createItem(new CosmosNamespace(namespace));
  }

  @Test
  public void
      createNamespace_WithCustomRuEqualTo4000AndNoScaling_ShouldCreateDatabaseWithManualThroughput()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";
    String noScaling = "true";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.createNamespace(
        namespace,
        ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, throughput, CosmosAdmin.NO_SCALING, noScaling));

    // Assert
    verify(client)
        .createDatabase(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).createItem(new CosmosNamespace(namespace));
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

    // for metadata table and namespaces table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenReturn(namespacesContainer);

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
            eq(METADATA_DATABASE),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("400"))));
    verify(metadataDatabase).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId())
        .isEqualTo(CosmosAdmin.TABLE_METADATA_CONTAINER);
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

    // for metadata table and namespaces table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenReturn(namespacesContainer);

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
            eq(METADATA_DATABASE),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("400"))));
    verify(metadataDatabase).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId())
        .isEqualTo(CosmosAdmin.TABLE_METADATA_CONTAINER);
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

    // for namespaces table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenReturn(namespacesContainer);

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace, table, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropTable_WithNoMetadataLeft_ShouldDropContainerAndDeleteTableMetadataContainer()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    // for metadata table
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(metadataContainer.queryItems(anyString(), any(), eq(Object.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.empty());

    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosNamespace> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.<CosmosNamespace>queryItems(anyString(), any(), any()))
        .thenReturn(pagedIterable);
    when(pagedIterable.stream())
        .thenReturn(
            Stream.of(new CosmosNamespace(METADATA_DATABASE), new CosmosNamespace(namespace)));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();

    // for metadata table
    verify(client, atLeastOnce()).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, atLeastOnce()).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
    String fullTable = getFullTableName(namespace, table);
    verify(metadataContainer)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(metadataContainer).delete();
    verify(metadataDatabase, never()).delete();
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
        .thenReturn(metadataContainer);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(metadataContainer.queryItems(anyString(), any(), eq(Object.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(new CosmosTableMetadata()));

    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosNamespace> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.<CosmosNamespace>queryItems(anyString(), any(), any()))
        .thenReturn(pagedIterable);
    when(pagedIterable.stream())
        .thenReturn(
            Stream.of(new CosmosNamespace(METADATA_DATABASE), new CosmosNamespace(namespace)));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();

    // for metadata table
    verify(client, atLeastOnce()).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, atLeastOnce()).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
    String fullTable = getFullTableName(namespace, table);
    verify(metadataContainer)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(metadataContainer, never()).delete();
    verify(metadataDatabase, never()).delete();
  }

  @Test
  public void
      dropNamespace_WithExistingDatabaseAndOnlyMetadataNamespaceLeft_ShouldDropDatabaseAndMetadataNamespace()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(any())).thenReturn(database, metadataDatabase);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(metadataDatabase.getContainer(anyString())).thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosNamespace> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.<CosmosNamespace>queryItems(anyString(), any(), any()))
        .thenReturn(pagedIterable);
    when(pagedIterable.stream()).thenReturn(Stream.of(new CosmosNamespace(METADATA_DATABASE)));

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosContainerProperties> containerPagedIterable =
        mock(CosmosPagedIterable.class);
    @SuppressWarnings("unchecked")
    Iterator<CosmosContainerProperties> iterator = mock(Iterator.class);
    CosmosContainerProperties containerProperties = mock(CosmosContainerProperties.class);
    when(containerProperties.getId()).thenReturn(CosmosAdmin.NAMESPACES_CONTAINER);
    when(iterator.hasNext()).thenReturn(true, false);
    when(iterator.next()).thenReturn(containerProperties);
    when(containerPagedIterable.iterator()).thenReturn(iterator);
    when(metadataDatabase.readAllContainers()).thenReturn(containerPagedIterable);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(client).getDatabase(namespace);
    verify(database).delete();
    verify(client, times(3)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, times(2)).getContainer(CosmosAdmin.NAMESPACES_CONTAINER);
    verify(namespacesContainer)
        .deleteItem(eq(new CosmosNamespace(namespace)), refEq(new CosmosItemRequestOptions()));
    verify(metadataDatabase).delete();
  }

  @Test
  public void dropNamespace_WithExistingDatabaseAndSomeNamespacesLeft_ShouldDropDatabase()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    when(client.getDatabase(any())).thenReturn(database, metadataDatabase);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(metadataDatabase.getContainer(anyString())).thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.queryItems(anyString(), any(), any())).thenReturn(pagedIterable);
    when(pagedIterable.stream())
        .thenReturn(Stream.of(mock(CosmosNamespace.class), mock(CosmosNamespace.class)));

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(client).getDatabase(namespace);
    verify(database).delete();
    verify(client, times(2)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, times(2)).getContainer(CosmosAdmin.NAMESPACES_CONTAINER);
    verify(namespacesContainer)
        .deleteItem(eq(new CosmosNamespace(namespace)), refEq(new CosmosItemRequestOptions()));
    verify(metadataDatabase, never()).delete();
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

    when(client.getDatabase(METADATA_DATABASE)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER)).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(CosmosTableMetadata.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(t1, t2));

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).containsExactly("t1", "t2");
    verify(client, atLeastOnce()).getDatabase(METADATA_DATABASE);
    verify(database, atLeastOnce()).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
        .thenReturn(metadataContainer);

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

    // Act Assert
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(client).createDatabaseIfNotExists(eq(METADATA_DATABASE), any());

    verify(metadataDatabase).createContainerIfNotExists(any());
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
    verify(storedProcedure).read();
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(client).createDatabaseIfNotExists(eq(METADATA_DATABASE), any());

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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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

    verify(client).createDatabaseIfNotExists(eq(METADATA_DATABASE), any());

    verify(metadataDatabase).createContainerIfNotExists(any());
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
    verify(storedProcedure).read();
    verify(scripts).createStoredProcedure(any());
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
    when(client.getDatabase(METADATA_DATABASE)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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

    when(client.getDatabase(METADATA_DATABASE)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER)).thenReturn(container);
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
  public void getNamespaceNames_NonExistingNamespacesContainer_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    when(client.getDatabase(anyString())).thenThrow(notFoundException);

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaces).isEmpty();
    verify(client).getDatabase(METADATA_DATABASE);
  }

  @Test
  public void getNamespaceNames_ShouldWorkProperly() throws ExecutionException {
    // Arrange
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(anyString())).thenReturn(namespacesContainer);

    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosNamespace> pagedIterable = mock(CosmosPagedIterable.class);
    when(namespacesContainer.queryItems(anyString(), any(), eq(CosmosNamespace.class)))
        .thenReturn(pagedIterable);
    when(pagedIterable.stream())
        .thenReturn(Stream.of(new CosmosNamespace("ns1"), new CosmosNamespace("ns2")));

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    verify(client, times(2)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, times(2)).getContainer(CosmosAdmin.NAMESPACES_CONTAINER);
    verify(namespacesContainer)
        .queryItems(
            eq("SELECT * FROM container"),
            any(CosmosQueryRequestOptions.class),
            eq(CosmosNamespace.class));
    assertThat(actualNamespaces).containsOnly("ns1", "ns2");
  }

  @Test
  public void namespaceExists_WithExistingNamespace_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(anyString())).thenReturn(namespacesContainer);

    // Act Assert
    assertThat(admin.namespaceExists("ns")).isTrue();

    verify(client).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).getContainer(CosmosAdmin.NAMESPACES_CONTAINER);
    verify(namespacesContainer).readItem("ns", new PartitionKey("ns"), CosmosNamespace.class);
  }

  @Test
  public void namespaceExists_WithNonExistingMetadataDatabase_ShouldReturnFalse()
      throws ExecutionException {
    // Arrange
    when(client.getDatabase(anyString())).thenThrow(notFoundException);

    // Act Assert
    assertThat(admin.namespaceExists("ns")).isFalse();

    verify(client).getDatabase(METADATA_DATABASE);
  }

  @Test
  public void
      repairNamespace_WithCustomRuBelow4000_ShouldCreateDatabaseIfNotExistsWithManualThroughput()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "2000";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.repairNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabaseIfNotExists(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).upsertItem(new CosmosNamespace(namespace));
  }

  @Test
  public void
      repairNamespace_WithCustomRuEqualTo4000_ShouldCreateDatabaseIfNotExistsWithAutoscaledThroughput()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.repairNamespace(
        namespace, Collections.singletonMap(CosmosAdmin.REQUEST_UNIT, throughput));

    // Assert
    verify(client)
        .createDatabaseIfNotExists(
            eq(namespace),
            refEq(ThroughputProperties.createAutoscaledThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).upsertItem(new CosmosNamespace(namespace));
  }

  @Test
  public void
      repairNamespace_WithCustomRuEqualTo4000AndNoScaling_ShouldCreateDatabaseIfNotExistsWithManualThroughput()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String throughput = "4000";
    String noScaling = "true";
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);

    // Act
    admin.repairNamespace(
        namespace,
        ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, throughput, CosmosAdmin.NO_SCALING, noScaling));

    // Assert
    verify(client)
        .createDatabaseIfNotExists(
            eq(namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(client, times(4)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(namespacesContainer).upsertItem(new CosmosNamespace(namespace));
  }

  @Test
  public void upgrade_WithExistingTables_ShouldUpsertNamespaces() throws ExecutionException {
    // Arrange
    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer namespacesContainer = mock(CosmosContainer.class);
    CosmosContainer tableMetadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(anyString())).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.NAMESPACES_CONTAINER))
        .thenThrow(notFoundException)
        .thenReturn(namespacesContainer);
    when(metadataDatabase.getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER))
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
    admin.upgrade(Collections.emptyMap());

    // Assert
    verify(client, times(7)).getDatabase(METADATA_DATABASE);
    verify(metadataDatabase, times(2)).getContainer(CosmosAdmin.TABLE_METADATA_CONTAINER);
    verify(tableMetadataContainer).read();
    verify(client)
        .createDatabaseIfNotExists(
            eq(METADATA_DATABASE),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(CosmosAdmin.DEFAULT_REQUEST_UNIT))));
    verify(metadataDatabase).createContainerIfNotExists(CosmosAdmin.NAMESPACES_CONTAINER, "/id");
    verify(namespacesContainer).createItem(new CosmosNamespace(METADATA_DATABASE));
    verify(tableMetadataContainer)
        .queryItems(
            eq("SELECT container.id FROM container"),
            any(CosmosQueryRequestOptions.class),
            eq(CosmosTableMetadata.class));
    verify(metadataDatabase, times(4)).getContainer(CosmosAdmin.NAMESPACES_CONTAINER);
    verify(namespacesContainer).upsertItem(new CosmosNamespace("ns1"));
    verify(namespacesContainer).upsertItem(new CosmosNamespace("ns2"));
  }
}
