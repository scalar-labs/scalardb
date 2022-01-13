package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.models.CompositePathSortOrder;
import com.azure.cosmos.models.CosmosContainerProperties;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CosmosAdminTest {
  @Mock private CosmosClient client;
  @Mock private CosmosConfig config;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  private CosmosAdmin admin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    admin = new CosmosAdmin(client, config);
  }

  @Test
  public void
      getTableMetadata_WithoutTableMetadataDatabaseChanged_ShouldReturnCorrectTableMetadata()
          throws ExecutionException {
    getTableMetadata_ShouldReturnCorrectTableMetadata(Optional.empty());
  }

  @Test
  public void getTableMetadata_WithTableMetadataDatabaseChanged_ShouldReturnCorrectTableMetadata()
      throws ExecutionException {
    getTableMetadata_ShouldReturnCorrectTableMetadata(Optional.of("changed"));
  }

  private void getTableMetadata_ShouldReturnCorrectTableMetadata(
      Optional<String> tableMetadataDatabase) throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String fullName = getFullTableName(namespace, table);
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> response = mock(CosmosItemResponse.class);

    when(client.getDatabase(metadataDatabaseName)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.METADATA_CONTAINER)).thenReturn(container);
    when(container.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(response);

    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setColumns(ImmutableMap.of("c1", "int", "c2", "text", "c3", "bigint"));
    cosmosTableMetadata.setPartitionKeyNames(Collections.singletonList("c1"));
    cosmosTableMetadata.setClusteringKeyNames(Collections.emptyList());
    cosmosTableMetadata.setSecondaryIndexNames(Collections.emptySet());

    when(response.getItem()).thenReturn(cosmosTableMetadata);

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

    // Act
    TableMetadata actual = admin.getTableMetadata(namespace, table);

    // Assert
    assertThat(actual)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addColumn("c1", DataType.INT)
                .addColumn("c2", DataType.TEXT)
                .addColumn("c3", DataType.BIGINT)
                .addPartitionKey("c1")
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
  public void createTable_WithoutTableMetadataDatabaseChanged_ShouldCreateContainer()
      throws ExecutionException {
    createTable_ShouldCreateContainer(Optional.empty());
  }

  @Test
  public void createTable_WithTableMetadataDatabaseChanged_ShouldCreateContainer()
      throws ExecutionException {
    createTable_ShouldCreateContainer(Optional.of("changed"));
  }

  private void createTable_ShouldCreateContainer(Optional<String> tableMetadataDatabase)
      throws ExecutionException {
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
            .addSecondaryIndex("c4")
            .build();

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // for metadata table
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

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
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(getFullTableName(namespace, table));
    cosmosTableMetadata.setPartitionKeyNames(Collections.singletonList("c3"));
    cosmosTableMetadata.setClusteringKeyNames(Arrays.asList("c1", "c2"));
    cosmosTableMetadata.setClusteringOrders(ImmutableMap.of("c1", "DESC", "c2", "ASC"));
    cosmosTableMetadata.setColumns(
        new ImmutableMap.Builder<String, String>()
            .put("c1", "text")
            .put("c2", "bigint")
            .put("c3", "boolean")
            .put("c4", "blob")
            .put("c5", "int")
            .put("c6", "double")
            .put("c7", "float")
            .build());
    cosmosTableMetadata.setSecondaryIndexNames(ImmutableSet.of("c4"));
    verify(metadataContainer).upsertItem(cosmosTableMetadata);
  }

  @Test
  public void
      createTable_WithoutClusteringKeysWithoutTableMetadataDatabaseChanged_ShouldCreateContainerWithCompositeIndex()
          throws ExecutionException {
    createTable_WithoutClusteringKeys_ShouldCreateContainerWithCompositeIndex(Optional.empty());
  }

  @Test
  public void
      createTable_WithoutClusteringKeysWithTableMetadataDatabaseChanged_ShouldCreateContainerWithCompositeIndex()
          throws ExecutionException {
    createTable_WithoutClusteringKeys_ShouldCreateContainerWithCompositeIndex(
        Optional.of("changed"));
  }

  private void createTable_WithoutClusteringKeys_ShouldCreateContainerWithCompositeIndex(
      Optional<String> tableMetadataDatabase) throws ExecutionException {
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
            .addSecondaryIndex("c4")
            .build();

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // for metadata table
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

    CosmosDatabase metadataDatabase = mock(CosmosDatabase.class);
    CosmosContainer metadataContainer = mock(CosmosContainer.class);
    when(client.getDatabase(metadataDatabaseName)).thenReturn(metadataDatabase);
    when(metadataDatabase.getContainer(CosmosAdmin.METADATA_CONTAINER))
        .thenReturn(metadataContainer);

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

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
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(getFullTableName(namespace, table));
    cosmosTableMetadata.setPartitionKeyNames(Collections.singletonList("c3"));
    cosmosTableMetadata.setClusteringOrders(Collections.emptyMap());
    cosmosTableMetadata.setClusteringKeyNames(Collections.emptyList());
    cosmosTableMetadata.setColumns(
        new ImmutableMap.Builder<String, String>()
            .put("c1", "text")
            .put("c2", "bigint")
            .put("c3", "boolean")
            .put("c4", "blob")
            .put("c5", "int")
            .put("c6", "double")
            .put("c7", "float")
            .build());
    cosmosTableMetadata.setSecondaryIndexNames(ImmutableSet.of("c4"));
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
  public void
      dropTable_WithNoMetadataLeftWithoutTableMetadataDatabaseChanged_ShouldDropContainerAndDeleteMetadataAndDatabase()
          throws ExecutionException {
    dropTable_WithNoMetadataLeft_ShouldDropContainerAndDeleteMetadataAndDatabase(Optional.empty());
  }

  @Test
  public void
      dropTable_WithNoMetadataLeftWithTableMetadataDatabaseChanged_ShouldDropContainerAndDeleteMetadataAndDatabase()
          throws ExecutionException {
    dropTable_WithNoMetadataLeft_ShouldDropContainerAndDeleteMetadataAndDatabase(
        Optional.of("changed"));
  }

  private void dropTable_WithNoMetadataLeft_ShouldDropContainerAndDeleteMetadataAndDatabase(
      Optional<String> tableMetadataDatabase) throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(namespace)).thenReturn(database);
    when(database.getContainer(table)).thenReturn(container);

    // for metadata table
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

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

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

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
  public void
      dropTable_WithMetadataLeftWithoutTableMetadataDatabaseChanged_ShouldDropContainerAndOnlyDeleteMetadata()
          throws ExecutionException {
    dropTable_WithMetadataLeft_ShouldDropContainerAndOnlyDeleteMetadata(Optional.empty());
  }

  @Test
  public void
      dropTable_WithMetadataLeftWithTableMetadataDatabaseChanged_ShouldDropContainerAndOnlyDeleteMetadata()
          throws ExecutionException {
    dropTable_WithMetadataLeft_ShouldDropContainerAndOnlyDeleteMetadata(Optional.of("changed"));
  }

  private void dropTable_WithMetadataLeft_ShouldDropContainerAndOnlyDeleteMetadata(
      Optional<String> tableMetadataDatabase) throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    // for metadata table
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

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

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

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

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(database).delete();
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
            refEq(new CosmosQueryRequestOptions()),
            eq(Record.class));
    verify(container)
        .deleteItem(
            eq("id1"), refEq(new PartitionKey("p1")), refEq(new CosmosItemRequestOptions()));
    verify(container)
        .deleteItem(
            eq("id2"), refEq(new PartitionKey("p2")), refEq(new CosmosItemRequestOptions()));
  }

  @Test
  public void
      getNamespaceTableNames_WithoutTableMetadataDatabaseChanged_ShouldGetTableNamesProperly()
          throws ExecutionException {
    getNamespaceTableNames_ShouldGetTableNamesProperly(Optional.empty());
  }

  @Test
  public void getNamespaceTableNames_WithTableMetadataDatabaseChanged_ShouldGetTableNamesProperly()
      throws ExecutionException {
    getNamespaceTableNames_ShouldGetTableNamesProperly(Optional.of("changed"));
  }

  private void getNamespaceTableNames_ShouldGetTableNamesProperly(
      Optional<String> tableMetadataDatabase) throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String metadataDatabaseName = tableMetadataDatabase.orElse(CosmosAdmin.METADATA_DATABASE);

    CosmosTableMetadata t1 = new CosmosTableMetadata();
    t1.setId(getFullTableName(namespace, "t1"));
    CosmosTableMetadata t2 = new CosmosTableMetadata();
    t2.setId(getFullTableName(namespace, "t2"));

    when(client.getDatabase(metadataDatabaseName)).thenReturn(database);
    when(database.getContainer(CosmosAdmin.METADATA_CONTAINER)).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(CosmosTableMetadata.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(t1, t2));

    if (tableMetadataDatabase.isPresent()) {
      when(config.getTableMetadataDatabase()).thenReturn(tableMetadataDatabase);
      admin = new CosmosAdmin(client, config);
    }

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).containsExactly("t1", "t2");
    verify(client, atLeastOnce()).getDatabase(metadataDatabaseName);
    verify(database, atLeastOnce()).getContainer(CosmosAdmin.METADATA_CONTAINER);
    verify(container)
        .queryItems(
            eq("SELECT * FROM metadata WHERE metadata.id LIKE 'ns.%'"),
            refEq(new CosmosQueryRequestOptions()),
            eq(CosmosTableMetadata.class));
  }
}
