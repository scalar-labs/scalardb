package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
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
  @Mock private DatabaseConfig config;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  private CosmosAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    when(config.getNamespacePrefix()).thenReturn(Optional.empty());
    admin = new CosmosAdmin(client, config);
  }

  @Test
  public void getTableMetadata_ContainerShouldBeCalledProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String fullName = getFullTableName(Optional.empty(), namespace, table);

    @SuppressWarnings("unchecked")
    CosmosItemResponse<CosmosTableMetadata> response = mock(CosmosItemResponse.class);

    when(client.getDatabase(any())).thenReturn(database);
    when(database.getContainer(any())).thenReturn(container);
    when(container.readItem(
            anyString(),
            any(PartitionKey.class),
            ArgumentMatchers.<Class<CosmosTableMetadata>>any()))
        .thenReturn(response);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
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
  public void createTable_WithCorrectParams_ShouldCreateContainer() throws ExecutionException {
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

    when(client.getDatabase(any())).thenReturn(database);
    when(database.getContainer(any())).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(database).createContainer(any(CosmosContainerProperties.class));
    verify(cosmosScripts).createStoredProcedure(any(CosmosStoredProcedureProperties.class));

    verify(client)
        .createDatabaseIfNotExists(
            eq(getFullNamespaceName(Optional.empty(), CosmosAdmin.METADATA_DATABASE)),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("400"))));
    ArgumentCaptor<CosmosContainerProperties> containerPropertiesCaptor =
        ArgumentCaptor.forClass(CosmosContainerProperties.class);
    verify(database).createContainerIfNotExists(containerPropertiesCaptor.capture());
    assertThat(containerPropertiesCaptor.getValue().getId())
        .isEqualTo(CosmosAdmin.METADATA_CONTAINER);
    assertThat(containerPropertiesCaptor.getValue().getPartitionKeyDefinition().getPaths())
        .containsExactly("/id");
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(getFullTableName(Optional.empty(), namespace, table));
    cosmosTableMetadata.setPartitionKeyNames(Collections.singletonList("c3"));
    cosmosTableMetadata.setClusteringKeyNames(Arrays.asList("c1", "c2"));
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
    verify(container).upsertItem(cosmosTableMetadata);
  }

  @Test
  public void createTable_WithBlobClusteringKey_ShouldThrowExecutionException() {
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
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void
      dropTable_WithExistingContainerWithNoMetadataLeft_ShouldDropContainerAndDeleteMetadataAndDatabase()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(Object.class))).thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.empty());

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container, times(2)).delete();
    String fullTable = getFullTableName(Optional.empty(), namespace, table);
    verify(container)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(database).delete();
  }

  @Test
  public void
      dropTable_WithExistingContainerWithMetadataLeft_ShouldDropContainerAndOnlyDeleteMetadata()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<Object> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(Object.class))).thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(new CosmosTableMetadata()));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();
    String fullTable = getFullTableName(Optional.empty(), namespace, table);
    verify(container)
        .deleteItem(
            eq(fullTable), eq(new PartitionKey(fullTable)), refEq(new CosmosItemRequestOptions()));
    verify(database, never()).delete();
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
            eq("SELECT t.id, t.concatenatedPartitionKey FROM " + table + " t"),
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
  public void getNamespaceTableNames_ShouldGetTableNamesProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";

    CosmosTableMetadata t1 = new CosmosTableMetadata();
    t1.setId(getFullTableName(Optional.empty(), namespace, "t1"));
    CosmosTableMetadata t2 = new CosmosTableMetadata();
    t2.setId(getFullTableName(Optional.empty(), namespace, "t2"));

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> queryResults = mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), eq(CosmosTableMetadata.class)))
        .thenReturn(queryResults);
    when(queryResults.stream()).thenReturn(Stream.of(t1, t2));

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).containsExactly("t1", "t2");
    verify(container)
        .queryItems(
            eq("SELECT * FROM metadata WHERE metadata.id LIKE 'ns.%'"),
            refEq(new CosmosQueryRequestOptions()),
            eq(CosmosTableMetadata.class));
  }
}
