package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosAdmin.DEFAULT_REQUEST_UNIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CosmosAdminTest {
  private static final String DATABASE_PREFIX = "db_pfx_";
  @Mock CosmosTableMetadataManager metadataManager;
  @Mock CosmosClient client;
  @Mock CosmosDatabase database;
  @Mock CosmosContainer container;
  CosmosAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    admin = new CosmosAdmin(client, metadataManager, Optional.of(DATABASE_PREFIX));
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void
      createNamespace_WithoutSettingRu_ShouldCreateDatabaseWithManualThroughputWithDefaultRu()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";

    // Act
    admin.createNamespace(namespace, Collections.emptyMap());

    // Assert
    verify(client)
        .createDatabase(
            eq(DATABASE_PREFIX + namespace),
            refEq(
                ThroughputProperties.createManualThroughput(
                    Integer.parseInt(DEFAULT_REQUEST_UNIT))));
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
            eq(DATABASE_PREFIX + namespace),
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
            eq(DATABASE_PREFIX + namespace),
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
            eq(DATABASE_PREFIX + namespace),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt(throughput))));
  }

  @Test
  public void createTable_WithCorrectParams_ShouldCreateContainer() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.INT).build();
    when(client.getDatabase(any())).thenReturn(database);
    when(database.getContainer(any())).thenReturn(container);
    CosmosScripts cosmosScripts = Mockito.mock(CosmosScripts.class);
    when(container.getScripts()).thenReturn(cosmosScripts);

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(database).createContainer(any(CosmosContainerProperties.class));
    verify(cosmosScripts).createStoredProcedure(any(CosmosStoredProcedureProperties.class));
    verify(metadataManager).addTableMetadata(namespace, table, metadata);
  }

  @Test
  public void dropTable_WithExistingContainer_ShouldDropContainer() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    when(client.getDatabase(any())).thenReturn(database);
    when(database.getContainer(any())).thenReturn(container);

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(container).delete();
    verify(metadataManager).deleteTableMetadata(namespace, table);
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
  public void getNamespaceTableNames_ShouldExecuteManagerMethod() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    Set<String> tableNames = ImmutableSet.of("t1", "t2");
    when(metadataManager.getTableNames(anyString())).thenReturn(tableNames);

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(namespace);

    // Assert
    verify(metadataManager).getTableNames(namespace);
    Assertions.assertThat(actualNames).isEqualTo(tableNames);
  }
}
