package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosTableMetadataManager.METADATA_CONTAINER;
import static com.scalar.db.storage.cosmos.CosmosTableMetadataManager.METADATA_DATABASE;
import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosTableMetadataManagerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String FULLNAME = ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME;
  private static final String DATABASE_PREFIX = "db_pfx";
  private CosmosTableMetadataManager manager;

  @Mock private CosmosClient client;
  @Mock private CosmosTableMetadata metadata;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private CosmosItemResponse<CosmosTableMetadata> response;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    manager = new CosmosTableMetadataManager(client, Optional.of(DATABASE_PREFIX));

    // Arrange
    when(metadata.getColumns()).thenReturn(ImmutableMap.of(ANY_NAME_1, "varchar"));
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  @Test
  public void getTableMetadata_ProperOperationGivenFirst_ShouldCallReadItem() {
    // Arrange
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    when(container.readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class)))
        .thenReturn(response);
    when(response.getItem()).thenReturn(metadata);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act
    manager.getTableMetadata(get);

    verify(container).readItem(FULLNAME, new PartitionKey(FULLNAME), CosmosTableMetadata.class);
  }

  @Test
  public void getTableMetadata_SameTableGiven_ShouldCallReadItemOnce() {
    // Arrange
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    when(container.readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class)))
        .thenReturn(response);
    when(response.getItem()).thenReturn(metadata);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get1 = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    Key partitionKey2 = new Key(ANY_NAME_1, ANY_TEXT_2);
    Get get2 = new Get(partitionKey2).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act
    manager.getTableMetadata(get1);
    manager.getTableMetadata(get2);

    verify(container, times(1))
        .readItem(FULLNAME, new PartitionKey(FULLNAME), CosmosTableMetadata.class);
  }

  @Test
  public void getTableMetadata_OperationWithoutTableGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(container.readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class)))
        .thenReturn(response);
    when(response.getItem()).thenReturn(metadata);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getTableMetadata(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getTableMetadata_CosmosExceptionThrown_ShouldThrowStorageRuntimeException() {
    // Arrange
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class));

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getTableMetadata(get);
            })
        .isInstanceOf(StorageRuntimeException.class)
        .hasCause(toThrow);
  }

  @Test
  public void addTableMetadata_WithCorrectParametersGiven_ShouldCreateMetadataDatabase() {
    // Arrange
    String databaseName = "sample_db";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1", Order.DESC)
            .addClusteringKey("c4", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c4")
            .build();
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    // Act
    manager.addTableMetadata(databaseName, table, metadata);

    // Assert
    verify(client)
        .createDatabaseIfNotExists(
            eq(getFullNamespaceName(Optional.of(DATABASE_PREFIX), METADATA_DATABASE)),
            refEq(ThroughputProperties.createManualThroughput(Integer.parseInt("300"))));
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    verify(database).createContainerIfNotExists(containerProperties);
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(getFullTableName(Optional.of(DATABASE_PREFIX), databaseName, table));
    cosmosTableMetadata.setPartitionKeyNames(Collections.singletonList("c3"));
    cosmosTableMetadata.setClusteringKeyNames(Arrays.asList("c1", "c4"));
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
  public void getTableNames_withCorrectParamGiven_shouldReturnTableNames() {
    // Arrange
    String databaseName = "sample_db";
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);
    @SuppressWarnings("unchecked")
    CosmosPagedIterable<CosmosTableMetadata> queryResults =
        (CosmosPagedIterable<CosmosTableMetadata>) mock(CosmosPagedIterable.class);
    when(container.queryItems(anyString(), any(), CosmosTableMetadata.class))
        .thenReturn(queryResults);

    // Act
    manager.getTableNames(databaseName);

    // Assert

  }
}
