package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Get;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.Collections;
import java.util.LinkedHashSet;
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

  private CosmosTableMetadataManager manager;
  @Mock private CosmosContainer container;
  @Mock private CosmosTableMetadata metadata;
  @Mock private CosmosItemResponse<CosmosTableMetadata> response;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    manager = new CosmosTableMetadataManager(container);

    // Arrange
    when(metadata.getColumns()).thenReturn(ImmutableMap.of(ANY_NAME_1, "varchar"));
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  @Test
  public void getTableMetadata_ProperOperationGivenFirst_ShouldCallReadItem() {
    // Arrange
    when(container.readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class)))
        .thenReturn(response);
    when(response.getItem()).thenReturn(metadata);

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act
    manager.getTableMetadata(get);

    verify(container).readItem(FULLNAME, new PartitionKey(FULLNAME), CosmosTableMetadata.class);
  }

  @Test
  public void getTableMetadata_SameTableGiven_ShouldCallReadItemOnce() {
    // Arrange
    when(container.readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class)))
        .thenReturn(response);
    when(response.getItem()).thenReturn(metadata);

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Get get1 = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    Key partitionKey2 = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_2));
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

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
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
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .readItem(anyString(), any(PartitionKey.class), eq(CosmosTableMetadata.class));

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getTableMetadata(get);
            })
        .isInstanceOf(StorageRuntimeException.class)
        .hasCause(toThrow);
  }
}
