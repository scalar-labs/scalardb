package com.scalar.db.storage.cosmos;

import com.azure.cosmos.models.PartitionKey;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CosmosOperationTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;

  @Mock private TableMetadataManager metadataManager;
  @Mock private CosmosTableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);
    CosmosOperation cosmosOperation = new CosmosOperation(operation, metadataManager);

    // Act Assert
    assertThatThrownBy(
            () -> {
              cosmosOperation.checkArgument(Get.class);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void isPrimaryKeySpecified_PrimaryKeyWithoutClusteringKeyGiven_ShouldReturnTrue() {
    // Arrange
    when(metadata.getClusteringKeyNames()).thenReturn(new LinkedHashSet<>());

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new IntValue(ANY_NAME_3, ANY_INT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act
    boolean actual = cosmosOperation.isPrimaryKeySpecified();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isPrimaryKeySpecified_PrimaryKeyWithClusteringKeyGiven_ShouldReturnTrue() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new IntValue(ANY_NAME_3, ANY_INT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act
    boolean actual = cosmosOperation.isPrimaryKeySpecified();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isPrimaryKeySpecified_NoClusteringKeyGiven_ShouldReturnTrue() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new IntValue(ANY_NAME_3, ANY_INT_1));
    Delete delete =
        new Delete(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(delete, metadataManager);

    // Act
    boolean actual = cosmosOperation.isPrimaryKeySpecified();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void getConcatenatedPartitionKey_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey =
        new Key(
            new TextValue(ANY_NAME_1, ANY_TEXT_1),
            new TextValue(ANY_NAME_2, ANY_TEXT_2),
            new IntValue(ANY_NAME_3, ANY_INT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act
    String actual = cosmosOperation.getConcatenatedPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(ANY_TEXT_1 + ":" + ANY_TEXT_2 + ":" + ANY_INT_1);
  }

  @Test
  public void
      getConcatenatedPartitionKey_WrongPartitionKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act Assert
    assertThatThrownBy(
            () -> {
              cosmosOperation.getConcatenatedPartitionKey();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getCosmosPartitionKey_MultipleKeysGiven_ShouldReturnPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey =
        new Key(
            new TextValue(ANY_NAME_1, ANY_TEXT_1),
            new TextValue(ANY_NAME_2, ANY_TEXT_2),
            new IntValue(ANY_NAME_3, ANY_INT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act
    PartitionKey actual = cosmosOperation.getCosmosPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(new PartitionKey(ANY_TEXT_1 + ":" + ANY_TEXT_2 + ":" + ANY_INT_1));
  }

  @Test
  public void getId_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getKeyNames()).thenReturn(ImmutableList.of(ANY_NAME_1, ANY_NAME_3, ANY_NAME_2));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new IntValue(ANY_NAME_3, ANY_INT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act
    String actual = cosmosOperation.getId();

    // Assert
    assertThat(actual).isEqualTo(ANY_TEXT_1 + ":" + ANY_INT_1 + ":" + ANY_TEXT_2);
  }

  @Test
  public void getId_WrongKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(metadata.getKeyNames()).thenReturn(ImmutableList.of(ANY_NAME_1, ANY_NAME_3, ANY_NAME_2));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new IntValue(ANY_NAME_3, ANY_INT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_4, ANY_TEXT_2));
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadataManager);

    // Act Assert
    assertThatThrownBy(
            () -> {
              cosmosOperation.getId();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }
}
