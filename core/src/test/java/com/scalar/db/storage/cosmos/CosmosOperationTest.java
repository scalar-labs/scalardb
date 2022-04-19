package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosOperationTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);
    CosmosOperation cosmosOperation = new CosmosOperation(operation, metadata);

    // Act Assert
    assertThatThrownBy(() -> cosmosOperation.checkArgument(Get.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void isPrimaryKeySpecified_PrimaryKeyWithoutClusteringKeyGiven_ShouldReturnTrue() {
    // Arrange
    when(metadata.getClusteringKeyNames()).thenReturn(new LinkedHashSet<>());

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_3, ANY_INT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadata);

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

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_3, ANY_INT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadata);

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

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_3, ANY_INT_1);
    Delete delete =
        new Delete(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(delete, metadata);

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
        new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_INT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadata);

    // Act
    String actual = cosmosOperation.getConcatenatedPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(ANY_TEXT_1 + ":" + ANY_TEXT_2 + ":" + ANY_INT_1);
  }

  @Test
  public void getCosmosPartitionKey_MultipleKeysGiven_ShouldReturnPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey =
        new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_INT_1);
    Get get = new Get(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadata);

    // Act
    PartitionKey actual = cosmosOperation.getCosmosPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(new PartitionKey(ANY_TEXT_1 + ":" + ANY_TEXT_2 + ":" + ANY_INT_1));
  }

  @Test
  public void getId_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_3)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_3, ANY_INT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    CosmosOperation cosmosOperation = new CosmosOperation(get, metadata);

    // Act
    String actual = cosmosOperation.getId();

    // Assert
    assertThat(actual).isEqualTo(ANY_TEXT_1 + ":" + ANY_INT_1 + ":" + ANY_TEXT_2);
  }
}
