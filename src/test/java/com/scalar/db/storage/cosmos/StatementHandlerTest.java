package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
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

public class StatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;

  private SelectStatementHandler handler;
  @Mock private CosmosClient client;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);

    handler = new SelectStatementHandler(client, metadataManager);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new SelectStatementHandler(null, metadataManager);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);

    // Act Assert
    assertThatThrownBy(
            () -> {
              SelectStatementHandler.checkArgument(operation, Get.class);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getConcatenatedPartitionKey_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(ImmutableSortedSet.of(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3));

    Key partitionKey =
        new Key(
            new TextValue(ANY_NAME_1, ANY_TEXT_1),
            new TextValue(ANY_NAME_2, ANY_TEXT_2),
            new IntValue(ANY_NAME_3, ANY_INT_1));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act
    String actual = handler.getConcatenatedPartitionKey(get);

    // Assert
    assertThat(actual).isEqualTo(ANY_TEXT_1 + ":" + ANY_TEXT_2 + ":" + ANY_INT_1);
  }

  @Test
  public void
      getConcatenatedPartitionKey_WrongPartitionKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(ImmutableSortedSet.of(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3));

    Key partitionKey =
        new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1), new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Get get = new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.getConcatenatedPartitionKey(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
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

    // Act
    String actual = handler.getId(get);

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

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.getId(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }
}
