package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosContainer;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosMutationTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;
  private static final int ANY_INT_3 = 3;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT_3);

  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private CosmosTableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new HashSet<String>(Arrays.asList(ANY_NAME_1)));
    when(metadata.getKeyNames()).thenReturn(Arrays.asList(ANY_NAME_1, ANY_NAME_2));
  }

  private Put preparePut() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(new IntValue(ANY_NAME_3, ANY_INT_1))
            .withValue(new IntValue(ANY_NAME_4, ANY_INT_2));

    return put;
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Delete del =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return del;
  }

  @Test
  public void makeRecord_PutGiven_ShouldReturnWithValues() {
    // Arrange
    Put put = preparePut();
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadataManager);
    String id = cosmosMutation.getId();
    String concatenatedPartitionKey = cosmosMutation.getConcatenatedPartitionKey();

    // Act
    Record actual = cosmosMutation.makeRecord();

    // Assert
    assertThat(actual.getId()).isEqualTo(id);
    assertThat(actual.getConcatenatedPartitionKey()).isEqualTo(concatenatedPartitionKey);
    assertThat(actual.getPartitionKey().get(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(actual.getClusteringKey().get(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(actual.getValues().get(ANY_NAME_3)).isEqualTo(ANY_INT_1);
    assertThat(actual.getValues().get(ANY_NAME_4)).isEqualTo(ANY_INT_2);
  }

  @Test
  public void makeRecord_DeleteGiven_ShouldReturnEmpty() {
    // Arrange
    Delete delete = prepareDelete();
    CosmosMutation cosmosMutation = new CosmosMutation(delete, metadataManager);

    // Act
    Record actual = cosmosMutation.makeRecord();

    // Assert
    assertThat(actual.getId()).isEqualTo("");
    assertThat(actual.getConcatenatedPartitionKey()).isEqualTo("");
  }

  @Test
  public void makeConditionalQuery_MutationWithoutConditionsGiven_ShouldReturnQuery() {
    // Arrange
    Put put = preparePut();
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadataManager);
    String id = cosmosMutation.getId();

    // Act
    String actual = cosmosMutation.makeConditionalQuery();

    // Assert
    assertThat(actual).isEqualTo("select * from Record r where r.id = '" + id + "'");
  }

  @Test
  public void makeConditionalQuery_MutationWithoutClusteringKeyGiven_ShouldReturnQuery() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new HashSet<String>(Arrays.asList(ANY_NAME_2)));

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Delete delete =
        new Delete(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);

    CosmosMutation cosmosMutation = new CosmosMutation(delete, metadataManager);
    String concatenatedPartitionKey = cosmosMutation.getConcatenatedPartitionKey();

    // Act
    String actual = cosmosMutation.makeConditionalQuery();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "select * from Record r where r.concatenatedPartitionKey = '"
                + concatenatedPartitionKey
                + "'");
  }

  @Test
  public void makeConditionalQuery_MutationWithoutAllClusteringKeyGiven_ShouldReturnQuery() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new HashSet<String>(Arrays.asList(ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    CosmosMutation cosmosMutation = new CosmosMutation(delete, metadataManager);
    String concatenatedPartitionKey = cosmosMutation.getConcatenatedPartitionKey();

    // Act
    String actual = cosmosMutation.makeConditionalQuery();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "select * from Record r where (r.concatenatedPartitionKey = '"
                + concatenatedPartitionKey
                + "' and r.clusteringKey."
                + ANY_NAME_2
                + " = '"
                + ANY_TEXT_2
                + "')");
  }

  @Test
  public void makeConditionalQuery_MutationWithConditionsGiven_ShouldReturnQuery() {
    // Arrange
    PutIf conditions =
        new PutIf(
            new ConditionalExpression(ANY_NAME_3, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, ANY_INT_VALUE, Operator.GT));
    Put put = preparePut().withCondition(conditions);
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadataManager);
    String id = cosmosMutation.getId();

    // Act
    String actual = cosmosMutation.makeConditionalQuery();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "select * from Record r where (r.id = '"
                + id
                + "' and r.values."
                + ANY_NAME_3
                + " = "
                + ANY_INT_3
                + " and r.values."
                + ANY_NAME_4
                + " > "
                + ANY_INT_3
                + ")");
  }
}
