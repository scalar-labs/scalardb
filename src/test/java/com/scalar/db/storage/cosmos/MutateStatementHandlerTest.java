package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
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

public class MutateStatementHandlerTest {
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

  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

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

  // executeStoredProcedure() is tested in DeleteStatementHandlerTest and
  // PutStatementHandlerTest

  @Test
  public void makeRecord_PutGiven_ShouldReturnWithValues() {
    // Arrange
    PutStatementHandler handler = new PutStatementHandler(client, metadataManager);
    Put put = preparePut();
    String id = handler.getId(put);
    String concatenatedPartitionKey = handler.getConcatenatedPartitionKey(put);

    // Act
    Record actual = handler.makeRecord(put);

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
    DeleteStatementHandler handler = new DeleteStatementHandler(client, metadataManager);
    Delete delete = prepareDelete();

    // Act
    Record actual = handler.makeRecord(delete);

    // Assert
    assertThat(actual.getId()).isEqualTo("");
    assertThat(actual.getConcatenatedPartitionKey()).isEqualTo("");
  }

  @Test
  public void makeConditionalQuery_MutationWithoutConditionsGiven_ShouldReturnQuery() {
    // Arrange
    PutStatementHandler handler = new PutStatementHandler(client, metadataManager);
    Put put = preparePut();
    String id = handler.getId(put);

    // Act
    String actual = handler.makeConditionalQuery(put);

    // Assert
    assertThat(actual).isEqualTo("select * from Record r where r.id = '" + id + "'");
  }

  @Test
  public void makeConditionalQuery_MutationWithConditionsGiven_ShouldReturnQuery() {
    // Arrange
    PutStatementHandler handler = new PutStatementHandler(client, metadataManager);
    PutIf conditions =
        new PutIf(
            new ConditionalExpression(ANY_NAME_3, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, ANY_INT_VALUE, Operator.GT));
    Put put = preparePut().withCondition(conditions);
    String id = handler.getId(put);

    // Act
    String actual = handler.makeConditionalQuery(put);

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
