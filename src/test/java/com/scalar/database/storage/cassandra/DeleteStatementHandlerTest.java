package com.scalar.database.storage.cassandra;

import static com.scalar.database.api.ConditionalExpression.Operator;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DeleteIf;
import com.scalar.database.api.DeleteIfExists;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** */
public class DeleteStatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;
  private DeleteStatementHandler handler;
  private Delete del;
  @Mock private Session session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new DeleteStatementHandler(session);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Delete del = new Delete(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
    return del;
  }

  private Delete prepareDeleteWithClusteringKey() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Delete del =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return del;
  }

  private void configureBehavior(String expected) {
    when(session.prepare(expected == null ? anyString() : expected)).thenReturn(prepared);

    when(prepared.bind()).thenReturn(bound);
    when(bound.setString(anyInt(), anyString())).thenReturn(bound);
    when(bound.setInt(anyInt(), anyInt())).thenReturn(bound);
    when(bound.setConsistencyLevel(any(ConsistencyLevel.class))).thenReturn(bound);
  }

  @Test
  public void prepare_DeleteOperationWithoutClusteringKeyGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "DELETE",
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?;"
                });
    configureBehavior(expected);
    del = prepareDelete();

    // Act
    handler.prepare(del);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_SameQueryGivenTwice_SecondTimeShouldUseStatementCache() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "DELETE",
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?;"
                });
    configureBehavior(expected);
    del = prepareDelete();

    // Act
    handler.prepare(del);
    handler.prepare(del);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithClusteringKeyGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "DELETE",
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?;"
                });
    configureBehavior(expected);
    del = prepareDeleteWithClusteringKey();

    // Act
    handler.prepare(del);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_DeleteOperationWithIfExistsGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "DELETE",
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "IF EXISTS;"
                });
    configureBehavior(expected);
    del = prepareDeleteWithClusteringKey();
    del.withCondition(new DeleteIfExists());

    // Act
    handler.prepare(del);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_DeleteOperationWithIfGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "DELETE",
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "IF",
                  ANY_NAME_3 + "=?",
                  "AND",
                  ANY_NAME_4 + "=?;"
                });
    configureBehavior(expected);
    del = prepareDeleteWithClusteringKey();
    del.withCondition(
        new DeleteIf(
            new ConditionalExpression(ANY_NAME_3, new IntValue(ANY_INT_1), Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, new TextValue(ANY_TEXT_3), Operator.EQ)));

    // Act
    handler.prepare(del);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_DeleteOperationWithIfExistsGiven_ShouldCallAccept() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    DeleteIfExists deleteIfExists = spy(new DeleteIfExists());
    del.withCondition(deleteIfExists);

    // Act
    handler.prepare(del);

    // Assert
    verify(deleteIfExists).accept(any(ConditionSetter.class));
  }

  @Test
  public void prepare_DeleteOperationWithIfGiven_ShouldCallAccept() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    DeleteIf deleteIf =
        spy(
            new DeleteIf(
                new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.EQ)));
    del.withCondition(deleteIf);

    // Act
    handler.prepare(del);

    // Assert
    verify(deleteIf).accept(any(ConditionSetter.class));
  }

  @Test
  public void bind_DeleteOperationGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();

    // Act
    handler.bind(prepared, del);

    // Assert
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
  }

  @Test
  public void bind_DeleteOperationWithIfGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withCondition(
        new DeleteIf(
            new ConditionalExpression(ANY_NAME_3, new IntValue(ANY_INT_1), Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, new TextValue(ANY_TEXT_3), Operator.EQ)));

    // Act
    handler.bind(prepared, del);

    // Assert
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
    verify(bound).setInt(2, ANY_INT_1);
    verify(bound).setString(3, ANY_TEXT_3);
  }

  @Test
  public void setConsistency_DeleteOperationWithStrongConsistencyGiven_ShouldBoundWithQuorum() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(bound, del);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_DeleteOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, del);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_DeleteOperationWithLinearizableConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(bound, del);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_DeleteOperationWithIfExistsGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withCondition(new DeleteIfExists()).withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, del);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(bound).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void setConsistency_DeleteOperationWithIfGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    del = prepareDeleteWithClusteringKey();
    del.withCondition(
            new DeleteIf(
                new ConditionalExpression(ANY_NAME_3, new IntValue(ANY_INT_1), Operator.EQ),
                new ConditionalExpression(ANY_NAME_4, new TextValue(ANY_TEXT_3), Operator.EQ)))
        .withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, del);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(bound).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  /** Unit testing for handle() method is covered in InsertStatementHandlerTest */
  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);

    // Act Assert
    assertThatThrownBy(
            () -> {
              StatementHandler.checkArgument(operation, Delete.class);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new DeleteStatementHandler(null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
