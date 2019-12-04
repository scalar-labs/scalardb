package com.scalar.database.storage.cassandra;

import static com.scalar.database.api.ConditionalExpression.Operator;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.base.Joiner;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Put;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.PutIfExists;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** */
public class UpdateStatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "ks";
  private static final String ANY_TABLE_NAME = "tbl";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;
  private UpdateStatementHandler handler;
  private Put put;
  @Mock private CqlSession session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatementBuilder builder;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new UpdateStatementHandler(session);
  }

  private Put preparePut() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Put put =
        new Put(partitionKey)
            .withValue(new IntValue(ANY_NAME_2, ANY_INT_1))
            .withValue(new IntValue(ANY_NAME_3, ANY_INT_2))
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return put;
  }

  private Put preparePutWithClusteringKey() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new IntValue(ANY_NAME_3, ANY_INT_1))
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return put;
  }

  private void configureBehavior(String expected) {
    when(session.prepare(expected == null ? anyString() : expected)).thenReturn(prepared);

    when(prepared.boundStatementBuilder()).thenReturn(builder);
    when(builder.setString(anyInt(), anyString())).thenReturn(builder);
    when(builder.setInt(anyInt(), anyInt())).thenReturn(builder);
    when(builder.setConsistencyLevel(any(ConsistencyLevel.class))).thenReturn(builder);
  }

  @Test
  public void prepare_PutOperationWithoutClusteringKeyGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "UPDATE",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_2 + "=?, " + ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?"
                });
    configureBehavior(expected);
    put = preparePut();

    // Act
    handler.prepare(put);

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
                  "UPDATE",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?"
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithIfExistsGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "UPDATE",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "IF EXISTS"
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfExists());

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithIfGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "UPDATE",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "IF",
                  ANY_NAME_4 + "=?",
                  "AND",
                  ANY_NAME_4 + "!=?",
                  "AND",
                  ANY_NAME_4 + ">?",
                  "AND",
                  ANY_NAME_4 + ">=?",
                  "AND",
                  ANY_NAME_4 + "<?",
                  "AND",
                  ANY_NAME_4 + "<=?"
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();
    put.withCondition(
        new PutIf(
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.NE),
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.GT),
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.GTE),
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.LT),
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.LTE)));

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithIfExistsGiven_ShouldCallAccept() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    PutIfExists putIfExists = Mockito.spy(new PutIfExists());
    put.withCondition(putIfExists);

    // Act
    handler.prepare(put);

    // Assert
    verify(putIfExists).accept(any(ConditionSetter.class));
  }

  @Test
  public void prepare_PutOperationWithIfGiven_ShouldCallAccept() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    PutIf putIf =
        Mockito.spy(
            new PutIf(new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.EQ)));
    put.withCondition(putIf);

    // Act
    handler.prepare(put);

    // Assert
    verify(putIf).accept(any(ConditionSetter.class));
  }

  @Test
  public void bind_PutOperationGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(builder).setInt(0, ANY_INT_1);
    verify(builder).setString(1, ANY_TEXT_1);
    verify(builder).setString(2, ANY_TEXT_2);
  }

  @Test
  public void bind_PutOperationWithIfGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withCondition(
        new PutIf(
            new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.EQ),
            new ConditionalExpression(ANY_NAME_5, new TextValue(ANY_TEXT_3), Operator.EQ)));

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(builder).setInt(0, ANY_INT_1);
    verify(builder).setString(1, ANY_TEXT_1);
    verify(builder).setString(2, ANY_TEXT_2);
    verify(builder).setInt(3, ANY_INT_2);
    verify(builder).setString(4, ANY_TEXT_3);
  }

  @Test
  public void setConsistency_PutOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_PutOperationWithLinearizableConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithIfExistsGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfExists()).withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(builder).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void setConsistency_PutOperationWithIfGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withCondition(
            new PutIf(new ConditionalExpression(ANY_NAME_4, new IntValue(ANY_INT_2), Operator.EQ)))
        .withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(builder).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  /** Unit testing for handle() method is covered in InsertStatementHandlerTest */

  /** Unit testing for checkArgument() method is covered in InsertStatementHandlerTest */
  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new UpdateStatementHandler(null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
