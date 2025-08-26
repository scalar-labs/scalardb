package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class UpdateStatementHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table_name";
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
  @Mock private Session session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new UpdateStatementHandler(session);
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .intValue(ANY_NAME_2, ANY_INT_1)
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Put preparePutWithClusteringKey() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Put preparePutWithReservedKeywords() {
    Key partitionKey = Key.ofText("from", ANY_TEXT_1);
    Key clusteringKey = Key.ofText("to", ANY_TEXT_2);
    return Put.newBuilder()
        .namespace("keyspace")
        .table("table")
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue("one", ANY_INT_1)
        .build();
  }

  private void configureBehavior(String expected) {
    when(session.prepare(expected == null ? anyString() : expected)).thenReturn(prepared);

    when(prepared.bind()).thenReturn(bound);
    when(bound.setString(anyInt(), anyString())).thenReturn(bound);
    when(bound.setInt(anyInt(), anyInt())).thenReturn(bound);
    when(bound.setConsistencyLevel(any(ConsistencyLevel.class))).thenReturn(bound);
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_2 + "=?," + ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?;"
                });
    configureBehavior(expected);
    put = preparePut();

    // Act
    handler.prepare(put);

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
                  "UPDATE",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_2 + "=?," + ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?;"
                });
    configureBehavior(expected);
    put = preparePut();

    // Act
    handler.prepare(put);
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?;"
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithReservedKeywordsGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "UPDATE",
                  "\"keyspace\"" + "." + "\"table\"",
                  "SET",
                  "\"one\"" + "=?",
                  "WHERE",
                  "\"from\"" + "=?",
                  "AND",
                  "\"to\"" + "=?;"
                });
    configureBehavior(expected);
    put = preparePutWithReservedKeywords();

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "SET",
                  ANY_NAME_3 + "=?",
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "IF EXISTS;"
                });
    configureBehavior(expected);
    put =
        Put.newBuilder(preparePutWithClusteringKey())
            .condition(ConditionBuilder.putIfExists())
            .build();

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
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
                  ANY_NAME_4 + "<=?;"
                });
    configureBehavior(expected);
    put =
        Put.newBuilder(preparePutWithClusteringKey())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_4).isEqualToInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_4).isNotEqualToInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_4).isGreaterThanInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_4).isGreaterThanOrEqualToInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_4).isLessThanInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_4).isLessThanOrEqualToInt(ANY_INT_2))
                    .build())
            .build();

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
    PutIfExists putIfExists = Mockito.spy(ConditionBuilder.putIfExists());
    put = Put.newBuilder(put).condition(putIfExists).build();

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
            ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_4).isEqualToInt(ANY_INT_2))
                .build());
    put = Put.newBuilder(put).condition(putIf).build();

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
    verify(bound).setInt(0, ANY_INT_1);
    verify(bound).setString(1, ANY_TEXT_1);
    verify(bound).setString(2, ANY_TEXT_2);
  }

  @Test
  public void bind_PutOperationWithIfGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put =
        Put.newBuilder(preparePutWithClusteringKey())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_4).isEqualToInt(ANY_INT_2))
                    .and(ConditionBuilder.column(ANY_NAME_5).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(bound).setInt(0, ANY_INT_1);
    verify(bound).setString(1, ANY_TEXT_1);
    verify(bound).setString(2, ANY_TEXT_2);
    verify(bound).setInt(3, ANY_INT_2);
    verify(bound).setString(4, ANY_TEXT_3);
  }

  @Test
  public void bind_PutOperationWithNullValueGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put = Put.newBuilder(preparePutWithClusteringKey()).intValue(ANY_NAME_3, null).build();

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(bound).setToNull(0);
    verify(bound).setString(1, ANY_TEXT_1);
    verify(bound).setString(2, ANY_TEXT_2);
  }

  @Test
  public void setConsistency_PutOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    put = Put.newBuilder(preparePutWithClusteringKey()).consistency(Consistency.SEQUENTIAL).build();

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    put = Put.newBuilder(preparePutWithClusteringKey()).consistency(Consistency.EVENTUAL).build();

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_PutOperationWithLinearizableConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    put =
        Put.newBuilder(preparePutWithClusteringKey()).consistency(Consistency.LINEARIZABLE).build();

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithIfExistsGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put =
        Put.newBuilder(preparePutWithClusteringKey())
            .condition(ConditionBuilder.putIfExists())
            .consistency(Consistency.EVENTUAL)
            .build();

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(bound).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void setConsistency_PutOperationWithIfGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put =
        Put.newBuilder(preparePutWithClusteringKey())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_4).isEqualToInt(ANY_INT_2))
                    .build())
            .consistency(Consistency.EVENTUAL)
            .build();

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(bound).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  /** Unit testing for checkArgument() method is covered in InsertStatementHandlerTest */
  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new UpdateStatementHandler(null))
        .isInstanceOf(NullPointerException.class);
  }
}
