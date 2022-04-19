package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.google.common.base.Joiner;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class InsertStatementHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table_name";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;
  private InsertStatementHandler handler;
  private Put put;
  private InsertStatementHandler spy;
  @Mock private Session session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new InsertStatementHandler(session);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Put(partitionKey)
        .withValue(ANY_NAME_2, ANY_INT_1)
        .withValue(ANY_NAME_3, ANY_INT_2)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePutWithClusteringKey() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withValue(ANY_NAME_3, ANY_INT_1)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePutWithReservedKeywords() {
    Key partitionKey = new Key("from", ANY_TEXT_1);
    Key clusteringKey = new Key("to", ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withValue("one", ANY_INT_1)
        .forNamespace("keyspace")
        .forTable("table");
  }

  private InsertStatementHandler prepareSpiedInsertStatementHandler() {
    InsertStatementHandler spy = spy(new InsertStatementHandler(session));
    doReturn(prepared).when(spy).prepare(put);
    doReturn(bound).when(spy).bind(prepared, put);
    return spy;
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
                  "INSERT INTO",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?);",
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
                  "INSERT INTO",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?);",
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
                  "INSERT INTO",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?);",
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
                  "INSERT INTO",
                  "\"keyspace\"" + "." + "\"table\"",
                  "(" + "\"from\"" + "," + "\"to\"" + "," + "\"one\"" + ")",
                  "VALUES",
                  "(?,?,?);",
                });
    configureBehavior(expected);
    put = preparePutWithReservedKeywords();

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithIfNotExistsGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "INSERT INTO",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?)",
                  "IF NOT EXISTS;"
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());

    // Act
    handler.prepare(put);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_PutOperationWithIfNotExistsGiven_ShouldCallAccept() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    PutIfNotExists putIfNotExists = spy(new PutIfNotExists());
    put.withCondition(putIfNotExists);

    // Act
    handler.prepare(put);

    // Assert
    verify(putIfNotExists).accept(any(ConditionSetter.class));
  }

  @Test
  public void bind_PutOperationGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
    verify(bound).setInt(2, ANY_INT_1);
  }

  @Test
  public void bind_PutOperationWithNullValueGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withIntValue(ANY_NAME_3, null);

    // Act
    handler.bind(prepared, put);

    // Assert
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
    verify(bound).setToNull(2);
  }

  @Test
  public void setConsistency_PutOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.EVENTUAL);

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
    put = preparePutWithClusteringKey();
    put.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_PutOperationWithIfNotExistsGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists()).withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, put);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(bound).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void handle_WTEWithCasThrown_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.CAS);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void
      handle_PutWithConditionGivenAndWTEWithSimpleThrown_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.SIMPLE);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put))
        .isInstanceOf(ReadRepairableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void
      handle_PutWithoutConditionGivenAndWTEWithSimpleThrown_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.SIMPLE);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_DriverExceptionThrown_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    spy = prepareSpiedInsertStatementHandler();

    DriverException toThrow = mock(DriverException.class);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_PutWithConditionButNoMutationApplied_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    ResultSet results = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(results.one()).thenReturn(row);
    when(row.getBool(0)).thenReturn(false);
    doReturn(results).when(spy).execute(bound, put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutWithoutConditionButNoMutationApplied_ShouldThrowProperExecutionException() {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    ResultSet results = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(results.one()).thenReturn(row);
    when(row.getBool(0)).thenReturn(false);
    doReturn(results).when(spy).execute(bound, put);

    // Act Assert
    assertThatThrownBy(() -> spy.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Get.class);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.checkArgument(operation, Put.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new InsertStatementHandler(null))
        .isInstanceOf(NullPointerException.class);
  }
}
