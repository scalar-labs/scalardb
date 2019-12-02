package com.scalar.database.storage.cassandra4driver;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.google.common.base.Joiner;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Get;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.PutIfNotExists;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.storage.NoMutationException;
import com.scalar.database.exception.storage.ReadRepairableExecutionException;
import com.scalar.database.exception.storage.RetriableExecutionException;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** */
public class InsertStatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "ks";
  private static final String ANY_TABLE_NAME = "tbl";
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
  @Mock private CqlSession session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;
  @Mock private BoundStatementBuilder builder;
  @Mock private ResultSet results;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new InsertStatementHandler(session);
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

  private InsertStatementHandler prepareSpiedInsertStatementHandler() {
    InsertStatementHandler spy = spy(new InsertStatementHandler(session));
    doReturn(prepared).when(spy).prepare(put);
    doReturn(builder).when(spy).bind(prepared, put);
    return spy;
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
                  "INSERT INTO",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?)",
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
                  "INSERT INTO",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?)",
                });
    configureBehavior(expected);
    put = preparePutWithClusteringKey();

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
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "(" + ANY_NAME_1 + "," + ANY_NAME_2 + "," + ANY_NAME_3 + ")",
                  "VALUES",
                  "(?,?,?)",
                  "IF NOT EXISTS"
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
    verify(builder).setString(0, ANY_TEXT_1);
    verify(builder).setString(1, ANY_TEXT_2);
    verify(builder).setInt(2, ANY_INT_1);
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
  public void setConsistency_PutOperationWithIfNotExistsGiven_ShouldPrepareWithQuorumAndSerial() {
    // Arrange
    configureBehavior(null);
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists()).withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, put);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
    verify(builder).setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void handle_WTEWithCasThrown_ShouldThrowProperExecutionException()
      throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.CAS);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void
      handle_PutWithConditionGivenAndWTEWithSimpleThrown_ShouldThrowProperExecutionException()
          throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.SIMPLE);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(ReadRepairableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void
      handle_PutWithoutConditionGivenAndWTEWithSimpleThrown_ShouldThrowProperExecutionException()
          throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    spy = prepareSpiedInsertStatementHandler();

    WriteTimeoutException toThrow = mock(WriteTimeoutException.class);
    when(toThrow.getWriteType()).thenReturn(WriteType.SIMPLE);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_DriverExceptionThrown_ShouldThrowProperExecutionException()
      throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    spy = prepareSpiedInsertStatementHandler();

    DriverException toThrow = mock(DriverException.class);
    doThrow(toThrow).when(spy).handleInternal(put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_PutWithConditionButNoMutationApplied_ShouldThrowProperExecutionException()
      throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    ResultSet results = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(results.one()).thenReturn(row);
    when(row.getBoolean(0)).thenReturn(false);
    when(builder.build()).thenReturn(bound);
    doReturn(results).when(spy).execute(bound, put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutWithoutConditionButNoMutationApplied_ShouldThrowProperExecutionException()
      throws ExecutionException {
    // Arrange
    put = preparePutWithClusteringKey();
    put.withCondition(new PutIfNotExists());
    spy = prepareSpiedInsertStatementHandler();

    ResultSet results = mock(ResultSet.class);
    Row row = mock(Row.class);
    when(results.one()).thenReturn(row);
    when(row.getBoolean(0)).thenReturn(false);
    when(builder.build()).thenReturn(bound);
    doReturn(results).when(spy).execute(bound, put);

    // Act Assert
    assertThatThrownBy(
            () -> {
              spy.handle(put);
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Get.class);

    // Act Assert
    assertThatThrownBy(
            () -> {
              StatementHandler.checkArgument(operation, Put.class);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new InsertStatementHandler(null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
