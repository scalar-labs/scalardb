package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class BatchHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final int ANY_INT_1 = 1;
  private BatchHandler batch;
  private BatchHandler spy;
  private StatementHandlerManager handlers;
  private List<Mutation> mutations;

  @Mock private Session session;
  @Mock private SelectStatementHandler select;
  @Mock private InsertStatementHandler insert;
  @Mock private UpdateStatementHandler update;
  @Mock private DeleteStatementHandler delete;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;
  @Mock private ResultSet results;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handlers =
        StatementHandlerManager.builder()
            .select(select)
            .insert(insert)
            .update(update)
            .delete(delete)
            .build();
    batch = new BatchHandler(session, handlers);
  }

  private void configureBehavior() {
    when(insert.prepare(any(Mutation.class))).thenReturn(prepared);
    when(insert.bind(any(PreparedStatement.class), any(Mutation.class))).thenReturn(bound);
    when(update.prepare(any(Mutation.class))).thenReturn(prepared);
    when(update.bind(any(PreparedStatement.class), any(Mutation.class))).thenReturn(bound);
    when(delete.prepare(any(Mutation.class))).thenReturn(prepared);
    when(delete.bind(any(PreparedStatement.class), any(Mutation.class))).thenReturn(bound);
  }

  private List<Mutation> prepareNonConditionalPuts() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey1 = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put1 =
        new Put(partitionKey, clusteringKey1)
            .withValue(ANY_NAME_3, ANY_INT_1)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Key clusteringKey2 = new Key(ANY_NAME_2, ANY_TEXT_3);
    Put put2 =
        new Put(partitionKey, clusteringKey2)
            .withValue(ANY_NAME_3, ANY_INT_1)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return Arrays.asList(put1, put2);
  }

  private List<Mutation> prepareConditionalPuts() {
    List<Mutation> mutations = prepareNonConditionalPuts();
    mutations.forEach(m -> m.withCondition(new PutIfNotExists()));
    return mutations;
  }

  private BatchHandler prepareSpiedBatchHandler() {
    return Mockito.spy(new BatchHandler(session, handlers));
  }

  @Test
  public void handle_CorrectHandlerAndConditionalOperationsGiven_ShouldExecuteProperly() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    when(session.execute(any(Statement.class))).thenReturn(results);
    when(results.wasApplied()).thenReturn(true);

    // Act Assert
    assertThatCode(() -> batch.handle(mutations)).doesNotThrowAnyException();

    // Assert
    verify(insert).prepare(mutations.get(0));
    verify(insert).bind(prepared, mutations.get(0));
    verify(insert).prepare(mutations.get(1));
    verify(insert).bind(prepared, mutations.get(1));
  }

  @Test
  public void handle_CorrectHandlerAndNonConditionalOperationsGiven_ShouldExecuteProperly() {
    // Arrange
    configureBehavior();
    mutations = prepareNonConditionalPuts();
    when(session.execute(any(Statement.class))).thenReturn(results);
    when(results.wasApplied()).thenReturn(true);

    // Act Assert
    assertThatCode(() -> batch.handle(mutations)).doesNotThrowAnyException();

    // Assert
    verify(insert).prepare(mutations.get(0));
    verify(insert).bind(prepared, mutations.get(0));
    verify(insert).prepare(mutations.get(1));
    verify(insert).bind(prepared, mutations.get(1));
  }

  @Test
  public void handle_CorrectHandlerAndConditionalPutAndUpdateGiven_ShouldExecuteProperly() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    mutations.get(1).withCondition(new PutIfExists());
    when(session.execute(any(Statement.class))).thenReturn(results);
    when(results.wasApplied()).thenReturn(true);

    // Act Assert
    assertThatCode(() -> batch.handle(mutations)).doesNotThrowAnyException();

    // Assert
    verify(insert).prepare(mutations.get(0));
    verify(insert).bind(prepared, mutations.get(0));
    verify(update).prepare(mutations.get(1));
    verify(update).bind(prepared, mutations.get(1));
  }

  @Test
  public void handle_CorrectHandlerAndAtLeastOneConditionalPutGiven_ShouldSetConsistencyProperly() {
    // Arrange
    configureBehavior();
    mutations = prepareNonConditionalPuts();
    mutations.get(1).withCondition(new PutIfNotExists());
    when(session.execute(any(Statement.class))).thenReturn(results);
    when(results.wasApplied()).thenReturn(true);
    spy = prepareSpiedBatchHandler();

    // Act Assert
    assertThatCode(() -> spy.handle(mutations)).doesNotThrowAnyException();

    // Assert
    verify(spy).setConsistencyForConditionalMutation(any(BatchStatement.class));
  }

  @Test
  public void handle_WTEThrownInLoggingInBatchExecution_ShouldThrowRetriableExecutionException() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    WriteTimeoutException e = mock(WriteTimeoutException.class);
    when(e.getWriteType()).thenReturn(WriteType.BATCH_LOG);
    when(session.execute(any(Statement.class))).thenThrow(e);

    // Act Assert
    assertThatThrownBy(() -> batch.handle(mutations))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(e);
  }

  @Test
  public void handle_WTEThrownInMutationInBatchExecution_ShouldExecuteProperly() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    WriteTimeoutException e = mock(WriteTimeoutException.class);
    when(e.getWriteType()).thenReturn(WriteType.BATCH);
    when(session.execute(any(Statement.class))).thenThrow(e);

    // Act Assert
    assertThatCode(() -> batch.handle(mutations)).doesNotThrowAnyException();
  }

  @Test
  public void handle_WTEThrownInCasInBatchExecution_ShouldThrowRetriableExecutionException() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    WriteTimeoutException e = mock(WriteTimeoutException.class);
    when(e.getWriteType()).thenReturn(WriteType.CAS);
    when(session.execute(any(Statement.class))).thenThrow(e);

    // Act Assert
    assertThatThrownBy(() -> batch.handle(mutations))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(e);
  }

  @Test
  public void
      handle_WTEThrownInSimpleWriteInBatchExecution_ShouldThrowRetriableExecutionException() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    WriteTimeoutException e = mock(WriteTimeoutException.class);
    when(e.getWriteType()).thenReturn(WriteType.SIMPLE);
    when(session.execute(any(Statement.class))).thenThrow(e);

    // Act Assert
    assertThatThrownBy(() -> batch.handle(mutations))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(e);
  }

  @Test
  public void handle_DriverExceptionThrownInExecution_ShouldThrowRetriableExecutionException() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    DriverException e = mock(DriverException.class);
    when(session.execute(any(Statement.class))).thenThrow(e);

    // Act Assert
    assertThatThrownBy(() -> batch.handle(mutations))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(e);
  }

  @Test
  public void handle_WasAppliedReturnedFalse_ShouldThrowRetriableExecutionException() {
    // Arrange
    configureBehavior();
    mutations = prepareConditionalPuts();
    when(session.execute(any(Statement.class))).thenReturn(results);
    when(results.wasApplied()).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> batch.handle(mutations)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new BatchHandler(null, null)).isInstanceOf(NullPointerException.class);
  }
}
