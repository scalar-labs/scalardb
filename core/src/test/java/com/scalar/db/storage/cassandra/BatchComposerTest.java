package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class BatchComposerTest {
  private BatchComposer spy;
  @Mock private BatchStatement batch;
  @Mock private StatementHandlerManager handlers;
  @Mock private SelectStatementHandler select;
  @Mock private InsertStatementHandler insert;
  @Mock private UpdateStatementHandler update;
  @Mock private DeleteStatementHandler delete;
  @Mock private Get get;
  @Mock private Scan scan;
  @Mock private Put put;
  @Mock private Delete del;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    spy = Mockito.spy(new BatchComposer(batch, handlers));
    doNothing().when(spy).composeWith(any(StatementHandler.class), any(Operation.class));
  }

  @Test
  public void visit_GetGiven_ShouldComposeWithSelectHandler() {
    // Arrange
    when(handlers.select()).thenReturn(select);

    // Act Assert
    assertThatCode(() -> spy.visit(get)).doesNotThrowAnyException();

    // Assert
    verify(handlers).select();
    verify(spy).composeWith(select, get);
  }

  @Test
  public void visit_ScanGiven_ShouldComposeWithSelectHandler() {
    // Arrange
    when(handlers.select()).thenReturn(select);

    // Act Assert
    assertThatCode(() -> spy.visit(scan)).doesNotThrowAnyException();

    // Assert
    verify(handlers).select();
    verify(spy).composeWith(select, scan);
  }

  @Test
  public void visit_PutGiven_ShouldComposeWithInsertHandler() {
    // Arrange
    when(handlers.get(any(Operation.class))).thenReturn(insert);

    // Act Assert
    assertThatCode(() -> spy.visit(put)).doesNotThrowAnyException();

    // Assert
    verify(spy).composeWith(insert, put);
  }

  @Test
  public void visit_PutForUpdateGiven_ShouldComposeWithUpdateHandler() {
    // Arrange
    when(handlers.get(any(Operation.class))).thenReturn(update);

    // Act Assert
    assertThatCode(() -> spy.visit(put)).doesNotThrowAnyException();

    // Assert
    verify(spy).composeWith(update, put);
  }

  @Test
  public void visit_DeleteGiven_ShouldComposeWithDeleteHandler() {
    // Arrange
    when(handlers.delete()).thenReturn(delete);

    // Act Assert
    assertThatCode(() -> spy.visit(del)).doesNotThrowAnyException();

    // Assert
    verify(spy).composeWith(delete, del);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new BatchComposer(null, null))
        .isInstanceOf(NullPointerException.class);
  }
}
