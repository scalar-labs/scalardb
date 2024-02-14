package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementHandlerManagerTest {
  private StatementHandlerManager manager;
  @Mock private SelectStatementHandler select;
  @Mock private InsertStatementHandler insert;
  @Mock private UpdateStatementHandler update;
  @Mock private DeleteStatementHandler delete;
  @Mock private Get get;
  @Mock private Put put;
  @Mock private Delete del;
  @Mock private PutIf putIf;
  @Mock private PutIfExists putIfExists;
  @Mock private PutIfNotExists putIfNotExists;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private StatementHandlerManager prepareManager() {
    return StatementHandlerManager.builder()
        .select(select)
        .insert(insert)
        .update(update)
        .delete(delete)
        .build();
  }

  @Test
  public void build_AllHandlersGiven_ShouldInstantiate() {
    // Arrange

    // Act Assert
    assertThatCode(
            () ->
                StatementHandlerManager.builder()
                    .select(select)
                    .insert(insert)
                    .update(update)
                    .delete(delete)
                    .build())
        .doesNotThrowAnyException();
  }

  @Test
  public void build_SomeHandlerNotGiven_ShouldNotInstantiate() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                StatementHandlerManager.builder()
                    .select(select)
                    .insert(insert)
                    .update(update)
                    .build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void get_PutWithPutIfNotExistsGiven_ShouldReturnInsertHandler() {
    // Arrange
    when(put.getCondition()).thenReturn(Optional.of(putIfNotExists));
    manager = prepareManager();

    // Act
    StatementHandler handler = manager.get(put);

    // Assert
    assertThat(handler).isEqualTo(insert);
  }

  @Test
  public void get_PutWithPutIfExistsGiven_ShouldReturnUpdateHandler() {
    // Arrange
    when(put.getCondition()).thenReturn(Optional.of(putIfExists));
    manager = prepareManager();

    // Act
    StatementHandler handler = manager.get(put);

    // Assert
    assertThat(handler).isEqualTo(update);
  }

  @Test
  public void get_PutWithPutIfGiven_ShouldReturnUpdateHandler() {
    // Arrange
    when(put.getCondition()).thenReturn(Optional.of(putIf));
    manager = prepareManager();

    // Act
    StatementHandler handler = manager.get(put);

    // Assert
    assertThat(handler).isEqualTo(update);
  }

  @Test
  public void get_GetGiven_ShouldReturnSelectHandler() {
    // Arrange
    manager = prepareManager();

    // Act
    StatementHandler handler = manager.get(get);

    // Assert
    assertThat(handler).isEqualTo(select);
  }

  @Test
  public void get_DeleteGiven_ShouldReturnDeleteHandler() {
    // Arrange
    manager = prepareManager();

    // Act
    StatementHandler handler = manager.get(del);

    // Assert
    assertThat(handler).isEqualTo(delete);
  }
}
