package com.scalar.db.transaction.consensuscommit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RecoveryHandlerTest {
  private static final String ANY_ID_1 = "id1";
  private static final long ANY_TIME_1 = 100;

  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private TransactionalTableMetadataManager tableMetadataManager;
  @Mock private Selection selection;

  private RecoveryHandler handler;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    handler = spy(new RecoveryHandler(storage, coordinator, tableMetadataManager));
  }

  private void configureResult(Result mock, long preparedAt, TransactionState transactionState) {
    ImmutableMap<String, Value<?>> values =
        ImmutableMap.<String, Value<?>>builder()
            .put(Attribute.ID, Attribute.toIdValue(ANY_ID_1))
            .put(Attribute.PREPARED_AT, Attribute.toPreparedAtValue(preparedAt))
            .put(Attribute.STATE, Attribute.toStateValue(transactionState))
            .put(Attribute.VERSION, Attribute.toVersionValue(1))
            .build();

    when(mock.getValues()).thenReturn(values);
  }

  private TransactionResult preparePreparedResult(long preparedAt) {
    Result result = mock(Result.class);
    configureResult(result, preparedAt, TransactionState.PREPARED);
    return new TransactionResult(result);
  }

  private TransactionResult prepareCommittedResult(long preparedAt) {
    Result result = mock(Result.class);
    configureResult(result, preparedAt, TransactionState.COMMITTED);
    return new TransactionResult(result);
  }

  @Test
  public void recover_SelectionAndResultGivenWhenStorageReturnsEmptyResult_ShouldDoNothing()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    doReturn(Optional.empty()).when(handler).getLatestResult(selection, result);

    // Act
    handler.recover(selection, result);

    // Assert
    verify(coordinator, never()).putState(any());
    verify(handler, never()).rollforwardRecord(any(), any());
    verify(handler, never()).rollbackRecord(any(), any());
  }

  @Test
  public void recover_SelectionAndResultGivenWhenStorageReturnsCommittedRecord_ShouldDoNothing()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    doReturn(Optional.of(prepareCommittedResult(ANY_TIME_1)))
        .when(handler)
        .getLatestResult(selection, result);

    // Act
    handler.recover(selection, result);

    // Assert
    verify(coordinator, never()).putState(any());
    verify(handler, never()).rollforwardRecord(any(), any());
    verify(handler, never()).rollbackRecord(any(), any());
  }

  @Test
  public void recover_SelectionAndResultGivenWhenCoordinatorStateCommitted_ShouldRollforward()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    doReturn(Optional.of(result)).when(handler).getLatestResult(selection, result);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED)));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result);

    // Assert
    verify(handler).rollforwardRecord(selection, result);
  }

  @Test
  public void recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ShouldRollback()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    doReturn(Optional.of(result)).when(handler).getLatestResult(selection, result);
    when(coordinator.getState(ANY_ID_1))
        .thenReturn(Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED)));
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result);

    // Assert
    verify(handler).rollbackRecord(selection, result);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldDoNothing()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(System.currentTimeMillis());
    doReturn(Optional.of(result)).when(handler).getLatestResult(selection, result);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result);

    // Assert
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(handler, never()).rollbackRecord(selection, result);
  }

  @Test
  public void recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_ShouldAbort()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result =
        preparePreparedResult(
            System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS * 2);
    doReturn(Optional.of(result)).when(handler).getLatestResult(selection, result);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    verify(handler).rollbackRecord(selection, result);
  }
}
