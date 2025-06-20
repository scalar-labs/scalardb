package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.TextValue;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RecoveryHandlerTest {

  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_ID_1 = "id1";
  private static final long ANY_TIME_1 = 100;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .build());

  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private Selection selection;

  private RecoveryHandler handler;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    handler = spy(new RecoveryHandler(storage, coordinator, tableMetadataManager));
  }

  private TransactionResult prepareResult(long preparedAt, TransactionState transactionState) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, ScalarDbUtils.toColumn(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID_1)))
            .put(
                Attribute.PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toPreparedAtValue(preparedAt)))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(Attribute.toStateValue(transactionState)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(1)))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult preparePreparedResult(long preparedAt) {
    return prepareResult(preparedAt, TransactionState.PREPARED);
  }

  @Test
  public void recover_SelectionAndResultGivenWhenCoordinatorStateCommitted_ShouldRollforward()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED));
    doNothing().when(handler).rollforwardRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result, state);

    // Assert
    verify(handler).rollforwardRecord(selection, result);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateCommitted_NoMutationExceptionThrownByStorage_ShouldNotThrowAnyException()
          throws ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED));
    CommitMutationComposer composer = mock(CommitMutationComposer.class);
    List<Mutation> mutations = Collections.singletonList(mock(Mutation.class));
    doReturn(mutations).when(composer).get();
    doReturn(composer).when(handler).createCommitMutationComposer(selection, result);
    doThrow(NoMutationException.class).when(storage).mutate(any());

    // Act Assert
    assertThatCode(() -> handler.recover(selection, result, state)).doesNotThrowAnyException();

    verify(handler).rollforwardRecord(selection, result);
    verify(composer).get();
    verify(storage).mutate(mutations);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateCommitted_ExecutionExceptionThrownByStorage_ShouldThrowExecutionException()
          throws ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.COMMITTED));
    CommitMutationComposer composer = mock(CommitMutationComposer.class);
    List<Mutation> mutations = Collections.singletonList(mock(Mutation.class));
    doReturn(mutations).when(composer).get();
    doReturn(composer).when(handler).createCommitMutationComposer(selection, result);
    doThrow(ExecutionException.class).when(storage).mutate(any());

    // Act Assert
    assertThatThrownBy(() -> handler.recover(selection, result, state))
        .isInstanceOf(ExecutionException.class);

    verify(handler).rollforwardRecord(selection, result);
    verify(composer).get();
    verify(storage).mutate(mutations);
  }

  @Test
  public void recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ShouldRollback()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result, state);

    // Assert
    verify(handler).rollbackRecord(selection, result);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateAborted_NoMutationExceptionThrownByStorage_ShouldNotThrowAnyException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    RollbackMutationComposer composer = mock(RollbackMutationComposer.class);
    List<Mutation> mutations = Collections.singletonList(mock(Mutation.class));
    doReturn(mutations).when(composer).get();
    doReturn(composer).when(handler).createRollbackMutationComposer(selection, result);
    doThrow(NoMutationException.class).when(storage).mutate(any());

    // Act
    assertThatCode(() -> handler.recover(selection, result, state)).doesNotThrowAnyException();

    // Assert
    verify(handler).rollbackRecord(selection, result);
    verify(composer).get();
    verify(storage).mutate(mutations);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateAborted_ExecutionExceptionThrownByStorage_ShouldThrowExecutionException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(ANY_TIME_1);
    Optional<Coordinator.State> state =
        Optional.of(new Coordinator.State(ANY_ID_1, TransactionState.ABORTED));
    RollbackMutationComposer composer = mock(RollbackMutationComposer.class);
    List<Mutation> mutations = Collections.singletonList(mock(Mutation.class));
    doReturn(mutations).when(composer).get();
    doReturn(composer).when(handler).createRollbackMutationComposer(selection, result);
    doThrow(ExecutionException.class).when(storage).mutate(any());

    // Act
    assertThatThrownBy(() -> handler.recover(selection, result, state))
        .isInstanceOf(ExecutionException.class);

    // Assert
    verify(handler).rollbackRecord(selection, result);
    verify(composer).get();
    verify(storage).mutate(mutations);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndNotExpired_ShouldDoNothing()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result = preparePreparedResult(System.currentTimeMillis());
    Optional<Coordinator.State> state = Optional.empty();
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result, state);

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
    Optional<Coordinator.State> state = Optional.empty();
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecord(any(Selection.class), any(TransactionResult.class));

    // Act
    handler.recover(selection, result, state);

    // Assert
    verify(coordinator).putStateForLazyRecoveryRollback(ANY_ID_1);
    verify(handler).rollbackRecord(selection, result);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_CoordinatorConflictExceptionByCoordinator_ShouldNotThrowAnyException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result =
        preparePreparedResult(
            System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS * 2);
    Optional<Coordinator.State> state = Optional.empty();
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putStateForLazyRecoveryRollback(anyString());

    // Act
    assertThatCode(() -> handler.recover(selection, result, state)).doesNotThrowAnyException();

    // Assert
    verify(coordinator).putStateForLazyRecoveryRollback(ANY_ID_1);
    verify(handler, never()).rollbackRecord(selection, result);
  }

  @Test
  public void
      recover_SelectionAndResultGivenWhenCoordinatorStateNotExistsAndExpired_CoordinatorExceptionByCoordinator_ShouldThrowCoordinatorException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult result =
        preparePreparedResult(
            System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS * 2);
    Optional<Coordinator.State> state = Optional.empty();
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putStateForLazyRecoveryRollback(anyString());

    // Act
    assertThatThrownBy(() -> handler.recover(selection, result, state))
        .isInstanceOf(CoordinatorException.class);

    // Assert
    verify(coordinator).putStateForLazyRecoveryRollback(ANY_ID_1);
    verify(handler, never()).rollbackRecord(selection, result);
  }
}
