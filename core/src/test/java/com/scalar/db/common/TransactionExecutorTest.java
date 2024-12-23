package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TransactionExecutorTest {

  @Test
  public void
      execute_ThrowableFunctionGiven_WithSingleCrudOperationTransactionManager_ShouldExecuteFunction()
          throws TransactionException {
    // Arrange
    DistributedTransactionManager transactionManager =
        mock(SingleCrudOperationTransactionManager.class);

    int expected = 10;

    // Act Assert
    Integer result =
        TransactionExecutor.execute(
            transactionManager,
            t -> {
              assertThat(t).isEqualTo(transactionManager);
              return expected;
            });

    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void
      execute_ThrowableFunctionGiven_WithDistributedTransactionManager_ShouldExecuteFunction()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin()).thenReturn(transaction);

    int expected = 10;

    // Act Assert
    Integer result =
        TransactionExecutor.execute(
            transactionManager,
            t -> {
              assertThat(t).isEqualTo(transaction);
              return expected;
            });

    assertThat(result).isEqualTo(expected);

    verify(transactionManager).begin();
    verify(transaction).commit();
  }

  @Test
  public void
      execute_ThrowableFunctionGiven_WithDistributedTransactionManager_TransactionExceptionThrown_ShouldThrowTransactionExceptionWithRollBack()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin()).thenReturn(transaction);

    TransactionException exception = mock(TransactionException.class);

    // Act Assert
    assertThatThrownBy(
            () ->
                TransactionExecutor.execute(
                    transactionManager,
                    (ThrowableFunction<CrudOperable<?>, Object, TransactionException>)
                        t -> {
                          throw exception;
                        }))
        .isEqualTo(exception);

    verify(transactionManager).begin();
    verify(transaction).rollback();
  }

  @Test
  public void
      execute_ThrowableFunctionGiven_WithDistributedTransactionManager_UnknownTransactionStatusExceptionThrown_ShouldThrowTransactionExceptionWithoutRollBack()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin()).thenReturn(transaction);

    UnknownTransactionStatusException exception = mock(UnknownTransactionStatusException.class);

    // Act Assert
    assertThatThrownBy(
            () ->
                TransactionExecutor.execute(
                    transactionManager,
                    (ThrowableFunction<CrudOperable<?>, Object, TransactionException>)
                        t -> {
                          throw exception;
                        }))
        .isEqualTo(exception);

    verify(transactionManager).begin();
    verify(transaction, never()).rollback();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void execute_ThrowableConsumerGiven_ShouldCallExecuteWithFunction()
      throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer =
          mock(ThrowableConsumer.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.execute(eq(transactionManager), any(ThrowableFunction.class)))
          .thenReturn(null);

      // Act
      TransactionExecutor.execute(transactionManager, throwableConsumer);

      // Assert
      mocked.verify(
          () -> TransactionExecutor.execute(eq(transactionManager), any(ThrowableFunction.class)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void executeWithRetries_ThrowableFunctionGiven_ShouldReturnResult()
      throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      Object expected = new Object();

      mocked
          .when(() -> TransactionExecutor.execute(transactionManager, throwableFunction))
          .thenReturn(expected);

      // Act
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      // Assert
      mocked.verify(() -> TransactionExecutor.execute(transactionManager, throwableFunction));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableFunctionGiven_CrudConflictExceptionThrownTwoTimesThenReturnResult_ShouldRetryAndReturnResult()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      CrudConflictException exception = mock(CrudConflictException.class);
      when(exception.getMessage()).thenReturn("message");

      Object expected = new Object();

      mocked
          .when(() -> TransactionExecutor.execute(transactionManager, throwableFunction))
          .thenThrow(exception)
          .thenThrow(exception)
          .thenReturn(expected);

      // Act Assert
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      mocked.verify(
          () -> TransactionExecutor.execute(transactionManager, throwableFunction), times(3));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableFunctionGiven_CommitConflictExceptionThrownTwoTimesThenReturnResult_ShouldRetryAndReturnResult()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      CrudConflictException exception = mock(CrudConflictException.class);
      when(exception.getMessage()).thenReturn("message");

      Object expected = new Object();

      mocked
          .when(() -> TransactionExecutor.execute(transactionManager, throwableFunction))
          .thenThrow(exception)
          .thenThrow(exception)
          .thenReturn(expected);

      // Act Assert
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      mocked.verify(
          () -> TransactionExecutor.execute(transactionManager, throwableFunction), times(3));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableConsumerGiven_CommitConflictExceptionThrownTwoTimesThenReturnResult_ShouldRetryAndReturnResult()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer =
          mock(ThrowableConsumer.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.executeWithRetries(
                      eq(transactionManager),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act Assert
      TransactionExecutor.executeWithRetries(transactionManager, throwableConsumer);

      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  any(ThrowableFunction.class),
                  anyInt(),
                  anyInt(),
                  anyInt(),
                  anyInt()));
    }
  }
}
