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

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
import java.util.Collections;
import java.util.Map;
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
    when(transactionManager.begin(Collections.emptyMap())).thenReturn(transaction);

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

    verify(transactionManager).begin(Collections.emptyMap());
    verify(transaction).commit();
  }

  @Test
  public void
      execute_ThrowableFunctionGivenWithAttributes_WithDistributedTransactionManager_ShouldExecuteFunctionWithAttributes()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    Map<String, String> attributes = ImmutableMap.of("key", "value");
    when(transactionManager.begin(attributes)).thenReturn(transaction);

    int expected = 10;

    // Act Assert
    Integer result =
        TransactionExecutor.execute(
            transactionManager,
            attributes,
            t -> {
              assertThat(t).isEqualTo(transaction);
              return expected;
            });

    assertThat(result).isEqualTo(expected);

    verify(transactionManager).begin(attributes);
    verify(transaction).commit();
  }

  @Test
  public void
      execute_ThrowableFunctionGiven_WithDistributedTransactionManager_TransactionExceptionThrown_ShouldThrowTransactionExceptionWithRollBack()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin(Collections.emptyMap())).thenReturn(transaction);

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

    verify(transactionManager).begin(Collections.emptyMap());
    verify(transaction).rollback();
  }

  @Test
  public void
      execute_ThrowableFunctionGiven_WithDistributedTransactionManager_UnknownTransactionStatusExceptionThrown_ShouldThrowTransactionExceptionWithoutRollBack()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin(Collections.emptyMap())).thenReturn(transaction);

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

    verify(transactionManager).begin(Collections.emptyMap());
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
                  TransactionExecutor.execute(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class)))
          .thenReturn(null);

      // Act
      TransactionExecutor.execute(transactionManager, throwableConsumer);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.execute(
                  eq(transactionManager),
                  eq(Collections.emptyMap()),
                  any(ThrowableFunction.class)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void execute_ThrowableConsumerGivenWithAttributes_ShouldCallExecuteWithFunction()
      throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      Map<String, String> attributes = ImmutableMap.of("key", "value");
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer =
          mock(ThrowableConsumer.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.execute(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class)))
          .thenReturn(null);

      // Act
      TransactionExecutor.execute(transactionManager, attributes, throwableConsumer);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.execute(
                  eq(transactionManager), eq(attributes), any(ThrowableFunction.class)));
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
          .when(
              () ->
                  TransactionExecutor.execute(
                      transactionManager, Collections.emptyMap(), throwableFunction))
          .thenReturn(expected);

      // Act
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.execute(
                  transactionManager, Collections.emptyMap(), throwableFunction));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void executeWithRetries_ThrowableFunctionGivenWithAttributes_ShouldReturnResult()
      throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      Map<String, String> attributes = ImmutableMap.of("key", "value");
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      Object expected = new Object();

      mocked
          .when(
              () -> TransactionExecutor.execute(transactionManager, attributes, throwableFunction))
          .thenReturn(expected);

      // Act
      Object actual =
          TransactionExecutor.executeWithRetries(transactionManager, attributes, throwableFunction);

      // Assert
      mocked.verify(
          () -> TransactionExecutor.execute(transactionManager, attributes, throwableFunction));

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
          .when(
              () ->
                  TransactionExecutor.execute(
                      transactionManager, Collections.emptyMap(), throwableFunction))
          .thenThrow(exception)
          .thenThrow(exception)
          .thenReturn(expected);

      // Act Assert
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      mocked.verify(
          () ->
              TransactionExecutor.execute(
                  transactionManager, Collections.emptyMap(), throwableFunction),
          times(3));

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

      CommitConflictException exception = mock(CommitConflictException.class);
      when(exception.getMessage()).thenReturn("message");

      Object expected = new Object();

      mocked
          .when(
              () ->
                  TransactionExecutor.execute(
                      transactionManager, Collections.emptyMap(), throwableFunction))
          .thenThrow(exception)
          .thenThrow(exception)
          .thenReturn(expected);

      // Act Assert
      Object actual = TransactionExecutor.executeWithRetries(transactionManager, throwableFunction);

      mocked.verify(
          () ->
              TransactionExecutor.execute(
                  transactionManager, Collections.emptyMap(), throwableFunction),
          times(3));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableFunctionGivenWithRetryParams_ShouldCallExecuteWithRetriesWithEmptyAttributes()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.executeWithRetries(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act
      TransactionExecutor.executeWithRetries(
          transactionManager, throwableFunction, 100, 1000, 2, 5);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  eq(Collections.emptyMap()),
                  eq(throwableFunction),
                  eq(100),
                  eq(1000),
                  eq(2),
                  eq(5)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableFunctionGivenWithAttributesAndRetryParams_ShouldCallExecuteWithRetriesWithAttributes()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      Map<String, String> attributes = ImmutableMap.of("key", "value");
      ThrowableFunction<CrudOperable<?>, Object, TransactionException> throwableFunction =
          mock(ThrowableFunction.class);

      Object expected = new Object();

      mocked
          .when(
              () ->
                  TransactionExecutor.execute(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class)))
          .thenReturn(expected);

      // Act
      Object actual =
          TransactionExecutor.executeWithRetries(
              transactionManager, attributes, throwableFunction, 100, 1000, 2, 5);

      // Assert
      mocked.verify(
          () -> TransactionExecutor.execute(transactionManager, attributes, throwableFunction));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void executeWithRetries_ThrowableConsumerGiven_ShouldCallExecuteWithRetriesWithFunction()
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
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act
      TransactionExecutor.executeWithRetries(transactionManager, throwableConsumer);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  eq(Collections.emptyMap()),
                  any(ThrowableFunction.class),
                  eq(100),
                  eq(1000),
                  eq(2),
                  eq(5)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableConsumerGivenWithAttributes_ShouldCallExecuteWithRetriesWithFunction()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      Map<String, String> attributes = ImmutableMap.of("key", "value");
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer =
          mock(ThrowableConsumer.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.executeWithRetries(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act
      TransactionExecutor.executeWithRetries(transactionManager, attributes, throwableConsumer);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  eq(attributes),
                  any(ThrowableFunction.class),
                  eq(100),
                  eq(1000),
                  eq(2),
                  eq(5)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableConsumerGivenWithRetryParams_ShouldCallExecuteWithRetriesWithEmptyAttributes()
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
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act
      TransactionExecutor.executeWithRetries(
          transactionManager, throwableConsumer, 100, 1000, 2, 5);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  eq(Collections.emptyMap()),
                  any(ThrowableFunction.class),
                  eq(100),
                  eq(1000),
                  eq(2),
                  eq(5)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void
      executeWithRetries_ThrowableConsumerGivenWithAttributesAndRetryParams_ShouldCallExecuteWithRetriesWithFunction()
          throws TransactionException {
    try (MockedStatic<TransactionExecutor> mocked =
        mockStatic(TransactionExecutor.class, CALLS_REAL_METHODS)) {
      // Arrange
      DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
      Map<String, String> attributes = ImmutableMap.of("key", "value");
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer =
          mock(ThrowableConsumer.class);

      mocked
          .when(
              () ->
                  TransactionExecutor.executeWithRetries(
                      any(DistributedTransactionManager.class),
                      any(Map.class),
                      any(ThrowableFunction.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt()))
          .thenReturn(null);

      // Act
      TransactionExecutor.executeWithRetries(
          transactionManager, attributes, throwableConsumer, 100, 1000, 2, 5);

      // Assert
      mocked.verify(
          () ->
              TransactionExecutor.executeWithRetries(
                  eq(transactionManager),
                  eq(attributes),
                  any(ThrowableFunction.class),
                  eq(100),
                  eq(1000),
                  eq(2),
                  eq(5)));
    }
  }
}
