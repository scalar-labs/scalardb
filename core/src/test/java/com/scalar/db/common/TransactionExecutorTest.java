package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
import org.junit.jupiter.api.Test;

public class TransactionExecutorTest {

  @Test
  public void execute_WithSingleCrudOperationTransactionManager_ShouldExecuteFunction()
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
  public void execute2_WithSingleCrudOperationTransactionManager_ShouldExecuteFunction()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager transactionManager =
        mock(SingleCrudOperationTransactionManager.class);

    // Act Assert
    TransactionExecutor.execute(
        transactionManager,
        t -> {
          assertThat(t).isEqualTo(transactionManager);
        });
  }

  @Test
  public void execute_WithDistributedTransactionManager_ShouldExecuteFunction()
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
  public void execute2_WithDistributedTransactionManager_ShouldExecuteFunction()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    DistributedTransactionManager transactionManager = mock(DistributedTransactionManager.class);
    when(transactionManager.begin()).thenReturn(transaction);

    // Act Assert
    TransactionExecutor.execute(
        transactionManager,
        t -> {
          assertThat(t).isEqualTo(transaction);
        });

    verify(transactionManager).begin();
    verify(transaction).commit();
  }

  @Test
  public void
      execute_WithDistributedTransactionManager_TransactionExceptionThrown_ShouldThrowTransactionException()
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
      execute2_WithDistributedTransactionManager_TransactionExceptionThrown_ShouldThrowTransactionException()
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
                    (ThrowableConsumer<CrudOperable<?>, TransactionException>)
                        t -> {
                          throw exception;
                        }))
        .isEqualTo(exception);

    verify(transactionManager).begin();
    verify(transaction).rollback();
  }
}
