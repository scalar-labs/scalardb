package com.scalar.db.common;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionUtils;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionExecutor {

  private static final Logger logger = LoggerFactory.getLogger(TransactionExecutor.class);

  private TransactionExecutor() {}

  public static <T> T execute(
      DistributedTransactionManager transactionManager,
      ThrowableFunction<CrudOperable<?>, T, TransactionException> throwableFunction)
      throws TransactionException {
    if (SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
        transactionManager)) {
      return throwableFunction.apply(transactionManager);
    }

    DistributedTransaction transaction = null;
    try {
      transaction = transactionManager.begin();
      T result = throwableFunction.apply(transaction);
      transaction.commit();
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        rollback(transaction);
      }

      throw e;
    }
  }

  public static void execute(
      DistributedTransactionManager transactionManager,
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableFunction)
      throws TransactionException {
    if (SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
        transactionManager)) {
      throwableFunction.accept(transactionManager);
      return;
    }

    DistributedTransaction transaction = null;
    try {
      transaction = transactionManager.begin();
      throwableFunction.accept(transaction);
      transaction.commit();
    } catch (Exception e) {
      if (transaction != null) {
        rollback(transaction);
      }

      throw e;
    }
  }

  private static void rollback(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      logger.warn("Failed to rollback a transaction", e);
    }
  }
}
