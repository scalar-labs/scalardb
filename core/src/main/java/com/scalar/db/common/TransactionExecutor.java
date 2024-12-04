package com.scalar.db.common;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionUtils;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionExecutor {

  private static final Logger logger = LoggerFactory.getLogger(TransactionExecutor.class);

  private static final int DEFAULT_RETRY_INITIAL_INTERVAL_MILLIS = 100;
  private static final int DEFAULT_RETRY_MAX_INTERVAL_MILLIS = 1000;
  private static final int DEFAULT_RETRY_MULTIPLIER = 2;
  private static final int DEFAULT_RETRY_MAX_RETRIES = 5;

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
    } catch (UnknownTransactionStatusException e) {
      // We don't need to rollback the transaction for UnknownTransactionStatusException
      throw e;
    } catch (Exception e) {
      if (transaction != null) {
        rollback(transaction);
      }

      throw e;
    }
  }

  public static void execute(
      DistributedTransactionManager transactionManager,
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer)
      throws TransactionException {
    execute(
        transactionManager,
        t -> {
          throwableConsumer.accept(t);
          return null;
        });
  }

  private static void rollback(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      logger.warn("Failed to rollback a transaction", e);
    }
  }

  public static <T> T executeWithRetries(
      DistributedTransactionManager transactionManager,
      ThrowableFunction<CrudOperable<?>, T, TransactionException> throwableFunction)
      throws TransactionException {
    return executeWithRetries(
        transactionManager,
        throwableFunction,
        DEFAULT_RETRY_INITIAL_INTERVAL_MILLIS,
        DEFAULT_RETRY_MAX_INTERVAL_MILLIS,
        DEFAULT_RETRY_MULTIPLIER,
        DEFAULT_RETRY_MAX_RETRIES);
  }

  public static <T> T executeWithRetries(
      DistributedTransactionManager transactionManager,
      ThrowableFunction<CrudOperable<?>, T, TransactionException> throwableFunction,
      int retryInitialIntervalMillis,
      int retryMaxIntervalMillis,
      int retryMultiplier,
      int retryMaxRetries)
      throws TransactionException {
    TransactionException lastException;
    int interval = retryInitialIntervalMillis;
    int attempt = 0;
    while (true) {
      try {
        return execute(transactionManager, throwableFunction);
      } catch (CrudConflictException | CommitConflictException e) {
        // Retry the transaction for the conflict exceptions
        lastException = e;
      }

      if (attempt++ >= retryMaxRetries) {
        break;
      }

      logger.warn(
          "The transaction failed. Retrying after {} milliseconds... The current attempt count: {}.",
          interval,
          attempt,
          lastException);

      Uninterruptibles.sleepUninterruptibly(interval, TimeUnit.MILLISECONDS);

      interval *= retryMultiplier;
      if (interval > retryMaxIntervalMillis) {
        interval = retryMaxIntervalMillis;
      }
    }

    logger.error("The transaction failed after {} retries.", retryMaxRetries, lastException);
    throw lastException;
  }

  public static void executeWithRetries(
      DistributedTransactionManager transactionManager,
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer)
      throws TransactionException {
    executeWithRetries(
        transactionManager,
        throwableConsumer,
        DEFAULT_RETRY_INITIAL_INTERVAL_MILLIS,
        DEFAULT_RETRY_MAX_INTERVAL_MILLIS,
        DEFAULT_RETRY_MULTIPLIER,
        DEFAULT_RETRY_MAX_RETRIES);
  }

  public static void executeWithRetries(
      DistributedTransactionManager transactionManager,
      ThrowableConsumer<CrudOperable<?>, TransactionException> throwableConsumer,
      int retryInitialIntervalMillis,
      int retryMaxIntervalMillis,
      int retryMultiplier,
      int retryMaxRetries)
      throws TransactionException {
    executeWithRetries(
        transactionManager,
        t -> {
          throwableConsumer.accept(t);
          return null;
        },
        retryInitialIntervalMillis,
        retryMaxIntervalMillis,
        retryMultiplier,
        retryMaxRetries);
  }
}
