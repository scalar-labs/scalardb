package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ParallelExecutor {
  private static final Logger logger = LoggerFactory.getLogger(ParallelExecutor.class);

  @FunctionalInterface
  public interface ParallelExecutorTask {
    void run() throws ExecutionException, ValidationConflictException, CrudException;
  }

  private final ConsensusCommitConfig config;
  @Nullable private final ExecutorService parallelExecutorService;

  public ParallelExecutor(ConsensusCommitConfig config) {
    this.config = config;
    if (config.isParallelPreparationEnabled()
        || config.isParallelValidationEnabled()
        || config.isParallelCommitEnabled()
        || config.isParallelRollbackEnabled()
        || config.isParallelImplicitPreReadEnabled()
        || config.isParallelBatchGetEnabled()) {
      parallelExecutorService =
          Executors.newFixedThreadPool(
              config.getParallelExecutorCount(),
              // Make this thread factory create daemon threads not to block JVM termination. JVM
              // shutdown hook is executed before terminating daemon threads. So, daemon threads
              // created by this thread factory will be properly terminated after pre-termination
              // operations are done if the operations are set in JVM shutdown hook.
              new ThreadFactoryBuilder()
                  .setNameFormat("parallel-executor-%d")
                  .setDaemon(true)
                  .build());
    } else {
      parallelExecutorService = null;
    }
  }

  @VisibleForTesting
  ParallelExecutor(
      ConsensusCommitConfig config, @Nullable ExecutorService parallelExecutorService) {
    this.config = config;
    this.parallelExecutorService = parallelExecutorService;
  }

  public void prepareRecords(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException {
    try {
      // When parallel preparation is disabled, we stop running the tasks when one of them fails
      // (stopOnError=true). When not, however, we need to wait for all the tasks to finish even if
      // some of them fail (stopOnError=false). This is because enabling stopOnError in parallel
      // preparation would cause the corresponding rollback to be executed earlier than the
      // preparation of records, which could result in left-unrecovered records even in a normal
      // case. Thus, we disable stopOnError when parallel preparation is enabled.
      boolean stopOnError = !config.isParallelPreparationEnabled();

      executeTasks(
          tasks,
          config.isParallelPreparationEnabled(),
          stopOnError,
          "prepareRecords",
          transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for preparing a transaction should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void validateRecords(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException, ValidationConflictException {
    try {
      executeTasks(
          tasks, config.isParallelValidationEnabled(), true, "validateRecords", transactionId);
    } catch (CrudException e) {
      throw new AssertionError(
          "Tasks for validating a transaction should not throw CrudException", e);
    }
  }

  public void commitRecords(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException {
    try {
      executeTasks(tasks, config.isParallelCommitEnabled(), false, "commitRecords", transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for committing a transaction should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void rollbackRecords(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException {
    try {
      executeTasks(
          tasks, config.isParallelRollbackEnabled(), false, "rollbackRecords", transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for rolling back a transaction should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void readRecordsForRollback(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException {
    try {
      executeTasks(
          tasks,
          config.isParallelRollbackEnabled(),
          // A failed read abandons the rollback anyway, so this stops at the first one
          true,
          "readRecordsForRollback",
          transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for reading records for a rollback should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void executeImplicitPreRead(List<ParallelExecutorTask> tasks, String transactionId)
      throws CrudException {
    try {
      executeTasks(
          tasks,
          config.isParallelImplicitPreReadEnabled(),
          true,
          "executeImplicitPreRead",
          transactionId);
    } catch (ExecutionException | ValidationConflictException e) {
      throw new AssertionError(
          "Tasks for implicit pre-read should not throw ExecutionException and ValidationConflictException",
          e);
    }
  }

  public boolean isParallelBatchGetEnabled() {
    return config.isParallelBatchGetEnabled();
  }

  /**
   * Runs the tasks of a batch Get on at most {@link
   * ConsensusCommitConfig#getParallelBatchGetMaxConcurrencyPerBatch()} threads, the calling thread
   * and threads of the parallel executor, each of which runs the next task not started yet until
   * none is left. This waits for the workers that have started even if some of the tasks fail, so
   * that none of them is left running on the transaction after this returns, but not for the
   * workers still queued in the parallel executor, which return without running any task.
   */
  public void executeBatchGets(List<ParallelExecutorTask> tasks, String transactionId) {
    try {
      if (tasks.size() == 1 || !config.isParallelBatchGetEnabled()) {
        executeTasksSerially(tasks, false);
        return;
      }

      int workerCount =
          Math.min(tasks.size(), Math.max(1, config.getParallelBatchGetMaxConcurrencyPerBatch()));
      AtomicInteger next = new AtomicInteger();
      ParallelExecutorTask worker =
          () -> {
            for (int j = next.getAndIncrement(); j < tasks.size(); j = next.getAndIncrement()) {
              tasks.get(j).run();
            }
          };

      // The calling thread runs one of the workers, so that a batch hands one worker fewer to the
      // pool. Each submitted worker runs only if it claims itself before the calling thread does,
      // so that the calling thread doesn't have to wait for workers still queued in the pool
      assert parallelExecutorService != null;
      List<AtomicBoolean> claims = new ArrayList<>(workerCount - 1);
      List<Future<Void>> futures = new ArrayList<>(workerCount - 1);
      Throwable throwable = null;
      try {
        for (int i = 1; i < workerCount; i++) {
          AtomicBoolean claim = new AtomicBoolean();
          futures.add(
              parallelExecutorService.submit(
                  () -> {
                    if (claim.compareAndSet(false, true)) {
                      worker.run();
                    }
                    return null;
                  }));
          claims.add(claim);
        }

        worker.run();
      } catch (Throwable t) {
        // When a submission fails, e.g., because the parallel executor is shut down, the calling
        // thread doesn't run its worker, but still takes care of the workers already submitted
        throwable = t;
      }

      // Wait for the other workers that have started even on failure, since they use the
      // transaction context too. A worker that hasn't started returns without running any task
      for (int i = 0; i < futures.size(); i++) {
        if (claims.get(i).compareAndSet(false, true)) {
          // Cancel it so that the parallel executor doesn't keep the tasks and the transaction
          // context while it is queued
          futures.get(i).cancel(false);
          continue;
        }
        try {
          Uninterruptibles.getUninterruptibly(futures.get(i));
        } catch (java.util.concurrent.ExecutionException e) {
          if (throwable == null) {
            throwable = e.getCause();
          } else {
            throwable.addSuppressed(e.getCause());
          }
        }
      }

      if (throwable != null) {
        logger.warn(
            "Failed to run executeBatchGets tasks. Transaction ID: {}", transactionId, throwable);
        rethrow(throwable);
      }
    } catch (ExecutionException | ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for batch gets should not throw ExecutionException, ValidationConflictException, and CrudException",
          e);
    }
  }

  @VisibleForTesting
  void executeTasks(
      List<ParallelExecutorTask> tasks,
      boolean parallel,
      boolean stopOnError,
      String taskName,
      String transactionId)
      throws ExecutionException, ValidationConflictException, CrudException {
    if (tasks.size() == 1) {
      // If there is only one task, we can run it directly without parallel execution.
      tasks.get(0).run();
      return;
    }

    if (parallel) {
      executeTasksInParallel(tasks, stopOnError, taskName, transactionId);
    } else {
      executeTasksSerially(tasks, stopOnError);
    }
  }

  private void executeTasksInParallel(
      List<ParallelExecutorTask> tasks, boolean stopOnError, String taskName, String transactionId)
      throws ExecutionException, ValidationConflictException, CrudException {
    assert parallelExecutorService != null;

    CompletionService<Void> completionService =
        new ExecutorCompletionService<>(parallelExecutorService);

    // Submit tasks
    for (ParallelExecutorTask task : tasks) {
      completionService.submit(
          () -> {
            try {
              task.run();
            } catch (Exception e) {
              logger.warn(
                  "Failed to run a {} task. Transaction ID: {}", taskName, transactionId, e);
              throw e;
            }
            return null;
          });
    }

    Throwable throwable = null;

    for (int i = 0; i < tasks.size(); i++) {
      Future<Void> future = ScalarDbUtils.takeUninterruptibly(completionService);
      try {
        Uninterruptibles.getUninterruptibly(future);
      } catch (java.util.concurrent.ExecutionException e) {
        Throwable cause = e.getCause();

        if (stopOnError) {
          rethrow(cause);
        } else {
          if (throwable == null) {
            throwable = cause;
          } else {
            throwable.addSuppressed(cause);
          }
        }
      }
    }

    // Rethrow exception if necessary
    if (!stopOnError && throwable != null) {
      rethrow(throwable);
    }
  }

  private void executeTasksSerially(List<ParallelExecutorTask> tasks, boolean stopOnError)
      throws ExecutionException, ValidationConflictException, CrudException {
    Exception exception = null;
    for (ParallelExecutorTask task : tasks) {
      try {
        task.run();
      } catch (ExecutionException | ValidationConflictException | CrudException e) {
        if (!stopOnError) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        } else {
          throw e;
        }
      }
    }

    if (!stopOnError && exception != null) {
      rethrow(exception);
    }
  }

  private void rethrow(Throwable cause)
      throws ExecutionException, ValidationConflictException, CrudException {
    if (cause instanceof ExecutionException) {
      throw (ExecutionException) cause;
    } else if (cause instanceof ValidationConflictException) {
      throw (ValidationConflictException) cause;
    } else if (cause instanceof CrudException) {
      throw (CrudException) cause;
    } else if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else if (cause instanceof Error) {
      throw (Error) cause;
    } else {
      throw new AssertionError("Unexpected exception type", cause);
    }
  }

  public void close() {
    if (parallelExecutorService != null) {
      parallelExecutorService.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(parallelExecutorService);
    }
  }
}
