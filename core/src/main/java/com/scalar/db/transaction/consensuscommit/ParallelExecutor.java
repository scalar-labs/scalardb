package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        || config.isParallelImplicitPreReadEnabled()) {
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

  public void prepare(List<ParallelExecutorTask> tasks, String transactionId)
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
          false,
          stopOnError,
          "preparation",
          transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for preparing a transaction should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void validate(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException, ValidationConflictException {
    try {
      executeTasks(
          tasks, config.isParallelValidationEnabled(), false, true, "validation", transactionId);
    } catch (CrudException e) {
      throw new AssertionError(
          "Tasks for validating a transaction should not throw CrudException", e);
    }
  }

  public void commitRecords(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException {
    try {
      executeTasks(
          tasks,
          config.isParallelCommitEnabled(),
          config.isAsyncCommitEnabled(),
          false,
          "commitRecords",
          transactionId);
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
          tasks,
          config.isParallelRollbackEnabled(),
          config.isAsyncRollbackEnabled(),
          false,
          "rollbackRecords",
          transactionId);
    } catch (ValidationConflictException | CrudException e) {
      throw new AssertionError(
          "Tasks for rolling back a transaction should not throw ValidationConflictException and CrudException",
          e);
    }
  }

  public void executeImplicitPreRead(List<ParallelExecutorTask> tasks, String transactionId)
      throws CrudException {
    try {
      executeTasks(
          tasks,
          config.isParallelImplicitPreReadEnabled(),
          false,
          true,
          "executeImplicitPreRead",
          transactionId);
    } catch (ExecutionException | ValidationConflictException e) {
      throw new AssertionError(
          "Tasks for implicit pre-read should not throw ExecutionException and ValidationConflictException",
          e);
    }
  }

  private void executeTasks(
      List<ParallelExecutorTask> tasks,
      boolean parallel,
      boolean noWait,
      boolean stopOnError,
      String taskName,
      String transactionId)
      throws ExecutionException, ValidationConflictException, CrudException {
    if (parallel) {
      executeTasksInParallel(tasks, noWait, stopOnError, taskName, transactionId);
    } else {
      executeTasksSerially(tasks, stopOnError, taskName, transactionId);
    }
  }

  private void executeTasksInParallel(
      List<ParallelExecutorTask> tasks,
      boolean noWait,
      boolean stopOnError,
      String taskName,
      String transactionId)
      throws ExecutionException, ValidationConflictException, CrudException {
    assert parallelExecutorService != null;

    CompletionService<Void> completionService =
        new ExecutorCompletionService<>(parallelExecutorService);
    tasks.forEach(
        t ->
            completionService.submit(
                () -> {
                  try {
                    t.run();
                  } catch (Exception e) {
                    logger.warn(
                        "Failed to run a {} task. Transaction ID: {}", taskName, transactionId, e);
                    throw e;
                  }
                  return null;
                }));

    if (!noWait) {
      Exception exception = null;
      for (int i = 0; i < tasks.size(); i++) {
        Future<Void> future = ScalarDbUtils.takeUninterruptibly(completionService);

        try {
          Uninterruptibles.getUninterruptibly(future);
        } catch (java.util.concurrent.ExecutionException e) {
          if (e.getCause() instanceof ExecutionException) {
            if (!stopOnError) {
              exception = (ExecutionException) e.getCause();
            } else {
              throw (ExecutionException) e.getCause();
            }
          } else if (e.getCause() instanceof ValidationConflictException) {
            if (!stopOnError) {
              exception = (ValidationConflictException) e.getCause();
            } else {
              throw (ValidationConflictException) e.getCause();
            }
          } else if (e.getCause() instanceof CrudException) {
            if (!stopOnError) {
              exception = (CrudException) e.getCause();
            } else {
              throw (CrudException) e.getCause();
            }
          } else if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          } else if (e.getCause() instanceof Error) {
            throw (Error) e.getCause();
          } else {
            throw new AssertionError("Can't reach here. Maybe a bug", e);
          }
        }
      }

      if (!stopOnError && exception != null) {
        if (exception instanceof ExecutionException) {
          throw (ExecutionException) exception;
        } else if (exception instanceof ValidationConflictException) {
          throw (ValidationConflictException) exception;
        } else {
          throw (CrudException) exception;
        }
      }
    }
  }

  private void executeTasksSerially(
      List<ParallelExecutorTask> tasks, boolean stopOnError, String taskName, String transactionId)
      throws ExecutionException, ValidationConflictException, CrudException {
    Exception exception = null;
    for (ParallelExecutorTask task : tasks) {
      try {
        task.run();
      } catch (ExecutionException | ValidationConflictException | CrudException e) {
        logger.warn("Failed to run a {} task. Transaction ID: {}", taskName, transactionId, e);

        if (!stopOnError) {
          exception = e;
        } else {
          throw e;
        }
      }
    }

    if (!stopOnError && exception != null) {
      if (exception instanceof ExecutionException) {
        throw (ExecutionException) exception;
      } else if (exception instanceof ValidationConflictException) {
        throw (ValidationConflictException) exception;
      } else {
        throw (CrudException) exception;
      }
    }
  }

  public void close() {
    if (parallelExecutorService != null) {
      parallelExecutorService.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(parallelExecutorService);
    }
  }
}
