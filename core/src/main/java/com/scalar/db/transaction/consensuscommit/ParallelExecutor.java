package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
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
    void run() throws ExecutionException, CommitConflictException;
  }

  private final ConsensusCommitConfig config;
  @Nullable private final ExecutorService parallelExecutorService;

  public ParallelExecutor(ConsensusCommitConfig config) {
    this.config = config;
    if (config.isParallelPreparationEnabled()
        || config.isParallelValidationEnabled()
        || config.isParallelCommitEnabled()
        || config.isParallelRollbackEnabled()) {
      parallelExecutorService =
          Executors.newFixedThreadPool(
              config.getParallelExecutorCount(),
              new ThreadFactoryBuilder().setNameFormat("parallel-executor-%d").build());
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

  public void prepare(List<ParallelExecutorTask> tasks) throws ExecutionException {
    try {
      executeTasks("preparation", tasks, config.isParallelPreparationEnabled(), false, true);
    } catch (CommitConflictException ignored) {
      // tasks for preparation should not throw CommitConflictException
    }
  }

  public void validate(List<ParallelExecutorTask> tasks)
      throws ExecutionException, CommitConflictException {
    executeTasks("validation", tasks, config.isParallelValidationEnabled(), false, true);
  }

  public void commitRecords(List<ParallelExecutorTask> tasks) throws ExecutionException {
    try {
      executeTasks(
          "commitRecords",
          tasks,
          config.isParallelCommitEnabled(),
          config.isAsyncCommitEnabled(),
          false);
    } catch (CommitConflictException ignored) {
      // tasks for commit should not throw CommitConflictException
    }
  }

  public void rollbackRecords(List<ParallelExecutorTask> tasks) throws ExecutionException {
    try {
      executeTasks(
          "rollbackRecords",
          tasks,
          config.isParallelRollbackEnabled(),
          config.isAsyncRollbackEnabled(),
          false);
    } catch (CommitConflictException ignored) {
      // tasks for rollback should not throw CommitConflictException
    }
  }

  private void executeTasks(
      String name,
      List<ParallelExecutorTask> tasks,
      boolean parallel,
      boolean noWait,
      boolean stopOnError)
      throws ExecutionException, CommitConflictException {
    if (parallel) {
      executeTasksInParallel(name, tasks, noWait, stopOnError);
    } else {
      executeTasksSerially(name, tasks, stopOnError);
    }
  }

  private void executeTasksInParallel(
      String name, List<ParallelExecutorTask> tasks, boolean noWait, boolean stopOnError)
      throws ExecutionException, CommitConflictException {
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
                    logger.warn("failed to run a {} task", name, e);
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
          } else if (e.getCause() instanceof CommitConflictException) {
            if (!stopOnError) {
              exception = (CommitConflictException) e.getCause();
            } else {
              throw (CommitConflictException) e.getCause();
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
        } else {
          throw (CommitConflictException) exception;
        }
      }
    }
  }

  private void executeTasksSerially(
      String name, List<ParallelExecutorTask> tasks, boolean stopOnError)
      throws ExecutionException, CommitConflictException {
    Exception exception = null;
    for (ParallelExecutorTask task : tasks) {
      try {
        task.run();
      } catch (ExecutionException | CommitConflictException e) {
        logger.warn("failed to run a {} task", name, e);

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
      } else {
        throw (CommitConflictException) exception;
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
