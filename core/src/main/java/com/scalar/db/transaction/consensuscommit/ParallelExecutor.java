package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.util.ScalarDbUtils;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
  @Nullable private ExecutorService parallelExecutorService;
  private final AtomicReference<Instant> lastParallelTaskTime = new AtomicReference<>(Instant.now());

  public ParallelExecutor(ConsensusCommitConfig config) {
    this.config = config;
    setupOrReuseParallelExecutionService(config);
    startIdleThreadAutoTerminator(config);
  }

  private void startIdleThreadAutoTerminator(ConsensusCommitConfig config) {
    if (!config.isIdleThreadAutoTerminationEnabled()) {
      return;
    }
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("idle-thread-auto-terminator-%d").build())
        .scheduleAtFixedRate(() -> {
          // TODO: Make the timeout and interval configurable
          Instant thresholdOfIdleTimeout = Instant.now().minusSeconds(30);
          if (lastParallelTaskTime.get().isBefore(thresholdOfIdleTimeout)) {
            teardownParallelExecutionService();
          }
        }, 10, 10, TimeUnit.SECONDS);
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
    } catch (CommitConflictException ignored) {
      // tasks for preparation should not throw CommitConflictException
    }
  }

  public void validate(List<ParallelExecutorTask> tasks, String transactionId)
      throws ExecutionException, CommitConflictException {
    executeTasks(
        tasks, config.isParallelValidationEnabled(), false, true, "validation", transactionId);
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
    } catch (CommitConflictException ignored) {
      // tasks for commit should not throw CommitConflictException
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
    } catch (CommitConflictException ignored) {
      // tasks for rollback should not throw CommitConflictException
    }
  }

  private void executeTasks(
      List<ParallelExecutorTask> tasks,
      boolean parallel,
      boolean noWait,
      boolean stopOnError,
      String taskName,
      String transactionId)
      throws ExecutionException, CommitConflictException {
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
      throws ExecutionException, CommitConflictException {
    lastParallelTaskTime.set(Instant.now());

    // This synchronization is needed for the following case without the synchronization
    // - main thread gets `this.parallelExecutorService` from setupOrReuseParallelExecutionService()
    // - monitor thread detects the idle timeout of the execution thread and shutdown the existing
    //   `this.parallelExecutorService`
    // - main thread submits tasks to the shutdown (old) `this.parallelExecutorService`. But the
    //   operation is rejected as it's already shutdown
    CompletionService<Void> completionService;
    synchronized (this) {
      ExecutorService executorService = setupOrReuseParallelExecutionService(config);

      completionService = new ExecutorCompletionService<>(executorService);
      tasks.forEach(
          t ->
              completionService.submit(
                  () -> {
                    try {
                      t.run();
                    } catch (Exception e) {
                      logger.warn(
                          "failed to run a {} task. transaction ID: {}", taskName, transactionId, e);
                      throw e;
                    }
                    return null;
                  }));
    }

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
      List<ParallelExecutorTask> tasks, boolean stopOnError, String taskName, String transactionId)
      throws ExecutionException, CommitConflictException {
    Exception exception = null;
    for (ParallelExecutorTask task : tasks) {
      try {
        task.run();
      } catch (ExecutionException | CommitConflictException e) {
        logger.warn("failed to run a {} task. transactionId: {}", taskName, transactionId, e);

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

  private synchronized ExecutorService setupOrReuseParallelExecutionService(ConsensusCommitConfig config) {
    if (config.isParallelPreparationEnabled()
        || config.isParallelValidationEnabled()
        || config.isParallelCommitEnabled()
        || config.isParallelRollbackEnabled()) {
      if (parallelExecutorService == null) {
        ExecutorService executorService = Executors.newFixedThreadPool(
            config.getParallelExecutorCount(),
            new ThreadFactoryBuilder().setNameFormat("parallel-executor-%d").build());
        parallelExecutorService = executorService;
        logger.info("Setup parallel-execution thread {}", executorService);
      }
      return parallelExecutorService;
    }
    return null;
  }

  private synchronized ExecutorService teardownParallelExecutionService() {
    if (parallelExecutorService != null) {
      logger.info("Shutting down parallel-execution thread {}", parallelExecutorService);
      parallelExecutorService.shutdown();
      ExecutorService shutdownParallelExecutorService = parallelExecutorService;
      parallelExecutorService = null;
      return shutdownParallelExecutorService;
    }
    return null;
  }

  public void close() {
    ExecutorService shutdownExecutorService = teardownParallelExecutionService();
    if (shutdownExecutorService != null) {
      Uninterruptibles.awaitTerminationUninterruptibly(shutdownExecutorService);
    }
  }
}
