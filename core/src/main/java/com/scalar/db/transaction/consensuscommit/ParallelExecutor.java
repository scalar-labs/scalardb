package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ParallelExecutor {

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
      executeTasks(tasks, config.isParallelPreparationEnabled(), false);
    } catch (CommitConflictException ignored) {
      // tasks for preparation should not throw CommitConflictException
    }
  }

  public void validate(List<ParallelExecutorTask> tasks)
      throws ExecutionException, CommitConflictException {
    executeTasks(tasks, config.isParallelValidationEnabled(), false);
  }

  public void commit(List<ParallelExecutorTask> tasks) throws ExecutionException {
    try {
      executeTasks(tasks, config.isParallelCommitEnabled(), config.isAsyncCommitEnabled());
    } catch (CommitConflictException ignored) {
      // tasks for commit should not throw CommitConflictException
    }
  }

  public void rollback(List<ParallelExecutorTask> tasks) throws ExecutionException {
    try {
      executeTasks(tasks, config.isParallelRollbackEnabled(), config.isAsyncRollbackEnabled());
    } catch (CommitConflictException ignored) {
      // tasks for rollback should not throw CommitConflictException
    }
  }

  private void executeTasks(List<ParallelExecutorTask> tasks, boolean parallel, boolean noWait)
      throws ExecutionException, CommitConflictException {
    List<Future<?>> futures;
    if (parallel) {
      assert parallelExecutorService != null;
      futures =
          tasks.stream()
              .map(
                  t ->
                      parallelExecutorService.submit(
                          () -> {
                            t.run();
                            return null;
                          }))
              .collect(Collectors.toList());
    } else {
      futures = Collections.emptyList();
      for (ParallelExecutorTask task : tasks) {
        task.run();
      }
    }

    if (!noWait) {
      for (Future<?> future : futures) {
        try {
          Uninterruptibles.getUninterruptibly(future);
        } catch (java.util.concurrent.ExecutionException e) {
          if (e.getCause() instanceof ExecutionException) {
            throw (ExecutionException) e.getCause();
          }
          if (e.getCause() instanceof CommitConflictException) {
            throw (CommitConflictException) e.getCause();
          }
          if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          if (e.getCause() instanceof Error) {
            throw (Error) e.getCause();
          }
          throw new AssertionError("Can't reach here. Maybe a bug", e);
        }
      }
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  public void close() {
    if (parallelExecutorService != null) {
      parallelExecutorService.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(parallelExecutorService);
    }
  }
}
