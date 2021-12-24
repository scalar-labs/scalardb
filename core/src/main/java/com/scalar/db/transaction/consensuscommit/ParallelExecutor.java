package com.scalar.db.transaction.consensuscommit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ThrowableRunnable;
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

  private final ConsensusCommitConfig config;
  @Nullable private final ExecutorService parallelExecutorService;

  public ParallelExecutor(ConsensusCommitConfig config) {
    this.config = config;
    if (config.isParallelPreparationEnabled()
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

  public void prepare(List<ThrowableRunnable<ExecutionException>> tasks) throws ExecutionException {
    executeTasks(tasks, config.isParallelPreparationEnabled(), false);
  }

  public void commit(List<ThrowableRunnable<ExecutionException>> tasks) throws ExecutionException {
    executeTasks(tasks, config.isParallelCommitEnabled(), false);
  }

  public void rollback(List<ThrowableRunnable<ExecutionException>> tasks)
      throws ExecutionException {
    executeTasks(tasks, config.isParallelRollbackEnabled(), false);
  }

  private void executeTasks(
      List<ThrowableRunnable<ExecutionException>> tasks, boolean parallel, boolean noWait)
      throws ExecutionException {

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
      for (ThrowableRunnable<ExecutionException> runnable : tasks) {
        runnable.run();
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
          if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          if (e.getCause() instanceof Error) {
            throw (Error) e.getCause();
          }
          throw new ExecutionException("an error occurred during executing the tasks", e);
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
