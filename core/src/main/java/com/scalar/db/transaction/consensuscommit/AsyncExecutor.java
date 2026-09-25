package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the phases of the Consensus Commit protocol that a transaction does not wait for, so that
 * the transaction returns to the caller without them.
 *
 * <p>This is the counterpart of {@link ParallelExecutor}: that one runs the tasks of a phase
 * concurrently, while this one moves a whole phase off the thread of the transaction. A phase run
 * here drives {@link ParallelExecutor} in turn, and waits for it, which is why the two use separate
 * thread pools: waiting on a thread of the pool the phase submits its tasks to could exhaust that
 * pool.
 */
@ThreadSafe
public class AsyncExecutor {
  private static final Logger logger = LoggerFactory.getLogger(AsyncExecutor.class);

  private static final long KEEP_ALIVE_TIME_SECONDS = 60;

  @FunctionalInterface
  public interface AsyncExecutorTask {
    void run();
  }

  private final ConsensusCommitConfig config;
  @Nullable private final ExecutorService asyncExecutorService;

  public AsyncExecutor(ConsensusCommitConfig config) {
    this.config = config;

    if (config.isAsyncCommitEnabled() || config.isAsyncRollbackEnabled()) {
      // The threads here drive the phases. A phase runs its storage operations on the threads of
      // ParallelExecutor when it has several of them to run in parallel, and on the thread here
      // otherwise, e.g., when all its mutations form a single group, as they always do on JDBC.
      // The number of threads here is therefore a safety cap rather than a degree of concurrency to
      // tune, and it is taken from the thread count of ParallelExecutor so that the asynchronous
      // phases do not run far more storage operations at once than that count. No separate
      // configuration is introduced for it.
      //
      // The pool holds no thread while nothing runs asynchronously, and hands a task to a thread
      // right away instead of queueing it, so a phase is never left waiting behind another one.
      asyncExecutorService =
          new ThreadPoolExecutor(
              0,
              config.getParallelExecutorCount(),
              KEEP_ALIVE_TIME_SECONDS,
              TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              // Make this thread factory create daemon threads not to block JVM termination, as
              // the thread factory of ParallelExecutor does
              new ThreadFactoryBuilder().setNameFormat("async-executor-%d").setDaemon(true).build(),
              // Run the phase on the calling thread when the cap is reached, and after this
              // executor is shut down, so that it is never dropped. The transaction then waits for
              // the phase, as it does when the phase is not asynchronous at all
              (task, executor) -> task.run());
    } else {
      asyncExecutorService = null;
    }
  }

  @VisibleForTesting
  AsyncExecutor(ConsensusCommitConfig config, @Nullable ExecutorService asyncExecutorService) {
    this.config = config;
    this.asyncExecutorService = asyncExecutorService;
  }

  /**
   * Runs the commit of the records of a transaction, on a thread of this executor when asynchronous
   * commit is enabled, and on the calling thread when it is not.
   *
   * @param task the commit to run
   * @param transactionId the ID of the transaction to commit
   */
  public void commitRecords(AsyncExecutorTask task, String transactionId) {
    if (config.isAsyncCommitEnabled()) {
      execute(task, "commitRecords", transactionId);
    } else {
      runSafely(task, "commitRecords", transactionId);
    }
  }

  /**
   * Runs the rollback of the records of a transaction, on a thread of this executor when
   * asynchronous rollback is enabled, and on the calling thread when it is not.
   *
   * @param task the rollback to run
   * @param transactionId the ID of the transaction to roll back
   */
  public void rollbackRecords(AsyncExecutorTask task, String transactionId) {
    if (config.isAsyncRollbackEnabled()) {
      execute(task, "rollbackRecords", transactionId);
    } else {
      runSafely(task, "rollbackRecords", transactionId);
    }
  }

  private void execute(AsyncExecutorTask task, String taskName, String transactionId) {
    assert asyncExecutorService != null;

    asyncExecutorService.execute(() -> runSafely(task, taskName, transactionId));
  }

  private void runSafely(AsyncExecutorTask task, String taskName, String transactionId) {
    try {
      task.run();
    } catch (Exception e) {
      // The tasks handle their own failures, so this is only a last resort that keeps one from
      // passing silently
      logger.warn("Failed to run a {} task. Transaction ID: {}", taskName, transactionId, e);
    }
  }

  public void close() {
    if (asyncExecutorService != null) {
      asyncExecutorService.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(asyncExecutorService);
    }
  }
}
