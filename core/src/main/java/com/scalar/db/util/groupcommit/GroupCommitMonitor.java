package com.scalar.db.util.groupcommit;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupCommitMonitor implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitMonitor.class);
  private final ExecutorService executorService;

  GroupCommitMonitor(String label, Supplier<GroupCommitMetrics> metricsSupplier) {
    executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-monitor-%d")
                .build());
    startExecutorService(metricsSupplier);
  }

  private void startExecutorService(Supplier<GroupCommitMetrics> metricsSupplier) {
    Runnable print =
        () ->
            logger.info(
                "Timestamp={}, GroupCommitMetrics={}", Instant.now(), metricsSupplier.get());

    executorService.execute(
        () -> {
          while (!executorService.isShutdown()) {
            print.run();
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
          }
          print.run();
        });
  }

  @Override
  public void close() {
    MoreExecutors.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
  }
}
