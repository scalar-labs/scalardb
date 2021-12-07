package com.scalar.db.util;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class ScalarDbUtils {

  public static void setTargetToIfNot(
      List<? extends Operation> operations,
      Optional<String> namespace,
      Optional<String> tableName) {
    operations.forEach(o -> setTargetToIfNot(o, namespace, tableName));
  }

  public static void setTargetToIfNot(
      Operation operation, Optional<String> namespace, Optional<String> tableName) {
    if (!operation.forNamespace().isPresent()) {
      operation.forNamespace(namespace.orElse(null));
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
  }

  public static boolean isSecondaryIndexSpecified(Operation operation, TableMetadata metadata) {
    List<Value<?>> keyValues = operation.getPartitionKey().get();
    if (keyValues.size() == 1) {
      String name = keyValues.get(0).getName();
      return metadata.getSecondaryIndexNames().contains(name);
    }

    return false;
  }

  public static void addProjectionsForKeys(Selection selection, TableMetadata metadata) {
    if (selection.getProjections().isEmpty()) { // meaning projecting all
      return;
    }
    Streams.concat(
            metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
        .filter(n -> !selection.getProjections().contains(n))
        .forEach(selection::withProjection);
  }

  public static <E> E pollUninterruptibly(BlockingQueue<E> queue, long timeout, TimeUnit unit) {
    boolean interrupted = false;
    try {
      long remainingNanos = unit.toNanos(timeout);
      long end = System.nanoTime() + remainingNanos;

      while (true) {
        try {
          return queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
          remainingNanos = end - System.nanoTime();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Return a fully qualified table name
   *
   * @param namespace a namespace
   * @param table a table
   * @return a fully qualified table name
   */
  public static String getFullTableName(String namespace, String table) {
    return namespace + "." + table;
  }

  public static void executeTasks(
      List<ThrowableRunnable<ExecutionException>> tasks,
      @Nullable ExecutorService executorService,
      boolean parallel,
      boolean noWait)
      throws ExecutionException {

    List<Future<?>> futures;
    if (parallel) {
      assert executorService != null;
      futures =
          tasks.stream()
              .map(
                  t ->
                      executorService.submit(
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
}
