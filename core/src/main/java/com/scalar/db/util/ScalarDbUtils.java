package com.scalar.db.util;

import com.google.common.collect.Streams;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Value;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class ScalarDbUtils {

  private ScalarDbUtils() {}

  @SuppressWarnings("unchecked")
  public static <T extends Mutation> List<T> copyAndSetTargetToIfNot(
      List<T> mutations, Optional<String> namespace, Optional<String> tableName) {
    return mutations.stream()
        .map(
            m -> {
              if (m instanceof Put) {
                return (T) copyAndSetTargetToIfNot((Put) m, namespace, tableName);
              } else { // Delete
                return (T) copyAndSetTargetToIfNot((Delete) m, namespace, tableName);
              }
            })
        .collect(Collectors.toList());
  }

  public static Get copyAndSetTargetToIfNot(
      Get get, Optional<String> namespace, Optional<String> tableName) {
    Get ret = new Get(get); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Scan copyAndSetTargetToIfNot(
      Scan scan, Optional<String> namespace, Optional<String> tableName) {
    Scan ret = new Scan(scan); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Mutation copyAndSetTargetToIfNot(
      Mutation mutation, Optional<String> namespace, Optional<String> tableName) {
    if (mutation instanceof Put) {
      return copyAndSetTargetToIfNot((Put) mutation, namespace, tableName);
    } else { // Delete
      return copyAndSetTargetToIfNot((Delete) mutation, namespace, tableName);
    }
  }

  public static Put copyAndSetTargetToIfNot(
      Put put, Optional<String> namespace, Optional<String> tableName) {
    Put ret = new Put(put); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  public static Delete copyAndSetTargetToIfNot(
      Delete delete, Optional<String> namespace, Optional<String> tableName) {
    Delete ret = new Delete(delete); // copy
    setTargetToIfNot(ret, namespace, tableName);
    return ret;
  }

  private static void setTargetToIfNot(
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
}
