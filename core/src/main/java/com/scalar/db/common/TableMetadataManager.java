package com.scalar.db.common;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Admin;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.ThrowableFunction;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** A class that manages and caches table metadata */
@ThreadSafe
public class TableMetadataManager {

  private final LoadingCache<TableKey, TableMetadata> tableMetadataCache;

  public TableMetadataManager(Admin admin, long cacheExpirationTimeSecs) {
    this(key -> admin.getTableMetadata(key.namespace, key.table), cacheExpirationTimeSecs);
  }

  public TableMetadataManager(
      ThrowableFunction<TableKey, TableMetadata, Exception> getTableMetadataFunc,
      long cacheExpirationTimeSecs) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    if (cacheExpirationTimeSecs >= 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    tableMetadataCache =
        builder.build(
            new CacheLoader<TableKey, TableMetadata>() {
              @Nullable
              @Override
              public TableMetadata load(@Nonnull TableKey key) throws Exception {
                return getTableMetadataFunc.apply(key);
              }
            });
  }

  /**
   * Returns a table metadata corresponding to the specified operation.
   *
   * @param operation an operation
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  public TableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_DOES_NOT_HAVE_TARGET_NAMESPACE_OR_TABLE_NAME.buildMessage(operation));
    }
    return getTableMetadata(operation.forNamespace().get(), operation.forTable().get());
  }

  /**
   * Returns a table metadata corresponding to the specified namespace and table.
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  @Nullable
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      TableKey key = new TableKey(namespace, table);
      return tableMetadataCache.get(key);
    } catch (CompletionException e) {
      throw new ExecutionException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e.getCause());
    }
  }

  @VisibleForTesting
  static class TableKey {
    public final String namespace;
    public final String table;

    public TableKey(String namespace, String table) {
      this.namespace = namespace;
      this.table = table;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableKey)) {
        return false;
      }
      TableKey tableKey = (TableKey) o;
      return namespace.equals(tableKey.namespace) && table.equals(tableKey.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, table);
    }
  }
}
