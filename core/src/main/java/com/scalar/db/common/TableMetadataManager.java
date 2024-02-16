package com.scalar.db.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Admin;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.ThrowableFunction;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/** A class that manages and caches table metadata */
@ThreadSafe
public class TableMetadataManager {

  private final LoadingCache<TableKey, Optional<TableMetadata>> tableMetadataCache;

  public TableMetadataManager(Admin admin, long cacheExpirationTimeSecs) {
    this(
        key -> Optional.ofNullable(admin.getTableMetadata(key.namespace, key.table)),
        cacheExpirationTimeSecs);
  }

  public TableMetadataManager(
      ThrowableFunction<TableKey, Optional<TableMetadata>, Exception> getTableMetadataFunc,
      long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs >= 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    tableMetadataCache =
        builder.build(
            new CacheLoader<TableKey, Optional<TableMetadata>>() {
              @Nonnull
              @Override
              public Optional<TableMetadata> load(@Nonnull TableKey key) throws Exception {
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
   * Returns a table metadata corresponding to the specified operation.
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      TableKey key = new TableKey(namespace, table);
      return tableMetadataCache.get(key).orElse(null);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  public static class TableKey {
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
