package com.scalar.db.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/** A class that manages and caches table metadata */
@ThreadSafe
public class TableMetadataManager {

  private final LoadingCache<TableKey, Optional<TableMetadata>> tableMetadataCache;

  public TableMetadataManager(DistributedStorageAdmin admin, long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs > 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    tableMetadataCache =
        builder.build(
            new CacheLoader<TableKey, Optional<TableMetadata>>() {
              @Override
              public Optional<TableMetadata> load(@Nonnull TableKey key) throws ExecutionException {
                return Optional.ofNullable(admin.getTableMetadata(key.namespace, key.table));
              }
            });
  }

  /**
   * Returns a table metadata corresponding to the specified operation.
   *
   * @param operation an operation
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation failed
   */
  public TableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    try {
      TableKey key = new TableKey(operation.forNamespace().get(), operation.forTable().get());
      return tableMetadataCache.get(key).orElse(null);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private static class TableKey {
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
