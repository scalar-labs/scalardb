package com.scalar.db.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Operation;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** A class that manages and caches virtual table information */
@ThreadSafe
public class VirtualTableInfoManager {

  private final LoadingCache<TableKey, Optional<VirtualTableInfo>> virtualTableInfoCache;

  public VirtualTableInfoManager(DistributedStorageAdmin admin, long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs >= 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    virtualTableInfoCache =
        builder.build(
            new CacheLoader<TableKey, Optional<VirtualTableInfo>>() {
              @Nonnull
              @Override
              public Optional<VirtualTableInfo> load(@Nonnull TableKey key) throws Exception {
                return admin.getVirtualTableInfo(key.namespace, key.table);
              }
            });
  }

  /**
   * Returns virtual table information corresponding to the specified operation.
   *
   * @param operation an operation
   * @return the virtual table information or null if the table is not a virtual table
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if the table does not exist
   */
  @Nullable
  public VirtualTableInfo getVirtualTableInfo(Operation operation) throws ExecutionException {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_DOES_NOT_HAVE_TARGET_NAMESPACE_OR_TABLE_NAME.buildMessage(operation));
    }
    return getVirtualTableInfo(operation.forNamespace().get(), operation.forTable().get());
  }

  /**
   * Returns virtual table information corresponding to the specified namespace and table.
   *
   * @param namespace a namespace to retrieve
   * @param table a table to retrieve
   * @return the virtual table information or null if the table is not a virtual table
   * @throws ExecutionException if the operation fails
   * @throws IllegalArgumentException if the table does not exist
   */
  @Nullable
  public VirtualTableInfo getVirtualTableInfo(String namespace, String table)
      throws ExecutionException {
    try {
      TableKey key = new TableKey(namespace, table);
      return virtualTableInfoCache.get(key).orElse(null);
    } catch (java.util.concurrent.ExecutionException
        | com.google.common.util.concurrent.UncheckedExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }

      throw new ExecutionException(
          CoreError.GETTING_VIRTUAL_TABLE_INFO_FAILED.buildMessage(
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
