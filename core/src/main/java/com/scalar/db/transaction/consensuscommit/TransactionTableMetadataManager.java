package com.scalar.db.transaction.consensuscommit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TransactionTableMetadataManager {

  private final LoadingCache<TableKey, Optional<TransactionTableMetadata>> tableMetadataCache;

  public TransactionTableMetadataManager(
      DistributedStorageAdmin admin, long cacheExpirationTimeSecs) {

    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs >= 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    tableMetadataCache =
        builder.build(
            new CacheLoader<TableKey, Optional<TransactionTableMetadata>>() {
              @Nonnull
              @Override
              public Optional<TransactionTableMetadata> load(@Nonnull TableKey key)
                  throws ExecutionException {
                TableMetadata tableMetadata = admin.getTableMetadata(key.namespace, key.table);
                if (tableMetadata == null) {
                  return Optional.empty();
                }
                return Optional.of(new TransactionTableMetadata(tableMetadata));
              }
            });
  }

  /**
   * Returns a transaction table metadata corresponding to the specified operation.
   *
   * @param operation an operation
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  public TransactionTableMetadata getTransactionTableMetadata(Operation operation)
      throws ExecutionException {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_DOES_NOT_HAVE_TARGET_NAMESPACE_OR_TABLE_NAME.buildMessage());
    }
    return getTransactionTableMetadata(operation.forNamespace().get(), operation.forTable().get());
  }

  /**
   * Returns a transaction table metadata corresponding to the specified namespace and table.
   *
   * @param namespace a namespace
   * @param table a table
   * @return a table metadata. null if the table is not found.
   * @throws ExecutionException if the operation fails
   */
  public TransactionTableMetadata getTransactionTableMetadata(String namespace, String table)
      throws ExecutionException {
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
