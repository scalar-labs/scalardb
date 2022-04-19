package com.scalar.db.sql.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CachedNamespaceMetadata implements NamespaceMetadata {

  private final String name;

  private final LoadingCache<String, ImmutableList<String>> tableNamesCache;
  private final LoadingCache<String, Optional<TableMetadata>> tableMetadataCache;

  CachedNamespaceMetadata(
      String name, DistributedTransactionAdmin admin, long cacheExpirationTimeSecs) {
    this.name = Objects.requireNonNull(name);

    tableNamesCache = createTableNamesCache(admin, cacheExpirationTimeSecs);
    tableMetadataCache = createTableMetadataCache(admin, cacheExpirationTimeSecs);
  }

  private LoadingCache<String, ImmutableList<String>> createTableNamesCache(
      DistributedTransactionAdmin admin, long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs > 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    return builder.build(
        new CacheLoader<String, ImmutableList<String>>() {
          @Override
          public ImmutableList<String> load(@Nonnull String namespaceName) {
            try {
              return ImmutableList.copyOf(
                  new ArrayList<>(admin.getNamespaceTableNames(namespaceName)));
            } catch (ExecutionException e) {
              throw new SqlException("Failed to check the namespace existence", e);
            }
          }
        });
  }

  private LoadingCache<String, Optional<TableMetadata>> createTableMetadataCache(
      DistributedTransactionAdmin admin, long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs > 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    return builder.build(
        new CacheLoader<String, Optional<TableMetadata>>() {
          @Override
          public Optional<TableMetadata> load(@Nonnull String tableName) {
            try {
              com.scalar.db.api.TableMetadata tableMetadata =
                  admin.getTableMetadata(name, tableName);
              if (tableMetadata == null) {
                return Optional.empty();
              }
              return Optional.of(new TableMetadataImpl(name, tableName, tableMetadata));
            } catch (ExecutionException e) {
              throw new SqlException("Failed to get a table metadata", e);
            }
          }
        });
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, TableMetadata> getTables() {
    try {
      return tableNamesCache.get(name).stream()
          .map(
              t -> {
                try {
                  return tableMetadataCache.get(t);
                } catch (java.util.concurrent.ExecutionException e) {
                  throw (RuntimeException) e.getCause();
                }
              })
          .filter(Optional::isPresent)
          .collect(Collectors.toMap(t -> t.get().getName(), Optional::get));
    } catch (java.util.concurrent.ExecutionException e) {
      throw (RuntimeException) e.getCause();
    }
  }

  @Override
  public Optional<TableMetadata> getTable(String tableName) {
    try {
      return tableMetadataCache.get(tableName);
    } catch (java.util.concurrent.ExecutionException e) {
      throw (RuntimeException) e.getCause();
    }
  }

  public void invalidateTableNamesCache() {
    tableNamesCache.invalidate(name);
  }

  public void invalidateTableMetadataCache(String tableName) {
    tableMetadataCache.invalidate(tableName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CachedNamespaceMetadata)) {
      return false;
    }
    CachedNamespaceMetadata that = (CachedNamespaceMetadata) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
