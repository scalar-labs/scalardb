package com.scalar.db.sql.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CachedMetadata implements Metadata {

  private final LoadingCache<String, Optional<NamespaceMetadata>> cache;

  private CachedMetadata(DistributedTransactionAdmin admin, long cacheExpirationTimeSecs) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
    if (cacheExpirationTimeSecs > 0) {
      builder.expireAfterWrite(cacheExpirationTimeSecs, TimeUnit.SECONDS);
    }
    cache =
        builder.build(
            new CacheLoader<String, Optional<NamespaceMetadata>>() {
              @Override
              public Optional<NamespaceMetadata> load(@Nonnull String namespaceName) {
                try {
                  if (admin.namespaceExists(namespaceName)) {
                    return Optional.of(
                        new CachedNamespaceMetadata(namespaceName, admin, cacheExpirationTimeSecs));
                  }
                } catch (ExecutionException e) {
                  throw new SqlException("Failed to check the namespace existence", e);
                }
                return Optional.empty();
              }
            });
  }

  @Override
  public Optional<NamespaceMetadata> getNamespace(String namespaceName) {
    try {
      return cache.get(namespaceName);
    } catch (java.util.concurrent.ExecutionException e) {
      throw (RuntimeException) e.getCause();
    }
  }

  public void invalidateCache(String namespaceName) {
    cache.invalidate(namespaceName);
  }

  public static Metadata create(DistributedTransactionAdmin admin, long cacheExpirationTimeSecs) {
    return new CachedMetadata(admin, cacheExpirationTimeSecs);
  }
}
