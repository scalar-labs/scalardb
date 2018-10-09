package com.scalar.database.storage.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A cache for statements. The cache will try to evict entries that haven't been used recently when
 * it reaches or is approaching the size limit.
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class StatementCache {
  private static final int DEFAULT_CACHE_SIZE = 128;
  private Cache<String, PreparedStatement> cache;

  /** Constructs a cache with default cache size (= 128). */
  public StatementCache() {
    this(DEFAULT_CACHE_SIZE);
  }

  public StatementCache(int maxSize) {
    cache = CacheBuilder.newBuilder().maximumSize(maxSize).build();
  }

  public PreparedStatement get(String cacheKey) {
    return cache.getIfPresent(cacheKey);
  }

  public void put(String cacheKey, PreparedStatement statement) {
    cache.put(cacheKey, statement);
  }
}
