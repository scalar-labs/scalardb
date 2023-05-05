package com.scalar.db.storage.redis;

import com.scalar.db.config.DatabaseConfig;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/** A simple wrapper for Jedis. It mainly glues {@link Jedis} and {@code DatabaseConfig}. */
@ThreadSafe
public class JedisClient {
  private static final int DEFAULT_REDIS_PORT = 6379;

  private final JedisPool pool;

  /**
   * Constructs a {@code RedisClient} with the specified {@code DatabaseConfig}
   *
   * @param config database configuration to create a client
   */
  public JedisClient(DatabaseConfig config) {
    pool = createPool(Objects.requireNonNull(config));
  }

  /**
   * Get a {@link Jedis} instance, which implements Closable so that it can be used in
   * try-with-resources.
   */
  public Jedis getJedis() {
    return pool.getResource();
  }

  /** Create a Redis connection pool from config */
  JedisPool createPool(DatabaseConfig config) {
    return new JedisPool(getRedisHost(config), getRedisPort(config));
  }

  /** Get a Redis host from config */
  String getRedisHost(DatabaseConfig config) {
    return config.getContactPoints().get(0);
  }

  /** Get a Redis port from config */
  int getRedisPort(DatabaseConfig config) {
    return config.getContactPort() == 0 ? DEFAULT_REDIS_PORT : config.getContactPort();
  }
}
