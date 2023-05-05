package com.scalar.db.storage.redis;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.config.DatabaseConfig;

public class RedisProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return "redis";
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new Redis(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new RedisAdmin(config);
  }
}
