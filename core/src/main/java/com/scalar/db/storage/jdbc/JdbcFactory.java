package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class JdbcFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "jdbc";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new JdbcDatabase(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new JdbcAdmin(config);
  }
}
