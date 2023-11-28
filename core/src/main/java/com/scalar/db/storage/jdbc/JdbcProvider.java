package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class JdbcProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return JdbcConfig.STORAGE_NAME;
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new JdbcDatabase(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    // If the database is SQLite, the namespace check is skipped because SQLite does not support
    // namespaces.
    boolean isSqlite = JdbcUtils.isSqlite(new JdbcConfig(config));
    return new CheckedDistributedStorageAdmin(new JdbcAdmin(config), !isSqlite);
  }
}
