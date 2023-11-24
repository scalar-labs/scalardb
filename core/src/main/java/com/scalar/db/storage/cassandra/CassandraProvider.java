package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class CassandraProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return CassandraConfig.STORAGE_NAME;
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new Cassandra(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new CheckedDistributedStorageAdmin(new CassandraAdmin(config));
  }
}
