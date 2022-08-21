package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class CassandraFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "cassandra";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new Cassandra(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new CassandraAdmin(config);
  }
}
