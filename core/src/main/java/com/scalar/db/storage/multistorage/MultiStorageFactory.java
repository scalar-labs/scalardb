package com.scalar.db.storage.multistorage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class MultiStorageFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "multi-storage";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new MultiStorage(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new MultiStorageAdmin(config);
  }
}
