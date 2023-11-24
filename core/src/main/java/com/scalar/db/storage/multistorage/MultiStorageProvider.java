package com.scalar.db.storage.multistorage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.config.DatabaseConfig;

public class MultiStorageProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return MultiStorageConfig.STORAGE_NAME;
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new MultiStorage(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new MultiStorageAdmin(config);
  }
}
