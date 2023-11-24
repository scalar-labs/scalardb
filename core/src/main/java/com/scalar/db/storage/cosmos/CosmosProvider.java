package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class CosmosProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return CosmosConfig.STORAGE_NAME;
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new Cosmos(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new CheckedDistributedStorageAdmin(new CosmosAdmin(config));
  }
}
