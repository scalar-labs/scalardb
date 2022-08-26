package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class CosmosFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "cosmos";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new Cosmos(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new CosmosAdmin(config);
  }
}
