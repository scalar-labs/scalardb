package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class DynamoFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "dynamo";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new Dynamo(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new DynamoAdmin(config);
  }
}
