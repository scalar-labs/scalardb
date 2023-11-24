package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class DynamoProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return DynamoConfig.STORAGE_NAME;
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new Dynamo(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    // Set the namespace check to false because DynamoDB does not support namespaces.
    return new CheckedDistributedStorageAdmin(new DynamoAdmin(config));
  }
}
