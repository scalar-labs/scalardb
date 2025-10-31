package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CommonDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public interface ObjectStorageProvider extends DistributedStorageProvider {

  @Override
  default DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return null;
  }

  @Override
  default DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new CommonDistributedStorageAdmin(new ObjectStorageAdmin(config), config);
  }
}
