package com.scalar.db.storage.rpc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageFactory;
import com.scalar.db.config.DatabaseConfig;

public class GrpcFactory implements DistributedStorageFactory {
  @Override
  public String getName() {
    return "grpc";
  }

  @Override
  public DistributedStorage getDistributedStorage(DatabaseConfig config) {
    return new GrpcStorage(config);
  }

  @Override
  public DistributedStorageAdmin getDistributedStorageAdmin(DatabaseConfig config) {
    return new GrpcAdmin(config);
  }
}
