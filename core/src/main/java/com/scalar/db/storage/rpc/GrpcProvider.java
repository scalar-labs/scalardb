package com.scalar.db.storage.rpc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.config.DatabaseConfig;

public class GrpcProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return "grpc";
  }

  @Override
  public DistributedStorage createDistributedStorage(DatabaseConfig config) {
    return new GrpcStorage(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new GrpcAdmin(config);
  }
}
