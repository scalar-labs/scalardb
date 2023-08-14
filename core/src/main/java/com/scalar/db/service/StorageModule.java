package com.scalar.db.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageAdmin;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcStorage;

/** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
@Deprecated
public class StorageModule extends AbstractModule {

  private final DatabaseConfig config;

  public StorageModule(DatabaseConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    Class<? extends DistributedStorage> storageClass;
    Class<? extends DistributedStorageAdmin> storageAdminClass;
    switch (config.getStorage().toLowerCase()) {
      case "cassandra":
        storageClass = Cassandra.class;
        storageAdminClass = CassandraAdmin.class;
        break;
      case "cosmos":
        storageClass = Cosmos.class;
        storageAdminClass = CosmosAdmin.class;
        break;
      case "dynamo":
        storageClass = Dynamo.class;
        storageAdminClass = DynamoAdmin.class;
        break;
      case "jdbc":
        storageClass = JdbcDatabase.class;
        storageAdminClass = JdbcAdmin.class;
        break;
      case "multi-storage":
        storageClass = MultiStorage.class;
        storageAdminClass = MultiStorageAdmin.class;
        break;
      case "grpc":
        storageClass = GrpcStorage.class;
        storageAdminClass = GrpcAdmin.class;
        break;
      default:
        throw new IllegalArgumentException("Storage '" + config.getStorage() + "' isn't supported");
    }

    bind(DistributedStorage.class).to(storageClass);
    bind(DistributedStorageAdmin.class).to(storageAdminClass);
  }

  @Singleton
  @Provides
  DatabaseConfig provideDatabaseConfig() {
    return config;
  }
}
