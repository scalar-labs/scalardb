package com.scalar.db.storage.multistorage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.service.StorageModule;
import java.util.HashMap;
import java.util.Map;

public class MultiStorageAdmin implements DistributedStorageAdmin {

  private final Map<String, DistributedStorageAdmin> adminMap;
  private final DistributedStorageAdmin defaultAdmin;

  @Inject
  public MultiStorageAdmin(MultiStorageConfig config) {
    Map<String, DistributedStorageAdmin> nameAdminMap = new HashMap<>();
    config
        .getDatabaseConfigMap()
        .forEach(
            (storage, databaseConfig) -> {
              // Instantiate admins with Guice
              Injector injector = Guice.createInjector(new StorageModule(databaseConfig));
              nameAdminMap.put(storage, injector.getInstance(DistributedStorageAdmin.class));
            });

    Builder<String, DistributedStorageAdmin> builder = ImmutableMap.builder();
    config
        .getTableStorageMap()
        .forEach((table, storage) -> builder.put(table, nameAdminMap.get(storage)));
    adminMap = builder.build();

    defaultAdmin = nameAdminMap.get(config.getDefaultStorage());
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropTable(String namespace, String table) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    return getAdmin(namespace, table).getTableMetadata(namespace, table);
  }

  private DistributedStorageAdmin getAdmin(String namespace, String table) {
    String fullTaleName = namespace + "." + table;
    DistributedStorageAdmin admin = adminMap.get(fullTaleName);
    if (admin == null) {
      return defaultAdmin;
    }
    return admin;
  }

  @Override
  public void close() {
    for (DistributedStorageAdmin admin : adminMap.values()) {
      admin.close();
    }
  }
}
