package com.scalar.db.storage.multistorage;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.service.StorageModule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class MultiStorageAdmin implements DistributedStorageAdmin {

  private final Map<String, DistributedStorageAdmin> tableAdminMap;
  private final Map<String, DistributedStorageAdmin> namespaceAdminMap;
  private final DistributedStorageAdmin defaultAdmin;
  private final List<DistributedStorageAdmin> admins;

  @Inject
  public MultiStorageAdmin(MultiStorageConfig config) {
    admins = new ArrayList<>();
    Map<String, DistributedStorageAdmin> nameAdminMap = new HashMap<>();
    config
        .getDatabaseConfigMap()
        .forEach(
            (storageName, databaseConfig) -> {
              // Instantiate admins with Guice
              Injector injector = Guice.createInjector(new StorageModule(databaseConfig));
              DistributedStorageAdmin admin = injector.getInstance(DistributedStorageAdmin.class);
              nameAdminMap.put(storageName, admin);
              admins.add(admin);
            });

    tableAdminMap = new HashMap<>();
    config
        .getTableStorageMap()
        .forEach((table, storageName) -> tableAdminMap.put(table, nameAdminMap.get(storageName)));

    namespaceAdminMap = new HashMap<>();
    config
        .getNamespaceStorageMap()
        .forEach(
            (table, storageName) -> namespaceAdminMap.put(table, nameAdminMap.get(storageName)));

    defaultAdmin = nameAdminMap.get(config.getDefaultStorage());
  }

  @VisibleForTesting
  MultiStorageAdmin(
      Map<String, DistributedStorageAdmin> tableAdminMap,
      Map<String, DistributedStorageAdmin> namespaceAdminMap,
      DistributedStorageAdmin defaultAdmin) {
    this.tableAdminMap = tableAdminMap;
    this.namespaceAdminMap = namespaceAdminMap;
    this.defaultAdmin = defaultAdmin;
    admins = null;
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
    DistributedStorageAdmin admin = tableAdminMap.get(fullTaleName);
    if (admin != null) {
      return admin;
    }
    admin = namespaceAdminMap.get(namespace);
    return admin != null ? admin : defaultAdmin;
  }

  @Override
  public void close() {
    for (DistributedStorageAdmin admin : admins) {
      admin.close();
    }
  }
}
