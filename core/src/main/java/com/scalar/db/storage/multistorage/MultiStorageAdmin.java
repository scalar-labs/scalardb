package com.scalar.db.storage.multistorage;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation with multi-storage for {@link DistributedStorageAdmin}.
 *
 * <p>This implementation holds multiple DistributedStorageAdmin instances. It chooses an instance
 * on the basis of the specified configuration and a given operation. If there is a conflict between
 * a table mapping and a namespace mapping, it prefers the table mapping because table mappings are
 * more specific than namespace mappings.
 *
 * @author Toshihiro Suzuki
 */
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
              StorageFactory factory = new StorageFactory(databaseConfig);
              DistributedStorageAdmin admin = factory.getAdmin();
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
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace).createNamespace(namespace, options);
  }

  @Override
  public void createNamespace(String namespace) throws ExecutionException {
    getAdmin(namespace).createNamespace(namespace);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    getAdmin(namespace).createNamespace(namespace, ifNotExists);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace).createNamespace(namespace, ifNotExists, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace, table).createTable(namespace, table, metadata, options);
  }

  @Override
  public void createTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    getAdmin(namespace, table).createTable(namespace, table, metadata);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    getAdmin(namespace, table).createTable(namespace, table, metadata, ifNotExists);
  }

  @Override
  public void createTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace, table).createTable(namespace, table, metadata, ifNotExists, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    getAdmin(namespace, table).dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    getAdmin(namespace).dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    getAdmin(namespace, table).truncateTable(namespace, table);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return getAdmin(namespace, table).getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return getAdmin(namespace).getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return getAdmin(namespace).namespaceExists(namespace);
  }

  private DistributedStorageAdmin getAdmin(String namespace) {
    DistributedStorageAdmin admin = namespaceAdminMap.get(namespace);
    return admin != null ? admin : defaultAdmin;
  }

  private DistributedStorageAdmin getAdmin(String namespace, String table) {
    String fullTaleName = namespace + "." + table;
    DistributedStorageAdmin admin = tableAdminMap.get(fullTaleName);
    if (admin != null) {
      return admin;
    }
    return getAdmin(namespace);
  }

  @Override
  public void close() {
    for (DistributedStorageAdmin admin : admins) {
      admin.close();
    }
  }
}
