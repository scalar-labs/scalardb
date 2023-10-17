package com.scalar.db.storage.multistorage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation with multi-storage for {@link DistributedStorageAdmin}.
 *
 * <p>This implementation holds multiple DistributedStorageAdmin instances. It chooses an instance
 * on the basis of the specified configuration and a given operation.
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
  public MultiStorageAdmin(DatabaseConfig databaseConfig) {
    MultiStorageConfig config = new MultiStorageConfig(databaseConfig);

    admins = new ArrayList<>();
    Map<String, DistributedStorageAdmin> nameAdminMap = new HashMap<>();
    config
        .getDatabasePropertiesMap()
        .forEach(
            (storageName, properties) -> {
              StorageFactory factory = StorageFactory.create(properties);
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
            (namespace, storageName) ->
                namespaceAdminMap.put(namespace, nameAdminMap.get(storageName)));

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
    ImmutableSet.Builder<DistributedStorageAdmin> adminsSet = ImmutableSet.builder();
    adminsSet.addAll(tableAdminMap.values());
    adminsSet.addAll(namespaceAdminMap.values());
    adminsSet.add(defaultAdmin);
    this.admins = adminsSet.build().asList();
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
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace, table).createIndex(namespace, table, columnName, options);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    getAdmin(namespace, table).dropIndex(namespace, table, columnName);
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

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace).repairNamespace(namespace, options);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    getAdmin(namespace, table).repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    getAdmin(namespace, table).addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    return getAdmin(namespace, table).getImportTableMetadata(namespace, table);
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    getAdmin(namespace, table).addRawColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public void importTable(String namespace, String table) throws ExecutionException {
    getAdmin(namespace, table).importTable(namespace, table);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    // Only return existing namespaces that are listed in the namespace mapping configuration or
    // when they belong to the default storage
    //
    // For example, if the storages contain the following namespaces :
    // - mysql : mysqlStorageAdmin.getNamespaceNames() = [ns1, ns2]
    // - cassandra : cassandraStorageAdmin.getNamespaceNames() = [ns3]
    // - cosmos : cosmosStorageAdmin.getNamespaceNames() = [ns4, ns5]
    // And the default storage is cosmos :
    // - scalar.db.multi_storage.default_storage=cosmos
    // And the namespace mapping set in the configuration is :
    // - scalar.db.multi_storage.namespace_mapping=ns1:mysql,ns2:cassandra,ns3:cassandra
    //
    // Then multiStorageAdmin.getNamespaceNames() = [ns1, ns3, ns4, ns5]
    // The reasoning is:
    // - ns1 is present in the mysql storage and listed in the mapping belonging to mysql
    //     => returned
    // - ns2 is present in the mysql storage but listed in the mapping belonging to cassandra
    //     => not returned
    // - ns3 is present in the cassandra storage and listed in the mapping belonging to cassandra
    //     => returned
    // - ns4 and ns5 are in the default storage (cosmos)
    //     => returned
    Set<String> namespaceNames = new HashSet<>(defaultAdmin.getNamespaceNames());

    Set<DistributedStorageAdmin> adminsWithoutDefaultAdmin =
        new HashSet<>(namespaceAdminMap.values());
    adminsWithoutDefaultAdmin.remove(defaultAdmin);
    for (DistributedStorageAdmin admin : adminsWithoutDefaultAdmin) {
      Set<String> existingNamespaces = admin.getNamespaceNames();
      // Filter out namespace not in the mapping
      for (String existingNamespace : existingNamespaces) {
        if (admin.equals(namespaceAdminMap.get(existingNamespace))) {
          namespaceNames.add(existingNamespace);
        }
      }
    }

    return namespaceNames;
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
