package com.scalar.db.storage.multistorage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

  /** @deprecated Will be removed in 5.0.0. */
  @Deprecated private final Map<String, DistributedStorageAdmin> tableAdminMap;

  private final Map<String, String> namespaceStorageNameMap;
  private final String defaultStorageName;
  private final Map<String, DistributedStorageAdmin> nameAdminMap;

  @Inject
  public MultiStorageAdmin(DatabaseConfig databaseConfig) {
    MultiStorageConfig config = new MultiStorageConfig(databaseConfig);

    nameAdminMap =
        config.getDatabasePropertiesMap().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, e -> StorageFactory.create(e.getValue()).getStorageAdmin()));

    tableAdminMap =
        config.getTableStorageMap().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, e -> nameAdminMap.get(e.getValue())));

    namespaceStorageNameMap = ImmutableMap.copyOf(config.getNamespaceStorageMap());

    defaultStorageName = config.getDefaultStorage();
  }

  @VisibleForTesting
  MultiStorageAdmin(
      Map<String, DistributedStorageAdmin> nameAdminMap,
      Map<String, DistributedStorageAdmin> tableAdminMap,
      Map<String, String> namespaceStorageNameMap,
      String defaultStorageName) {
    this.nameAdminMap = ImmutableMap.copyOf(nameAdminMap);
    this.tableAdminMap = ImmutableMap.copyOf(tableAdminMap);
    this.namespaceStorageNameMap = ImmutableMap.copyOf(namespaceStorageNameMap);
    this.defaultStorageName = defaultStorageName;
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
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    getAdmin(namespace, table).dropColumnFromTable(namespace, table, columnName);
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    getAdmin(namespace, table).renameColumn(namespace, table, oldColumnName, newColumnName);
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    getAdmin(namespace, table).alterColumnType(namespace, table, columnName, newColumnType);
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    getAdmin(namespace, oldTableName).renameTable(namespace, oldTableName, newTableName);
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    getAdmin(namespace, table).importTable(namespace, table, options, overrideColumnsType);
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
    DistributedStorageAdmin defaultAdmin = nameAdminMap.get(defaultStorageName);
    Set<String> namespaceNames = new HashSet<>(defaultAdmin.getNamespaceNames());

    Set<DistributedStorageAdmin> adminsWithoutDefaultAdmin =
        namespaceStorageNameMap.values().stream()
            .map(nameAdminMap::get)
            .collect(Collectors.toSet());
    adminsWithoutDefaultAdmin.remove(defaultAdmin);

    for (DistributedStorageAdmin admin : adminsWithoutDefaultAdmin) {
      Set<String> existingNamespaces = admin.getNamespaceNames();
      // Filter out namespace not in the mapping
      for (String existingNamespace : existingNamespaces) {
        String storageName = namespaceStorageNameMap.get(existingNamespace);
        if (storageName != null && admin.equals(nameAdminMap.get(storageName))) {
          namespaceNames.add(existingNamespace);
        }
      }
    }

    return namespaceNames;
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    for (DistributedStorageAdmin admin : nameAdminMap.values()) {
      admin.upgrade(options);
    }
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) throws ExecutionException {
    try {
      String storageName = getStorageName(namespace);
      StorageInfo storageInfo = nameAdminMap.get(storageName).getStorageInfo(namespace);
      return new StorageInfoImpl(
          storageName,
          storageInfo.getMutationAtomicityUnit(),
          storageInfo.getMaxAtomicMutationsCount(),
          storageInfo.isConsistentVirtualTableReadGuaranteed());
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ExecutionException) {
        throw (ExecutionException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      Map<String, String> options)
      throws ExecutionException {
    StorageInfo storageInfo = getStorageInfo(namespace);
    StorageInfo storageInfoForLeftSourceNamespace = getStorageInfo(leftSourceNamespace);
    StorageInfo storageInfoForRightSourceNamespace = getStorageInfo(rightSourceNamespace);
    if (!storageInfo.getStorageName().equals(storageInfoForLeftSourceNamespace.getStorageName())) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_IN_DIFFERENT_STORAGE_FROM_SOURCE_TABLES.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table),
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }
    if (!storageInfoForLeftSourceNamespace
        .getStorageName()
        .equals(storageInfoForRightSourceNamespace.getStorageName())) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_SOURCE_TABLES_IN_DIFFERENT_STORAGES.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }

    getAdmin(namespace)
        .createVirtualTable(
            namespace,
            table,
            leftSourceNamespace,
            leftSourceTable,
            rightSourceNamespace,
            rightSourceTable,
            joinType,
            options);
  }

  @Override
  public Optional<VirtualTableInfo> getVirtualTableInfo(String namespace, String table)
      throws ExecutionException {
    return getAdmin(namespace, table).getVirtualTableInfo(namespace, table);
  }

  private String getStorageName(String namespace) {
    return namespaceStorageNameMap.getOrDefault(namespace, defaultStorageName);
  }

  private DistributedStorageAdmin getAdmin(String namespace) {
    return nameAdminMap.get(getStorageName(namespace));
  }

  private DistributedStorageAdmin getAdmin(String namespace, String table) {
    String fullTaleName = namespace + "." + table;
    DistributedStorageAdmin admin = tableAdminMap.get(fullTaleName);
    if (admin != null) {
      return admin;
    }
    return getAdmin(namespace);
  }

  /**
   * Returns a map of underlying storage admins keyed by the user-defined storage name (configured
   * via {@code scalar.db.multi_storage.storages}).
   *
   * @return an unmodifiable map of underlying storage admins keyed by storage name
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Map<String, DistributedStorageAdmin> getNameAdminMap() {
    return nameAdminMap;
  }

  @Override
  public void close() {
    for (DistributedStorageAdmin admin : nameAdminMap.values()) {
      admin.close();
    }
  }
}
