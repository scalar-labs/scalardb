package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ObjectStorageAdmin implements DistributedStorageAdmin {
  public static final String NAMESPACE_METADATA_TABLE = "namespaces";
  public static final String TABLE_METADATA_TABLE = "metadata";

  private static final StorageInfo STORAGE_INFO =
      new StorageInfoImpl(
          "object_storage",
          StorageInfo.MutationAtomicityUnit.PARTITION,
          // No limit on the number of mutations
          Integer.MAX_VALUE);

  private final ObjectStorageWrapper wrapper;
  private final String metadataNamespace;

  @Inject
  public ObjectStorageAdmin(DatabaseConfig databaseConfig) {
    ObjectStorageConfig objectStorageConfig =
        ObjectStorageUtils.getObjectStorageConfig(databaseConfig);
    wrapper = ObjectStorageWrapperFactory.create(objectStorageConfig);
    metadataNamespace = objectStorageConfig.getMetadataNamespace();
  }

  ObjectStorageAdmin(ObjectStorageWrapper wrapper, ObjectStorageConfig objectStorageConfig) {
    this.wrapper = wrapper;
    metadataNamespace = objectStorageConfig.getMetadataNamespace();
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) throws ExecutionException {
    return STORAGE_INFO;
  }

  @Override
  public void close() {
    wrapper.close();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      // Insert the namespace metadata
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          getNamespaceMetadataTable(readVersionMap);
      assert !metadataTable.containsKey(namespace);
      if (metadataTable.isEmpty()) {
        Map<String, ObjectStorageNamespaceMetadata> newMetadataTable =
            Collections.singletonMap(namespace, new ObjectStorageNamespaceMetadata(namespace));
        insertMetadataTable(NAMESPACE_METADATA_TABLE, newMetadataTable);
      } else {
        metadataTable.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
        updateMetadataTable(
            NAMESPACE_METADATA_TABLE, metadataTable, readVersionMap.get(NAMESPACE_METADATA_TABLE));
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to create the namespace %s", namespace), e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      // Insert the table metadata
      String tableMetadataKey = getTableMetadataKey(namespace, table);
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      assert !metadataTable.containsKey(tableMetadataKey);
      if (metadataTable.isEmpty()) {
        Map<String, ObjectStorageTableMetadata> newMetadataTable =
            Collections.singletonMap(tableMetadataKey, new ObjectStorageTableMetadata(metadata));
        insertMetadataTable(TABLE_METADATA_TABLE, newMetadataTable);
      } else {
        metadataTable.put(tableMetadataKey, new ObjectStorageTableMetadata(metadata));
        updateMetadataTable(
            TABLE_METADATA_TABLE, metadataTable, readVersionMap.get(TABLE_METADATA_TABLE));
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to create the table %s", ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try {
      deleteTableData(namespace, table);
      // Delete the table metadata
      String tableMetadataKey = getTableMetadataKey(namespace, table);
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      assert metadataTable.containsKey(tableMetadataKey);
      metadataTable.remove(tableMetadataKey);
      String readVersion = readVersionMap.get(TABLE_METADATA_TABLE);
      if (metadataTable.isEmpty()) {
        deleteMetadataTable(TABLE_METADATA_TABLE, readVersion);
      } else {
        updateMetadataTable(TABLE_METADATA_TABLE, metadataTable, readVersion);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to drop the table %s", ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      // Delete the namespace metadata
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          getNamespaceMetadataTable(readVersionMap);
      assert metadataTable.containsKey(namespace);
      metadataTable.remove(namespace);
      String readVersion = readVersionMap.get(NAMESPACE_METADATA_TABLE);
      if (metadataTable.isEmpty()) {
        deleteMetadataTable(NAMESPACE_METADATA_TABLE, readVersion);
      } else {
        updateMetadataTable(NAMESPACE_METADATA_TABLE, metadataTable, readVersion);
      }
    } catch (Exception e) {
      throw new ExecutionException(String.format("Failed to drop the namespace %s", namespace), e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    try {
      deleteTableData(namespace, table);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to truncate the table %s", ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      String tableMetadataKey = getTableMetadataKey(namespace, table);
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable();
      ObjectStorageTableMetadata tableMetadata = metadataTable.get(tableMetadataKey);
      return tableMetadata != null ? tableMetadata.toTableMetadata() : null;
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to get the table metadata of %s",
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      if (!namespaceExists(namespace)) {
        return Collections.emptySet();
      }
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable();
      return metadataTable.keySet().stream()
          .filter(
              tableMetadataKey ->
                  getNamespaceNameFromTableMetadataKey(tableMetadataKey).equals(namespace))
          .map(ObjectStorageAdmin::getTableNameFromTableMetadataKey)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to get the tables in the namespace %s", namespace), e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (metadataNamespace.equals(namespace)) {
      return true;
    }
    try {
      return getNamespaceMetadataTable().containsKey(namespace);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to check the existence of the namespace %s", namespace), e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      // Upsert the namespace metadata
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          getNamespaceMetadataTable(readVersionMap);
      if (metadataTable.isEmpty()) {
        insertMetadataTable(
            NAMESPACE_METADATA_TABLE,
            Collections.singletonMap(namespace, new ObjectStorageNamespaceMetadata(namespace)));
      } else {
        metadataTable.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
        updateMetadataTable(
            NAMESPACE_METADATA_TABLE, metadataTable, readVersionMap.get(NAMESPACE_METADATA_TABLE));
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to repair the namespace %s", namespace), e);
    }
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      // Upsert the table metadata
      String tableMetadataKey = getTableMetadataKey(namespace, table);
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      if (metadataTable.isEmpty()) {
        insertMetadataTable(
            TABLE_METADATA_TABLE,
            Collections.singletonMap(tableMetadataKey, new ObjectStorageTableMetadata(metadata)));
      } else {
        metadataTable.put(tableMetadataKey, new ObjectStorageTableMetadata(metadata));
        updateMetadataTable(
            TABLE_METADATA_TABLE, metadataTable, readVersionMap.get(TABLE_METADATA_TABLE));
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to repair the table %s", ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      // Update the table metadata
      String tableMetadataKey = getTableMetadataKey(namespace, table);
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      TableMetadata currentTableMetadata = metadataTable.get(tableMetadataKey).toTableMetadata();
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).addColumn(columnName, columnType).build();
      metadataTable.put(tableMetadataKey, new ObjectStorageTableMetadata(updatedTableMetadata));
      updateMetadataTable(
          TABLE_METADATA_TABLE, metadataTable, readVersionMap.get(TABLE_METADATA_TABLE));
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to add a new column to the table %s",
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_DROP_COLUMN_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_RENAME_COLUMN_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_ALTER_COLUMN_TYPE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_RENAME_TABLE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      return getNamespaceMetadataTable().keySet();
    } catch (Exception e) {
      throw new ExecutionException("Failed to get the namespace names", e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    // Currently, nothing needs to be upgraded. Do nothing.
  }

  private Map<String, ObjectStorageNamespaceMetadata> getNamespaceMetadataTable()
      throws ExecutionException {
    return getNamespaceMetadataTable(null);
  }

  private Map<String, ObjectStorageNamespaceMetadata> getNamespaceMetadataTable(
      @Nullable Map<String, String> readVersionMap) throws ExecutionException {
    try {
      Optional<ObjectStorageWrapperResponse> response =
          wrapper.get(ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_METADATA_TABLE));
      if (!response.isPresent()) {
        return Collections.emptyMap();
      }
      if (readVersionMap != null) {
        readVersionMap.put(NAMESPACE_METADATA_TABLE, response.get().getVersion());
      }
      return Serializer.deserialize(
          response.get().getPayload(),
          new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private Map<String, ObjectStorageTableMetadata> getTableMetadataTable()
      throws ExecutionException {
    return getTableMetadataTable(null);
  }

  private Map<String, ObjectStorageTableMetadata> getTableMetadataTable(
      @Nullable Map<String, String> readVersionMap) throws ExecutionException {
    try {
      Optional<ObjectStorageWrapperResponse> response =
          wrapper.get(ObjectStorageUtils.getObjectKey(metadataNamespace, TABLE_METADATA_TABLE));
      if (!response.isPresent()) {
        return Collections.emptyMap();
      }
      if (readVersionMap != null) {
        readVersionMap.put(TABLE_METADATA_TABLE, response.get().getVersion());
      }
      return Serializer.deserialize(
          response.get().getPayload(),
          new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private <T> void insertMetadataTable(String table, Map<String, T> metadataTable)
      throws ExecutionException {
    try {
      wrapper.insert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, table),
          Serializer.serialize(metadataTable));
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException("Failed to insert the metadata table.", e);
    }
  }

  private <T> void updateMetadataTable(
      String table, Map<String, T> metadataTable, String readVersion) throws ExecutionException {
    try {
      wrapper.update(
          ObjectStorageUtils.getObjectKey(metadataNamespace, table),
          Serializer.serialize(metadataTable),
          readVersion);
    } catch (Exception e) {
      throw new ExecutionException("Failed to update the metadata table.", e);
    }
  }

  private void deleteMetadataTable(String table, String readVersion) throws ExecutionException {
    try {
      wrapper.delete(ObjectStorageUtils.getObjectKey(metadataNamespace, table), readVersion);
    } catch (Exception e) {
      throw new ExecutionException("Failed to delete the metadata table.", e);
    }
  }

  private void deleteTableData(String namespace, String table) throws ExecutionException {
    try {
      wrapper.deleteByPrefix(ObjectStorageUtils.getObjectKey(namespace, table, ""));
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to delete the table data of %s",
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  private static String getTableMetadataKey(String namespace, String table) {
    return String.join(
        String.valueOf(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER), namespace, table);
  }

  private static String getNamespaceNameFromTableMetadataKey(String tableMetadataKey) {
    List<String> parts =
        Splitter.on(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER).splitToList(tableMetadataKey);
    if (parts.size() != 2 || parts.get(0).isEmpty()) {
      throw new IllegalArgumentException("Invalid table metadata key: " + tableMetadataKey);
    }
    return parts.get(0);
  }

  private static String getTableNameFromTableMetadataKey(String tableMetadataKey) {
    List<String> parts =
        Splitter.on(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER).splitToList(tableMetadataKey);
    if (parts.size() != 2 || parts.get(1).isEmpty()) {
      throw new IllegalArgumentException("Invalid table metadata key: " + tableMetadataKey);
    }
    return parts.get(1);
  }
}
