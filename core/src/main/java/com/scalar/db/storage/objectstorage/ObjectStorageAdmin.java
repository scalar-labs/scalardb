package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ObjectStorageAdmin implements DistributedStorageAdmin {
  public static final String NAMESPACE_METADATA_TABLE = "namespaces";
  public static final String TABLE_METADATA_TABLE = "metadata";

  private final ObjectStorageWrapper wrapper;
  private final String metadataNamespace;

  @Inject
  public ObjectStorageAdmin(DatabaseConfig databaseConfig) {
    ObjectStorageConfig objectStorageConfig =
        ObjectStorageUtils.getObjectStorageConfig(databaseConfig);
    wrapper = ObjectStorageUtils.getObjectStorageWrapper(objectStorageConfig);
    metadataNamespace = objectStorageConfig.getMetadataNamespace();
  }

  public ObjectStorageAdmin(ObjectStorageWrapper wrapper, ObjectStorageConfig objectStorageConfig) {
    this.wrapper = wrapper;
    metadataNamespace = objectStorageConfig.getMetadataNamespace();
  }

  @Override
  public TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void close() {
    wrapper.close();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      insertNamespaceMetadata(namespace);
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
      insertTableMetadata(namespace, table, metadata);
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
      deleteTableMetadata(namespace, table);
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
      deleteNamespaceMetadata(namespace);
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
      upsertNamespaceMetadata(namespace);
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
      upsertTableMetadata(namespace, table, metadata);
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
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).addColumn(columnName, columnType).build();
      upsertTableMetadata(namespace, table, updatedTableMetadata);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to add a new column to the table %s",
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
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
    try {
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable();
      List<String> namespaceNames =
          metadataTable.keySet().stream()
              .map(ObjectStorageAdmin::getNamespaceNameFromTableMetadataKey)
              .distinct()
              .collect(Collectors.toList());
      for (String namespaceName : namespaceNames) {
        upsertNamespaceMetadata(namespaceName);
      }
    } catch (Exception e) {
      throw new ExecutionException("Failed to upgrade", e);
    }
  }

  private void insertNamespaceMetadata(String namespace) throws ExecutionException {
    try {
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          getNamespaceMetadataTable(readVersionMap);
      if (metadataTable.containsKey(namespace)) {
        throw new ExecutionException(
            String.format("The namespace metadata already exists: %s", namespace));
      }
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
          String.format("Failed to insert the namespace metadata: %s", namespace), e);
    }
  }

  private void insertTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    String tableMetadataKey = getTableMetadataKey(namespace, table);
    try {
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      if (metadataTable.containsKey(tableMetadataKey)) {
        throw new ExecutionException(
            String.format("The table metadata already exists: %s", tableMetadataKey));
      }
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
          String.format("Failed to insert the table metadata: %s", tableMetadataKey), e);
    }
  }

  private void upsertNamespaceMetadata(String namespace) throws ExecutionException {
    try {
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
          String.format("Failed to upsert the namespace metadata: %s", namespace), e);
    }
  }

  private void upsertTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    String tableMetadataKey = getTableMetadataKey(namespace, table);
    try {
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
          String.format("Failed to upsert the table metadata: %s", tableMetadataKey), e);
    }
  }

  private void deleteNamespaceMetadata(String namespace) throws ExecutionException {
    try {
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          getNamespaceMetadataTable(readVersionMap);
      if (metadataTable.isEmpty() || !metadataTable.containsKey(namespace)) {
        throw new ExecutionException(
            String.format("The namespace metadata does not exist: %s", namespace));
      }
      metadataTable.remove(namespace);
      String readVersion = readVersionMap.get(NAMESPACE_METADATA_TABLE);
      if (metadataTable.isEmpty()) {
        deleteMetadataTable(NAMESPACE_METADATA_TABLE, readVersion);
      } else {
        updateMetadataTable(NAMESPACE_METADATA_TABLE, metadataTable, readVersion);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to delete the namespace metadata: %s", namespace), e);
    }
  }

  private void deleteTableMetadata(String namespace, String table) throws ExecutionException {
    String tableMetadataKey = getTableMetadataKey(namespace, table);
    try {
      Map<String, String> readVersionMap = new HashMap<>();
      Map<String, ObjectStorageTableMetadata> metadataTable = getTableMetadataTable(readVersionMap);
      if (metadataTable.isEmpty() || !metadataTable.containsKey(tableMetadataKey)) {
        throw new ExecutionException(
            String.format("The table metadata does not exist: %s", tableMetadataKey));
      }
      metadataTable.remove(tableMetadataKey);
      String readVersion = readVersionMap.get(TABLE_METADATA_TABLE);
      if (metadataTable.isEmpty()) {
        deleteMetadataTable(TABLE_METADATA_TABLE, readVersion);
      } else {
        updateMetadataTable(TABLE_METADATA_TABLE, metadataTable, readVersion);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to delete the table metadata: %s", tableMetadataKey), e);
    }
  }

  private Map<String, ObjectStorageNamespaceMetadata> getNamespaceMetadataTable()
      throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_METADATA_TABLE, null));
      return JsonConvertor.deserialize(
          response.getPayload(),
          new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      if (e.getStatusCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        return Collections.emptyMap();
      }
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private Map<String, ObjectStorageNamespaceMetadata> getNamespaceMetadataTable(
      Map<String, String> readVersionMap) throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_METADATA_TABLE, null));
      readVersionMap.put(NAMESPACE_METADATA_TABLE, response.getVersion());
      return JsonConvertor.deserialize(
          response.getPayload(),
          new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      if (e.getStatusCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        return Collections.emptyMap();
      }
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private Map<String, ObjectStorageTableMetadata> getTableMetadataTable()
      throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, TABLE_METADATA_TABLE, null));
      return JsonConvertor.deserialize(
          response.getPayload(), new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      if (e.getStatusCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        return Collections.emptyMap();
      }
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private Map<String, ObjectStorageTableMetadata> getTableMetadataTable(
      Map<String, String> readVersionMap) throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, TABLE_METADATA_TABLE, null));
      readVersionMap.put(TABLE_METADATA_TABLE, response.getVersion());
      return JsonConvertor.deserialize(
          response.getPayload(), new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    } catch (ObjectStorageWrapperException e) {
      if (e.getStatusCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        return Collections.emptyMap();
      }
      throw new ExecutionException("Failed to get the metadata table.", e);
    }
  }

  private <T> void insertMetadataTable(String table, Map<String, T> metadataTable)
      throws ExecutionException {
    try {
      wrapper.insert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, table, null),
          JsonConvertor.serialize(metadataTable));
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException("Failed to insert the metadata table.", e);
    }
  }

  private <T> void updateMetadataTable(
      String table, Map<String, T> metadataTable, String readVersion) throws ExecutionException {
    try {
      wrapper.update(
          ObjectStorageUtils.getObjectKey(metadataNamespace, table, null),
          JsonConvertor.serialize(metadataTable),
          readVersion);
    } catch (Exception e) {
      throw new ExecutionException("Failed to update the metadata table.", e);
    }
  }

  private void deleteMetadataTable(String table, String readVersion) throws ExecutionException {
    try {
      wrapper.delete(ObjectStorageUtils.getObjectKey(metadataNamespace, table, null), readVersion);
    } catch (Exception e) {
      throw new ExecutionException("Failed to delete the metadata table.", e);
    }
  }

  private void deleteTableData(String namespace, String table) throws ExecutionException {
    try {
      wrapper.deleteByPrefix(ObjectStorageUtils.getObjectKey(namespace, table, null));
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
