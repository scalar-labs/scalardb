package com.scalar.db.storage.multistorage;

import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
public class MultiStorageConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "multi_storage.";
  public static final String STORAGES = PREFIX + "storages";
  public static final String TABLE_MAPPING = PREFIX + "table_mapping";
  public static final String NAMESPACE_MAPPING = PREFIX + "namespace_mapping";
  public static final String DEFAULT_STORAGE = PREFIX + "default_storage";

  private static final String MULTI_STORAGE = "multi-storage";

  private final Map<String, Properties> databasePropertiesMap;
  private final Map<String, String> tableStorageMap;
  private final Map<String, String> namespaceStorageMap;
  private final String defaultStorage;

  public MultiStorageConfig(DatabaseConfig databaseConfig) {
    String storage = getString(databaseConfig.getProperties(), DatabaseConfig.STORAGE, null);
    if (storage == null || !storage.equals(MULTI_STORAGE)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + MULTI_STORAGE + "'");
    }

    databasePropertiesMap = loadDatabasePropertiesMapping(databaseConfig.getProperties());
    tableStorageMap = loadTableStorageMapping(databaseConfig.getProperties());
    namespaceStorageMap = loadNamespaceStorageMapping(databaseConfig.getProperties());

    defaultStorage = getString(databaseConfig.getProperties(), DEFAULT_STORAGE, null);
    checkIfStorageExists(defaultStorage);
  }

  private Map<String, Properties> loadDatabasePropertiesMapping(Properties properties) {
    String[] storages = getStringArray(properties, STORAGES, null);
    if (storages == null) {
      return Collections.emptyMap();
    }

    ImmutableMap.Builder<String, Properties> builder = ImmutableMap.builder();
    for (String storage : storages) {
      Properties dbProps = new Properties();
      for (String propertyName : properties.stringPropertyNames()) {
        if (propertyName.startsWith(STORAGES + "." + storage + ".")) {
          dbProps.put(
              propertyName.replace("multi_storage.storages." + storage + ".", ""),
              properties.getProperty(propertyName));
        }
      }

      if (dbProps.getProperty(DatabaseConfig.STORAGE).equals(MULTI_STORAGE)) {
        throw new IllegalArgumentException(
            "Does not support nested " + MULTI_STORAGE + ": " + storage);
      }
      builder.put(storage, dbProps);
    }
    return builder.build();
  }

  private Map<String, String> loadTableStorageMapping(Properties properties) {
    String[] tableMapping = getStringArray(properties, TABLE_MAPPING, null);
    if (tableMapping == null) {
      return Collections.emptyMap();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String tableAndStorage : tableMapping) {
      String[] s = tableAndStorage.split(":", -1);
      String table = s[0];
      String storage = s[1];
      checkIfStorageExists(storage);
      builder.put(table, storage);
    }
    return builder.build();
  }

  private Map<String, String> loadNamespaceStorageMapping(Properties properties) {
    String[] namespaceMapping = getStringArray(properties, NAMESPACE_MAPPING, null);
    if (namespaceMapping == null) {
      return Collections.emptyMap();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String namespaceAndStorage : namespaceMapping) {
      String[] s = namespaceAndStorage.split(":", -1);
      String namespace = s[0];
      String storage = s[1];
      checkIfStorageExists(storage);
      builder.put(namespace, storage);
    }
    return builder.build();
  }

  private void checkIfStorageExists(String storage) {
    if (storage == null || !databasePropertiesMap.containsKey(storage)) {
      throw new IllegalArgumentException("storage not found: " + storage);
    }
  }

  public Map<String, Properties> getDatabasePropertiesMap() {
    return databasePropertiesMap;
  }

  public Map<String, String> getTableStorageMap() {
    return tableStorageMap;
  }

  public Map<String, String> getNamespaceStorageMap() {
    return namespaceStorageMap;
  }

  public String getDefaultStorage() {
    return defaultStorage;
  }
}
