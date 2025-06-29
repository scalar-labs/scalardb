package com.scalar.db.storage.multistorage;

import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class MultiStorageConfig {

  private static final Logger logger = LoggerFactory.getLogger(MultiStorageConfig.class);

  public static final String STORAGE_NAME = "multi-storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + "multi_storage.";
  public static final String STORAGES = PREFIX + "storages";
  public static final String TABLE_MAPPING = PREFIX + "table_mapping";
  public static final String NAMESPACE_MAPPING = PREFIX + "namespace_mapping";
  public static final String DEFAULT_STORAGE = PREFIX + "default_storage";

  private final ImmutableMap<String, Properties> databasePropertiesMap;
  private final ImmutableMap<String, String> tableStorageMap;
  private final ImmutableMap<String, String> namespaceStorageMap;
  private final String defaultStorage;

  public MultiStorageConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    databasePropertiesMap = loadDatabasePropertiesMapping(databaseConfig.getProperties());
    tableStorageMap = loadTableStorageMapping(databaseConfig.getProperties());
    namespaceStorageMap = loadNamespaceStorageMapping(databaseConfig.getProperties());

    defaultStorage = getString(databaseConfig.getProperties(), DEFAULT_STORAGE, null);
    checkIfStorageExists(defaultStorage);
  }

  private ImmutableMap<String, Properties> loadDatabasePropertiesMapping(Properties properties) {
    String[] storages = getStringArray(properties, STORAGES, null);
    if (storages == null) {
      return ImmutableMap.of();
    }

    // Create a new Properties object for each storage
    ImmutableMap.Builder<String, Properties> builder = ImmutableMap.builder();
    for (String storage : storages) {
      Properties dbProps = new Properties();

      // Put all properties unrelated to multi-storage first for global properties for all storages
      for (String propertyName : properties.stringPropertyNames()) {
        if (!propertyName.startsWith(PREFIX)) {
          dbProps.put(propertyName, properties.getProperty(propertyName));
        }
      }

      // Put all properties related to the current storage while removing
      // `multi_storage.storages.<storage name>.` from the property name. For example, if the
      // storage name is `cassandra`, and if the property name is
      // `scalar.db.multi_storage.storages.cassandra.storage`, then we put the property with the key
      // `scalar.db.storage`.
      for (String propertyName : properties.stringPropertyNames()) {
        if (propertyName.startsWith(STORAGES + "." + storage + ".")) {
          dbProps.put(
              propertyName.replace("multi_storage.storages." + storage + ".", ""),
              properties.getProperty(propertyName));
        }
      }

      if (dbProps.getProperty(DatabaseConfig.STORAGE).equals(STORAGE_NAME)) {
        throw new IllegalArgumentException(
            CoreError.MULTI_STORAGE_NESTED_MULTI_STORAGE_DEFINITION_NOT_SUPPORTED.buildMessage(
                storage));
      }

      builder.put(storage, dbProps);
    }

    return builder.build();
  }

  private ImmutableMap<String, String> loadTableStorageMapping(Properties properties) {
    String[] tableMapping = getStringArray(properties, TABLE_MAPPING, null);
    if (tableMapping == null) {
      return ImmutableMap.of();
    }

    logger.warn(
        "The table mapping property \""
            + TABLE_MAPPING
            + "\" is deprecated and will be removed in 5.0.0. "
            + "Please use the namespace mapping property \""
            + NAMESPACE_MAPPING
            + "\" instead");

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

  private ImmutableMap<String, String> loadNamespaceStorageMapping(Properties properties) {
    String[] namespaceMapping = getStringArray(properties, NAMESPACE_MAPPING, null);
    if (namespaceMapping == null) {
      return ImmutableMap.of();
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
      throw new IllegalArgumentException(
          CoreError.MULTI_STORAGE_STORAGE_NOT_FOUND.buildMessage(storage));
    }
  }

  public Map<String, Properties> getDatabasePropertiesMap() {
    return databasePropertiesMap;
  }

  /**
   * @return a table storage mapping
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
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
