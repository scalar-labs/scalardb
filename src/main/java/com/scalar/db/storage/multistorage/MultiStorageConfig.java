package com.scalar.db.storage.multistorage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
public class MultiStorageConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "multistorage.";
  public static final String STORAGES = PREFIX + "storages";
  public static final String TABLE_MAPPING = PREFIX + "table-mapping";
  public static final String DEFAULT_STORAGE = PREFIX + "default-storage";

  private final Properties props;

  private Map<String, DatabaseConfig> databaseConfigMap;
  private Map<String, String> tableStorageMap;
  private String defaultStorage;

  public MultiStorageConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
  }

  public MultiStorageConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public MultiStorageConfig(Properties properties) {
    props = new Properties(properties);
    load();
  }

  public Properties getProperties() {
    return props;
  }

  private void load() {
    String storage = props.getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("multistorage")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be multistorage");
    }

    loadDatabaseConfigs();
    loadTableStorageMapping();

    defaultStorage = props.getProperty(DEFAULT_STORAGE);
    checkIfStorageExists(defaultStorage);
  }

  private void loadDatabaseConfigs() {
    Builder<String, DatabaseConfig> builder = ImmutableMap.builder();

    String storages = props.getProperty(STORAGES);
    if (storages != null) {
      for (String storage : storages.split(",")) {
        Properties properties = new Properties();
        for (String propertyName : props.stringPropertyNames()) {
          if (propertyName.startsWith(STORAGES + "." + storage + ".")) {
            properties.put(
                propertyName.replace("multistorage.storages." + storage + ".", ""),
                props.getProperty(propertyName));
          }
        }

        DatabaseConfig config = new DatabaseConfig(properties);
        if (config.getStorageClass() == MultiStorage.class) {
          throw new IllegalArgumentException("Does not support nested multi-storage: " + storage);
        }
        builder.put(storage, config);
      }
    }
    databaseConfigMap = builder.build();
  }

  private void loadTableStorageMapping() {
    Builder<String, String> builder = ImmutableMap.builder();

    String tableMapping = props.getProperty(TABLE_MAPPING);
    for (String tableAndStorage : tableMapping.split(",")) {
      String[] s = tableAndStorage.split(":");
      String table = s[0];
      String storage = s[1];

      checkIfStorageExists(storage);
      builder.put(table, storage);
    }
    tableStorageMap = builder.build();
  }

  private void checkIfStorageExists(String storage) {
    if (!databaseConfigMap.containsKey(storage)) {
      throw new IllegalArgumentException("storage not found: " + storage);
    }
  }

  public Map<String, DatabaseConfig> getDatabaseConfigMap() {
    return databaseConfigMap;
  }

  public Map<String, String> getTableStorageMap() {
    return tableStorageMap;
  }

  public String getDefaultStorage() {
    return defaultStorage;
  }
}
