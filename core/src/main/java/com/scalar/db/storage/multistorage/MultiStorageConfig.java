package com.scalar.db.storage.multistorage;

import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class MultiStorageConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "multi_storage.";
  public static final String STORAGES = PREFIX + "storages";
  public static final String TABLE_MAPPING = PREFIX + "table_mapping";
  public static final String NAMESPACE_MAPPING = PREFIX + "namespace_mapping";
  public static final String DEFAULT_STORAGE = PREFIX + "default_storage";

  private static final String MULTI_STORAGE = "multi-storage";

  private final Properties props;

  private Map<String, DatabaseConfig> databaseConfigMap;
  private Map<String, String> tableStorageMap;
  private Map<String, String> namespaceStorageMap;
  private String defaultStorage;

  public MultiStorageConfig(File propertiesFile) throws IOException {
    try (FileInputStream stream = new FileInputStream(propertiesFile)) {
      props = new Properties();
      props.load(stream);
    }
    load();
  }

  public MultiStorageConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public MultiStorageConfig(Properties properties) {
    props = new Properties();
    props.putAll(properties);
    load();
  }

  public Properties getProperties() {
    return props;
  }

  private void load() {
    String storage = getString(getProperties(), DatabaseConfig.STORAGE, null);
    if (storage == null || !storage.equals(MULTI_STORAGE)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + MULTI_STORAGE + "'");
    }

    loadDatabaseConfigs();
    loadTableStorageMapping();
    loadNamespaceStorageMapping();

    defaultStorage = getString(getProperties(), DEFAULT_STORAGE, null);
    checkIfStorageExists(defaultStorage);
  }

  private void loadDatabaseConfigs() {
    String[] storages = getStringArray(getProperties(), STORAGES, null);
    if (storages == null) {
      databaseConfigMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, DatabaseConfig> builder = ImmutableMap.builder();
    for (String storage : storages) {
      Properties dbProps = new Properties();
      for (String propertyName : props.stringPropertyNames()) {
        if (propertyName.startsWith(STORAGES + "." + storage + ".")) {
          dbProps.put(
              propertyName.replace("multi_storage.storages." + storage + ".", ""),
              props.getProperty(propertyName));
        }
      }

      if (dbProps.getProperty(DatabaseConfig.STORAGE).equals(MULTI_STORAGE)) {
        throw new IllegalArgumentException(
            "Does not support nested " + MULTI_STORAGE + ": " + storage);
      }
      builder.put(storage, new DatabaseConfig(dbProps));
    }
    databaseConfigMap = builder.build();
  }

  private void loadTableStorageMapping() {
    String[] tableMapping = getStringArray(getProperties(), TABLE_MAPPING, null);
    if (tableMapping == null) {
      tableStorageMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String tableAndStorage : tableMapping) {
      String[] s = tableAndStorage.split(":", -1);
      String table = s[0];
      String storage = s[1];
      checkIfStorageExists(storage);
      builder.put(table, storage);
    }
    tableStorageMap = builder.build();
  }

  private void loadNamespaceStorageMapping() {
    String[] namespaceMapping = getStringArray(getProperties(), NAMESPACE_MAPPING, null);
    if (namespaceMapping == null) {
      namespaceStorageMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String namespaceAndStorage : namespaceMapping) {
      String[] s = namespaceAndStorage.split(":", -1);
      String namespace = s[0];
      String storage = s[1];
      checkIfStorageExists(storage);
      builder.put(namespace, storage);
    }
    namespaceStorageMap = builder.build();
  }

  private void checkIfStorageExists(String storage) {
    if (storage == null || !databaseConfigMap.containsKey(storage)) {
      throw new IllegalArgumentException("storage not found: " + storage);
    }
  }

  public Map<String, DatabaseConfig> getDatabaseConfigMap() {
    return databaseConfigMap;
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
