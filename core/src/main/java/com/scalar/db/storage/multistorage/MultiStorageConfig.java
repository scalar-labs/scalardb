package com.scalar.db.storage.multistorage;

import static com.scalar.db.config.ConfigUtils.getString;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class MultiStorageConfig extends DatabaseConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "multi_storage.";
  public static final String STORAGES = PREFIX + "storages";
  public static final String TABLE_MAPPING = PREFIX + "table_mapping";
  public static final String NAMESPACE_MAPPING = PREFIX + "namespace_mapping";
  public static final String DEFAULT_STORAGE = PREFIX + "default_storage";

  private static final String MULTI_STORAGE = "multi-storage";

  private Map<String, DatabaseConfig> databaseConfigMap;
  private Map<String, String> tableStorageMap;
  private Map<String, String> namespaceStorageMap;
  private String defaultStorage;

  public MultiStorageConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public MultiStorageConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public MultiStorageConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getString(getProperties(), DatabaseConfig.STORAGE, null);
    if (storage == null || !storage.equals(MULTI_STORAGE)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + MULTI_STORAGE + "'");
    }

    super.load();

    loadDatabaseConfigs();
    loadTableStorageMapping();
    loadNamespaceStorageMapping();

    defaultStorage = getString(getProperties(), DEFAULT_STORAGE, null);
    checkIfStorageExists(defaultStorage);
  }

  private void loadDatabaseConfigs() {
    String storages = getString(getProperties(), STORAGES, null);
    if (storages == null) {
      databaseConfigMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, DatabaseConfig> builder = ImmutableMap.builder();
    for (String storage : storages.split(",", -1)) {
      Properties dbProps = new Properties();
      for (String propertyName : getProperties().stringPropertyNames()) {
        if (propertyName.startsWith(STORAGES + "." + storage + ".")) {
          dbProps.put(
              propertyName.replace("multi_storage.storages." + storage + ".", ""),
              getProperties().getProperty(propertyName));
        }
      }

      // since operations are copied in MultiStorage, we don't need to copy operations in the
      // storage
      dbProps.put(DatabaseConfig.NEED_OPERATION_COPY, "false");

      if (dbProps.getProperty(DatabaseConfig.STORAGE).equals(MULTI_STORAGE)) {
        throw new IllegalArgumentException(
            "Does not support nested " + MULTI_STORAGE + ": " + storage);
      }
      builder.put(storage, new DatabaseConfig(dbProps));
    }
    databaseConfigMap = builder.build();
  }

  private void loadTableStorageMapping() {
    String tableMapping = getString(getProperties(), TABLE_MAPPING, null);
    if (tableMapping == null) {
      tableStorageMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String tableAndStorage : tableMapping.split(",", -1)) {
      String[] s = tableAndStorage.split(":", -1);
      String table = s[0];
      String storage = s[1];
      checkIfStorageExists(storage);
      builder.put(table, storage);
    }
    tableStorageMap = builder.build();
  }

  private void loadNamespaceStorageMapping() {
    String namespaceMapping = getString(getProperties(), NAMESPACE_MAPPING, null);
    if (namespaceMapping == null) {
      namespaceStorageMap = Collections.emptyMap();
      return;
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String namespaceAndStorage : namespaceMapping.split(",", -1)) {
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
