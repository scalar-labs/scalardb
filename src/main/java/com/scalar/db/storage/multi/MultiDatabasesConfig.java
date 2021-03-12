package com.scalar.db.storage.multi;

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
public class MultiDatabasesConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "multi.";
  public static final String DATABASES = PREFIX + "databases";
  public static final String TABLE_MAPPING = PREFIX + "mapping.tables";
  public static final String DEFAULT_DATABASE = PREFIX + "mapping.default";

  private final Properties props;

  private Map<String, DatabaseConfig> databaseConfigMap;
  private Map<String, String> tableDatabaseMap;
  private String defaultDatabase;

  public MultiDatabasesConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
  }

  public MultiDatabasesConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public MultiDatabasesConfig(Properties properties) {
    props = new Properties(properties);
    load();
  }

  public Properties getProperties() {
    return props;
  }

  private void load() {
    String storage = props.getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("multi")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be multi");
    }

    loadDatabaseConfigs();
    loadTableDatabaseMappings();

    defaultDatabase = props.getProperty(DEFAULT_DATABASE);
    checkIfDatabaseExists(defaultDatabase);
  }

  private void loadDatabaseConfigs() {
    Builder<String, DatabaseConfig> builder = ImmutableMap.builder();

    String databases = props.getProperty(DATABASES);
    if (databases != null) {
      for (String database : databases.split(",")) {
        Properties properties = new Properties();
        for (String propertyName : props.stringPropertyNames()) {
          if (propertyName.startsWith(DATABASES + "." + database + ".")) {
            properties.put(
                propertyName.replace("multi.databases." + database + ".", ""),
                props.getProperty(propertyName));
          }
        }

        DatabaseConfig config = new DatabaseConfig(properties);
        if (config.getStorageClass() == MultiDatabases.class) {
          throw new IllegalArgumentException(
              "Does not support nested multi databases: " + database);
        }
        builder.put(database, config);
      }
    }
    databaseConfigMap = builder.build();
  }

  private void loadTableDatabaseMappings() {
    Builder<String, String> builder = ImmutableMap.builder();

    String tables = props.getProperty(TABLE_MAPPING);
    for (String table : tables.split(",")) {
      String database = props.getProperty(TABLE_MAPPING + "." + table);
      checkIfDatabaseExists(database);
      builder.put(table, database);
    }
    tableDatabaseMap = builder.build();
  }

  private void checkIfDatabaseExists(String database) {
    if (!databaseConfigMap.containsKey(database)) {
      throw new IllegalArgumentException("database not found: " + database);
    }
  }

  public Map<String, DatabaseConfig> getDatabaseConfigMap() {
    return databaseConfigMap;
  }

  public Map<String, String> getTableDatabaseMap() {
    return tableDatabaseMap;
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }
}
