package com.scalar.db.storage.objectstorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ObjectStorageEnv {
  private static final String PROP_OBJECT_STORAGE_ENDPOINT = "scalardb.object_storage.endpoint";
  private static final String PROP_OBJECT_STORAGE_USERNAME = "scalardb.object_storage.username";
  private static final String PROP_OBJECT_STORAGE_PASSWORD = "scalardb.object_storage.password";

  private static final String DEFAULT_OBJECT_STORAGE_ENDPOINT =
      "http://localhost:10000/test/test-container";
  private static final String DEFAULT_OBJECT_STORAGE_USERNAME = "test";
  private static final String DEFAULT_OBJECT_STORAGE_PASSWORD = "test";

  private ObjectStorageEnv() {}

  public static Properties getProperties(String testName) {
    String accountName =
        System.getProperty(PROP_OBJECT_STORAGE_USERNAME, DEFAULT_OBJECT_STORAGE_USERNAME);
    String accountKey =
        System.getProperty(PROP_OBJECT_STORAGE_PASSWORD, DEFAULT_OBJECT_STORAGE_PASSWORD);
    String endpoint =
        System.getProperty(PROP_OBJECT_STORAGE_ENDPOINT, DEFAULT_OBJECT_STORAGE_ENDPOINT);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, endpoint);
    properties.setProperty(DatabaseConfig.USERNAME, accountName);
    properties.setProperty(DatabaseConfig.PASSWORD, accountKey);
    properties.setProperty(DatabaseConfig.STORAGE, BlobStorageConfig.STORAGE_NAME);
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");

    // Add testName as a metadata namespace suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }

  public static Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }
}
