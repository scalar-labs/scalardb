package com.scalar.db.storage.objectstorage;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public class ObjectStorageEnv {
  private static final String PROP_OBJECT_STORAGE_ENDPOINT = "scalardb.object_storage.endpoint";
  private static final String PROP_OBJECT_STORAGE_USERNAME = "scalardb.object_storage.username";
  private static final String PROP_OBJECT_STORAGE_PASSWORD = "scalardb.object_storage.password";
  private static final String PROP_OBJECT_STORAGE_BUCKET = "scalardb.object_storage.storage_type";

  private static final String DEFAULT_BLOB_ENDPOINT = "http://localhost:10000/";
  private static final String DEFAULT_BLOB_USERNAME = "devstoreaccount1";
  private static final String DEFAULT_BLOB_PASSWORD =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final String DEFAULT_BLOB_CONTAINER = "fake-container";

  private ObjectStorageEnv() {}

  public static Properties getProperties(String testName) {
    String accountName = System.getProperty(PROP_OBJECT_STORAGE_USERNAME, DEFAULT_BLOB_USERNAME);
    String accountKey = System.getProperty(PROP_OBJECT_STORAGE_PASSWORD, DEFAULT_BLOB_PASSWORD);
    String endpoint =
        System.getProperty(PROP_OBJECT_STORAGE_ENDPOINT, DEFAULT_BLOB_ENDPOINT) + accountName;
    String bucket = System.getProperty(PROP_OBJECT_STORAGE_BUCKET, DEFAULT_BLOB_CONTAINER);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, endpoint);
    properties.setProperty(DatabaseConfig.USERNAME, accountName);
    properties.setProperty(DatabaseConfig.PASSWORD, accountKey);
    properties.setProperty(DatabaseConfig.STORAGE, BlobConfig.STORAGE_NAME);
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");
    properties.setProperty(BlobConfig.BUCKET, bucket);

    // Add testName as a metadata namespace suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }
}
