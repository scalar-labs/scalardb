package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;

public class CassandraConfig {
  public static final String STORAGE_NAME = "cassandra";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  private final String metadataKeyspace;

  public CassandraConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }
    metadataKeyspace = databaseConfig.getSystemNamespaceName();
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public String getMetadataKeyspace() {
    return metadataKeyspace;
  }
}
