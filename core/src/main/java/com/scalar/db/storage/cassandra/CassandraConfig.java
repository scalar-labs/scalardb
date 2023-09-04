package com.scalar.db.storage.cassandra;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;

public class CassandraConfig {
  public static final String PREFIX = DatabaseConfig.PREFIX + "cassandra.";
  public static final String METADATA_KEYSPACE = PREFIX + "metadata.keyspace";
  @Nullable private final String metadataKeyspace;

  public CassandraConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals("cassandra")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'cassandra'");
    }
    metadataKeyspace = getString(databaseConfig.getProperties(), METADATA_KEYSPACE, null);
  }

  public Optional<String> getMetadataKeyspace() {
    return Optional.ofNullable(metadataKeyspace);
  }
}
