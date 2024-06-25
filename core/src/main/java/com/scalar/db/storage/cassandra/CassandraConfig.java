package com.scalar.db.storage.cassandra;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;

public class CassandraConfig {
  // just hold the storage name for consistency with other storage
  public static final String STORAGE_NAME = "cassandra";
  // We introduced this property so that `admin.getNamespaceNames()` can return the system namespace
  // configured by ScalarDB Cluster.
  // In ScalarDB core, storages besides Cassandra can define the system namespace with storage
  // specific configuration. While ScalarDB Cluster can define the system namespace through this
  // property below. To not introduce a new configuration for Cassandra to define the system
  // namespace, we reuse the existing configuration defined by ScalarDB Cluster.
  public static final String SYSTEM_NAMESPACE_NAME = "scalar.db.system_namespace_name";
  private final String systemNamespaceName;

  public CassandraConfig(DatabaseConfig databaseConfig) {
    systemNamespaceName = getString(databaseConfig.getProperties(), SYSTEM_NAMESPACE_NAME, null);
  }
  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public Optional<String> getSystemNamespaceName() {
    return Optional.ofNullable(systemNamespaceName);
  }
}
