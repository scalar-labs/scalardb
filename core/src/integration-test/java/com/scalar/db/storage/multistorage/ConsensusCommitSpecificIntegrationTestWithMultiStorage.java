package com.scalar.db.storage.multistorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitSpecificIntegrationTestWithMultiStorage
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, cassandra and jdbc
    properties.setProperty(MultiStorageConfig.STORAGES, "cassandra,jdbc");

    Properties propertiesForCassandra = MultiStorageEnv.getPropertiesForCassandra(testName);
    for (String propertyName : propertiesForCassandra.stringPropertyNames()) {
      properties.setProperty(
          MultiStorageConfig.STORAGES
              + ".cassandra."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForCassandra.getProperty(propertyName));
    }

    Properties propertiesForJdbc = MultiStorageEnv.getPropertiesForJdbc(testName);
    for (String propertyName : propertiesForJdbc.stringPropertyNames()) {
      properties.setProperty(
          MultiStorageConfig.STORAGES
              + ".jdbc."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForJdbc.getProperty(propertyName));
    }

    // Define namespace mapping from namespace1 to cassandra, from namespace2 to jdbc, and from
    // the coordinator namespace to cassandra
    properties.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING,
        getNamespace1()
            + ":cassandra,"
            + getNamespace2()
            + ":jdbc,"
            + Coordinator.NAMESPACE
            + ":cassandra");

    // The default storage is cassandra
    properties.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Add testName as a metadata schema suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
