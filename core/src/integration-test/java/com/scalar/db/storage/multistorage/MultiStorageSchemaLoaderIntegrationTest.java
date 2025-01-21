package com.scalar.db.storage.multistorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class MultiStorageSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, cassandra and jdbc
    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,jdbc");

    Properties propertiesForCassandra = MultiStorageEnv.getPropertiesForCassandra(testName);
    for (String propertyName : propertiesForCassandra.stringPropertyNames()) {
      props.setProperty(
          MultiStorageConfig.STORAGES
              + ".cassandra."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForCassandra.getProperty(propertyName));
    }

    Properties propertiesForJdbc = MultiStorageEnv.getPropertiesForJdbc(testName);
    for (String propertyName : propertiesForJdbc.stringPropertyNames()) {
      props.setProperty(
          MultiStorageConfig.STORAGES
              + ".jdbc."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForJdbc.getProperty(propertyName));
    }

    // Define namespace mapping from namespace1 to cassandra, from namespace2 to jdbc, and from
    // the coordinator namespace to cassandra
    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING,
        getNamespace1()
            + ":cassandra,"
            + getNamespace2()
            + ":jdbc,"
            + Coordinator.NAMESPACE
            + ":cassandra");

    // The default storage is cassandra
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    return props;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new MultiStorageAdminTestUtils(
        MultiStorageEnv.getPropertiesForCassandra(testName),
        MultiStorageEnv.getPropertiesForJdbc(testName));
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
