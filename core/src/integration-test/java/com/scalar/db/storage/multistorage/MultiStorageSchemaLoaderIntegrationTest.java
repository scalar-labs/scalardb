package com.scalar.db.storage.multistorage;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MultiStorageSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
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

    // Add testName as a coordinator schema suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    Properties propertiesForCassandra = MultiStorageEnv.getPropertiesForCassandra(testName);
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(propertiesForCassandra, testName);

    Properties propertiesForJdbc = MultiStorageEnv.getPropertiesForJdbc(testName);
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(propertiesForJdbc, testName);

    return new MultiStorageAdminTestUtils(propertiesForCassandra, propertiesForJdbc);
  }

  @Override
  protected List<String> getCommandArgsForCreation(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--replication-factor=1")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForReparation(Path configFilePath, Path schemaFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForReparation(configFilePath, schemaFilePath))
        .add("--replication-factor=1")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForUpgrade(Path configFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForUpgrade(configFilePath))
        .add("--replication-factor=1")
        .build();
  }

  @Override
  protected void waitForCreationIfNecessary() {
    // In some of the tests, we modify metadata in one Cassandra cluster session (via the
    // Schema Loader) and verify if such metadata were updated by using another session (via the
    // MultiStorageAdminTestUtils). But it takes some time for metadata change to be propagated from
    // one session to the other, so we need to wait
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
