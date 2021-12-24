package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.multistorage.MultiStorageConfig;
import com.scalar.db.storage.multistorage.MultiStorageEnv;
import java.util.Properties;

public class ConsensusCommitWithMultiStorageIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, storage1 and storage2
    props.setProperty(MultiStorageConfig.STORAGES, "storage1,storage2");

    DatabaseConfig configForStorage1 = MultiStorageEnv.getDatabaseConfigForStorage1();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.storage",
        configForStorage1.getProperties().getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.contact_points",
        configForStorage1.getProperties().getProperty(DatabaseConfig.CONTACT_POINTS));
    if (configForStorage1.getProperties().containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage1.contact_port",
          configForStorage1.getProperties().getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.username",
        configForStorage1.getProperties().getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.password",
        configForStorage1.getProperties().getProperty(DatabaseConfig.PASSWORD));

    DatabaseConfig configForStorage2 = MultiStorageEnv.getDatabaseConfigForStorage2();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.storage",
        configForStorage2.getProperties().getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.contact_points",
        configForStorage2.getProperties().getProperty(DatabaseConfig.CONTACT_POINTS));
    if (configForStorage2.getProperties().containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage2.contact_port",
          configForStorage2.getProperties().getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.username",
        configForStorage2.getProperties().getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.password",
        configForStorage2.getProperties().getProperty(DatabaseConfig.PASSWORD));

    // Define namespace mapping from namespace1 to storage1, from namespace2 to storage2, and from
    // the coordinator namespace to storage1
    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING,
        getNamespace1()
            + ":storage1,"
            + getNamespace2()
            + ":storage2,"
            + Coordinator.NAMESPACE
            + ":storage1");

    // The default storage is storage1
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "storage1");

    return new DatabaseConfig(props);
  }
}
