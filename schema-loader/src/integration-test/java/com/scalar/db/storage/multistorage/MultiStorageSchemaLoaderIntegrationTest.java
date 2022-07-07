package com.scalar.db.storage.multistorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class MultiStorageSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, storage1 and storage2
    props.setProperty(MultiStorageConfig.STORAGES, "storage1,storage2");

    Properties propertiesForStorage1 = MultiStorageEnv.getPropertiesForStorage1();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.storage",
        propertiesForStorage1.getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.contact_points",
        propertiesForStorage1.getProperty(DatabaseConfig.CONTACT_POINTS));
    if (propertiesForStorage1.containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage1.contact_port",
          propertiesForStorage1.getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.username",
        propertiesForStorage1.getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.password",
        propertiesForStorage1.getProperty(DatabaseConfig.PASSWORD));

    Properties propertiesForStorage2 = MultiStorageEnv.getPropertiesForStorage2();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.storage",
        propertiesForStorage2.getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.contact_points",
        propertiesForStorage2.getProperty(DatabaseConfig.CONTACT_POINTS));
    if (propertiesForStorage2.containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage2.contact_port",
          propertiesForStorage2.getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.username",
        propertiesForStorage2.getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.password",
        propertiesForStorage2.getProperty(DatabaseConfig.PASSWORD));

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

    return props;
  }

  @Override
  protected void dropMetadataTable() throws Exception {
    AdminTestUtils storage1AdminUtils =
        AdminTestUtils.create(MultiStorageEnv.getPropertiesForStorage1());
    storage1AdminUtils.dropMetadataTable();
    AdminTestUtils storage2AdminUtils =
        AdminTestUtils.create(MultiStorageEnv.getPropertiesForStorage2());
    storage2AdminUtils.dropMetadataTable();
  }

  @Override
  protected void assertTableMetadataForCoordinatorTableArePresent() throws Exception {
    assertTableMetadataForCoordinatorTableArePresentForStorage(
        MultiStorageEnv.getPropertiesForStorage1());
  }
}
