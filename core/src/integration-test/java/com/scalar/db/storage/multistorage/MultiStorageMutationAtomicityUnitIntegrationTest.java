package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class MultiStorageMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  @Override
  public Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, jdbc and cassandra
    properties.setProperty(MultiStorageConfig.STORAGES, "jdbc,cassandra");

    Properties propertiesForJdbc = MultiStorageEnv.getPropertiesForJdbc(testName);
    for (String propertyName : propertiesForJdbc.stringPropertyNames()) {
      properties.setProperty(
          MultiStorageConfig.STORAGES
              + ".jdbc."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForJdbc.getProperty(propertyName));
    }

    Properties propertiesForCassandra = MultiStorageEnv.getPropertiesForCassandra(testName);
    for (String propertyName : propertiesForCassandra.stringPropertyNames()) {
      properties.setProperty(
          MultiStorageConfig.STORAGES
              + ".cassandra."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForCassandra.getProperty(propertyName));
    }

    // Define namespace mappings
    properties.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING,
        NAMESPACE1 + ":jdbc," + NAMESPACE2 + ":jdbc," + NAMESPACE3 + ":cassandra");

    // The default storage is jdbc
    properties.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "jdbc");

    // Metadata cache expiration time
    properties.setProperty(DatabaseConfig.METADATA_CACHE_EXPIRATION_TIME_SECS, "1");

    return properties;
  }

  @Test
  public void mutate_MutationsAcrossStorageGiven_ShouldBehaveCorrectlyBaseOnMutationAtomicityUnit()
      throws ExecutionException {
    // Arrange
    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace3)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 1))
            .clusteringKey(Key.ofInt(COL_NAME2, 2))
            .intValue(COL_NAME3, 2)
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2)));

    // Assert
    assertThat(exception).isInstanceOf(IllegalArgumentException.class);
  }
}
