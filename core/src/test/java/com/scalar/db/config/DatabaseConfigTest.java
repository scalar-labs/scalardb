package com.scalar.db.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class DatabaseConfigTest {
  private static final String ANY_HOST = "localhost";
  private static final int ANY_PORT = 9999;
  private static final String ANY_USERNAME = "username";
  private static final String ANY_PASSWORD = "password";
  private static final String ANY_TRUE = "true";
  private static final String ANY_FALSE = "false";

  @Test
  public void constructor_PropertiesWithoutPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("cassandra");
    assertThat(config.getTransactionManager()).isEqualTo("consensus-commit");
    assertThat(config.getMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
    assertThat(config.getActiveTransactionManagementExpirationTimeMillis()).isEqualTo(-1);
    assertThat(config.isCrossPartitionScanEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanFilteringEnabled()).isFalse();
    assertThat(config.isCrossPartitionScanOrderingEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithoutUsernameGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isFalse();
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("cassandra");
    assertThat(config.getTransactionManager()).isEqualTo("consensus-commit");
    assertThat(config.getMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
    assertThat(config.getActiveTransactionManagementExpirationTimeMillis()).isEqualTo(-1);
    assertThat(config.getDefaultNamespaceName()).isEmpty();
    assertThat(config.isCrossPartitionScanEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanFilteringEnabled()).isFalse();
    assertThat(config.isCrossPartitionScanOrderingEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithoutPasswordGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isFalse();
    assertThat(config.getStorage()).isEqualTo("cassandra");
    assertThat(config.getTransactionManager()).isEqualTo("consensus-commit");
    assertThat(config.getMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
    assertThat(config.getActiveTransactionManagementExpirationTimeMillis()).isEqualTo(-1);
    assertThat(config.getDefaultNamespaceName()).isEmpty();
    assertThat(config.isCrossPartitionScanEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanFilteringEnabled()).isFalse();
    assertThat(config.isCrossPartitionScanOrderingEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("cassandra");
    assertThat(config.getTransactionManager()).isEqualTo("consensus-commit");
    assertThat(config.getMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
    assertThat(config.getActiveTransactionManagementExpirationTimeMillis()).isEqualTo(-1);
    assertThat(config.getDefaultNamespaceName()).isEmpty();
    assertThat(config.isCrossPartitionScanEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanFilteringEnabled()).isFalse();
    assertThat(config.isCrossPartitionScanOrderingEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithNegativePortGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(-1));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithCassandraGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("cassandra");
  }

  @Test
  public void constructor_PropertiesWithCosmosGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("cosmos");
  }

  @Test
  public void constructor_PropertiesWithDynamoGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("dynamo");
  }

  @Test
  public void constructor_PropertiesWithJdbcGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorage()).isEqualTo("jdbc");
  }

  @Test
  public void constructor_PropertiesWithMultiStorageGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEmpty();
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isFalse();
    assertThat(config.getPassword().isPresent()).isFalse();
    assertThat(config.getStorage()).isEqualTo("multi-storage");
  }

  @Test
  public void
      constructor_PropertiesWithConsensusCommitTransactionManagerGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getTransactionManager()).isEqualTo("consensus-commit");
  }

  @Test
  public void constructor_PropertiesWithJdbcTransactionManagerGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getTransactionManager()).isEqualTo("jdbc");
  }

  @Test
  public void constructor_PropertiesWithTableMetadataExpirationTimeSecsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.METADATA_CACHE_EXPIRATION_TIME_SECS, "3600");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getMetadataCacheExpirationTimeSecs()).isEqualTo(3600);
  }

  @Test
  public void
      constructor_PropertiesWithActiveTransactionManagementExpirationTimeMillisGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS, "3600");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getActiveTransactionManagementExpirationTimeMillis()).isEqualTo(3600);
  }

  @Test
  public void constructor_PropertiesWithDefaultNamespaceNameGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.DEFAULT_NAMESPACE_NAME, "ns");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getDefaultNamespaceName()).hasValue("ns");
  }

  @Test
  public void constructor_PropertiesWithCrossPartitionScanSettingsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, ANY_TRUE);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, ANY_TRUE);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, ANY_FALSE);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.isCrossPartitionScanEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanFilteringEnabled()).isTrue();
    assertThat(config.isCrossPartitionScanOrderingEnabled()).isFalse();
  }

  @Test
  public void
      constructor_PropertiesWithInvalidCrossPartitionScanSettingsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, ANY_FALSE);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, ANY_TRUE);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, ANY_TRUE);

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
