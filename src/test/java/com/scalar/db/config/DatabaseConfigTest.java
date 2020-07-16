package com.scalar.db.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cosmos.Cosmos;
import java.util.Arrays;
import java.util.Properties;
import org.junit.Test;

public class DatabaseConfigTest {
  private static final String ANY_HOST = "localhost";
  private static final int ANY_PORT = 9999;
  private static final String ANY_USERNAME = "username";
  private static final String ANY_PASSWORD = "password";

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
    assertThat(config.getContactPoints()).isEqualTo(Arrays.asList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
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
    assertThat(config.getContactPoints()).isEqualTo(Arrays.asList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
  }

  @Test
  public void constructor_NonQualifiedPropertiesGiven_ShouldThrowRuntimeException() {
    // Arrange
    Properties props = new Properties();

    // Act Assert
    assertThatThrownBy(
            () -> {
              new DatabaseConfig(props);
            })
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void constructor_PropertiesWithNegativePortGiven_ShouldThrowRuntimeException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(-1));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act Assert
    assertThatThrownBy(
            () -> {
              new DatabaseConfig(props);
            })
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void constructor_PropertiesWithCosmosGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "Cosmos");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Arrays.asList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cosmos.class);
  }

  @Test
  public void constructor_WrongStorageClassGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "WrongStorage");

    // Act Assert
    assertThatThrownBy(
            () -> {
              new DatabaseConfig(props);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }
}
