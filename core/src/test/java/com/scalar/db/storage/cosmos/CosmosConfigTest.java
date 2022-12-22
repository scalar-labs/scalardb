package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class CosmosConfigTest {

  private static final String ANY_ENDPOINT = "http://localhost:8081";
  private static final String ANY_KEY = "any_key";
  private static final String COSMOS_STORAGE = "cosmos";
  private static final String ANY_TABLE_METADATA_DATABASE = "any_database";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);
    props.setProperty(CosmosConfig.METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getKey()).isEqualTo(ANY_KEY);
    assertThat(config.getMetadataDatabase()).isPresent();
    assertThat(config.getMetadataDatabase().get()).isEqualTo(ANY_TABLE_METADATA_DATABASE);
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(CosmosConfig.METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act Assert
    assertThatThrownBy(() -> new CosmosConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_WithoutTableMetadataDatabase_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);

    // Act
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getKey()).isEqualTo(ANY_KEY);
    assertThat(config.getMetadataDatabase()).isNotPresent();
  }

  @Test
  public void
      constructor_WithTableMetadataDatabaseAndMetadataDatabaseGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);
    props.setProperty(CosmosConfig.METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);
    props.setProperty(CosmosConfig.TABLE_METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act Assert
    assertThatThrownBy(() -> new CosmosConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_WithTableMetadataDatabaseGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);
    props.setProperty(CosmosConfig.TABLE_METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getKey()).isEqualTo(ANY_KEY);
    assertThat(config.getMetadataDatabase()).isPresent();
    assertThat(config.getMetadataDatabase().get()).isEqualTo(ANY_TABLE_METADATA_DATABASE);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);

    // Act
    assertThatThrownBy(() -> new CosmosConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
