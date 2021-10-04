package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

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
    props.setProperty(CosmosConfig.TABLE_METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act
    CosmosConfig config = new CosmosConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_ENDPOINT));
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_KEY);
    assertThat(config.getStorageClass()).isEqualTo(Cosmos.class);
    assertThat(config.getAdminClass()).isEqualTo(CosmosAdmin.class);
    assertThat(config.getTableMetadataDatabase()).isPresent();
    assertThat(config.getTableMetadataDatabase().get()).isEqualTo(ANY_TABLE_METADATA_DATABASE);
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(CosmosConfig.TABLE_METADATA_DATABASE, ANY_TABLE_METADATA_DATABASE);

    // Act Assert
    assertThatThrownBy(() -> new CosmosConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_WithoutTableMetadataDatabase_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_ENDPOINT);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_KEY);
    props.setProperty(DatabaseConfig.STORAGE, COSMOS_STORAGE);

    // Act
    CosmosConfig config = new CosmosConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_ENDPOINT));
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_KEY);
    assertThat(config.getStorageClass()).isEqualTo(Cosmos.class);
    assertThat(config.getAdminClass()).isEqualTo(CosmosAdmin.class);
    assertThat(config.getTableMetadataDatabase()).isNotPresent();
  }
}
