package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class DynamoDatabaseConfigTest {

  private static final String ANY_REGION = "us-west";
  private static final String ANY_ACCESS_KEY_ID = "accessKeyId";
  private static final String ANY_SECRET_ACCESS_ID = "secret_access_id";
  private static final String ANY_NAMESPACE_PREFIX = "prefix";
  private static final String DYNAMO_STORAGE = "dynamo";
  private static final String ANY_ENDPOINT_OVERRIDE = "http://localhost:8000";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_REGION);
    props.setProperty(DatabaseConfig.USERNAME, ANY_ACCESS_KEY_ID);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_SECRET_ACCESS_ID);
    props.setProperty(DatabaseConfig.STORAGE, DYNAMO_STORAGE);
    props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, ANY_NAMESPACE_PREFIX);
    props.setProperty(DynamoDatabaseConfig.ENDPOINT_OVERRIDE, ANY_ENDPOINT_OVERRIDE);

    // Act
    DynamoDatabaseConfig config = new DynamoDatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_REGION));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_ACCESS_KEY_ID);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_SECRET_ACCESS_ID);
    assertThat(config.getNamespacePrefix().isPresent()).isTrue();
    assertThat(config.getNamespacePrefix().get()).isEqualTo(ANY_NAMESPACE_PREFIX + "_");
    assertThat(config.getStorageClass()).isEqualTo(Dynamo.class);
    assertThat(config.getAdminClass()).isEqualTo(DynamoAdmin.class);
    assertThat(config.getEndpointOverride().isPresent()).isTrue();
    assertThat(config.getEndpointOverride().get()).isEqualTo(ANY_ENDPOINT_OVERRIDE);
  }

  @Test
  public void constructor_PropertiesWithoutEndpointOverrideGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_REGION);
    props.setProperty(DatabaseConfig.USERNAME, ANY_ACCESS_KEY_ID);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_SECRET_ACCESS_ID);
    props.setProperty(DatabaseConfig.STORAGE, DYNAMO_STORAGE);
    props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, ANY_NAMESPACE_PREFIX);

    // Act
    DynamoDatabaseConfig config = new DynamoDatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_REGION));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_ACCESS_KEY_ID);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_SECRET_ACCESS_ID);
    assertThat(config.getNamespacePrefix().isPresent()).isTrue();
    assertThat(config.getNamespacePrefix().get()).isEqualTo(ANY_NAMESPACE_PREFIX + "_");
    assertThat(config.getStorageClass()).isEqualTo(Dynamo.class);
    assertThat(config.getAdminClass()).isEqualTo(DynamoAdmin.class);
    assertThat(config.getEndpointOverride().isPresent()).isFalse();
  }
}
