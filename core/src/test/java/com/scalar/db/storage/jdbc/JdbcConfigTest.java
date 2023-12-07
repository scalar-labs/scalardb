package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class JdbcConfigTest {

  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:3306/";
  private static final String ANY_USERNAME = "root";
  private static final String ANY_PASSWORD = "mysql";
  private static final String JDBC_STORAGE = "jdbc";
  private static final String ANY_METADATA_SCHEMA = "any_schema";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, ANY_METADATA_SCHEMA);
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "1");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "100");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "500");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "300");
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, Isolation.SERIALIZABLE.name());
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "50");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "150");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "200");

    // Act
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getJdbcUrl()).isEqualTo(ANY_JDBC_URL);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getConnectionPoolMinIdle()).isEqualTo(1);
    assertThat(config.getConnectionPoolMaxIdle()).isEqualTo(100);
    assertThat(config.getConnectionPoolMaxTotal()).isEqualTo(500);
    assertThat(config.isPreparedStatementsPoolEnabled()).isEqualTo(true);
    assertThat(config.getPreparedStatementsPoolMaxOpen()).isEqualTo(300);
    assertThat(config.getIsolation()).isPresent();
    assertThat(config.getIsolation().get()).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(config.getMetadataSchema()).isEqualTo(ANY_METADATA_SCHEMA);
    assertThat(config.getTableMetadataConnectionPoolMinIdle()).isEqualTo(100);
    assertThat(config.getTableMetadataConnectionPoolMaxIdle()).isEqualTo(200);
    assertThat(config.getTableMetadataConnectionPoolMaxTotal()).isEqualTo(300);
    assertThat(config.getAdminConnectionPoolMinIdle()).isEqualTo(50);
    assertThat(config.getAdminConnectionPoolMaxIdle()).isEqualTo(150);
    assertThat(config.getAdminConnectionPoolMaxTotal()).isEqualTo(200);
  }

  @Test
  public void
      constructor_PropertiesWithoutConnectionPoolPropertiesGiven_ShouldLoadProperlyAndUseDefaultValues() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);

    // Act
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getJdbcUrl()).isEqualTo(ANY_JDBC_URL);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getConnectionPoolMinIdle())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getConnectionPoolMaxIdle())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getConnectionPoolMaxTotal())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    assertThat(config.isPreparedStatementsPoolEnabled())
        .isEqualTo(JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    assertThat(config.getPreparedStatementsPoolMaxOpen())
        .isEqualTo(JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
    assertThat(config.getIsolation()).isNotPresent();
    assertThat(config.getMetadataSchema()).isEqualTo(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(config.getTableMetadataConnectionPoolMinIdle())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getTableMetadataConnectionPoolMaxIdle())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getTableMetadataConnectionPoolMaxTotal())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL);
  }

  @Test
  public void constructor_PropertiesWithWrongStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "ccc");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "ddd");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "eee");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidPreparedStatementsPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "ddd");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "eee");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidIsolationLevelGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidTableMetadataConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "ccc");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidAdminConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "ccc");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithTableMetadataSchemaGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.TABLE_METADATA_SCHEMA, ANY_METADATA_SCHEMA);

    // Act
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getJdbcUrl()).isEqualTo(ANY_JDBC_URL);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getMetadataSchema()).isEqualTo(ANY_METADATA_SCHEMA);
  }
}
